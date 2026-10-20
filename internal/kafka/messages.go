package kafka

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	kgo "github.com/segmentio/kafka-go"
)

// PayloadView 是 key/value/header value 的展示形式。
type PayloadView struct {
	Text string
	// Encoding: empty | text | json | hex
	Encoding string
}

// HeaderView 是消息 header 的展示形式。
type HeaderView struct {
	Key   string
	Value PayloadView
}

// MessageView 是消息列表/详情的一行。
type MessageView struct {
	Partition int
	Offset    int64
	Time      time.Time
	Key       PayloadView
	Value     PayloadView
	Size      int
	Headers   []HeaderView
}

// BrowseRequest 请求从指定分区的某个位置读取最多 Count 条消息。
type BrowseRequest struct {
	Topic     string
	Partition int
	// Offset 为 OffsetEarliest / OffsetLatest 哨兵值或绝对 offset。
	Offset int64
	Count  int
}

// BrowseResult 是一次浏览的结果。
type BrowseResult struct {
	Messages []MessageView
	// Partition 是实际读取的分区。
	Partition int
	// PartitionCount 是 topic 的分区总数（用于渲染下拉框）。
	PartitionCount int
	// FirstOffset / LastOffset 是分区的 offset 边界 [first, last)。
	FirstOffset int64
	LastOffset  int64
	// StartOffset 是实际开始读取的位置。
	StartOffset int64
	// NextOffset 是下一批读取的起始 offset。
	NextOffset int64
	// TimedOut 表示在读满 Count 条前达到超时（常见于尾部跟随无新消息）。
	TimedOut bool
	// Truncated 表示还有更多消息可读。
	Truncated bool
	// Note 记录非致命的中断原因。
	Note string
}

// maxDisplay 是单条 payload 展示的最大字节数，防止超大消息撑爆页面。
const maxDisplay = 128 << 10

// FormatPayload 把原始字节转成可展示文本。
// allowJSON 时优先做 JSON 缩进；非 UTF-8 内容退化为十六进制。
func FormatPayload(b []byte, allowJSON bool) PayloadView {
	if len(b) == 0 {
		return PayloadView{Encoding: "empty"}
	}
	if allowJSON && looksLikeJSON(b) && json.Valid(b) {
		var buf bytes.Buffer
		if err := json.Indent(&buf, b, "", "  "); err == nil {
			return PayloadView{Text: clip(buf.String()), Encoding: "json"}
		}
	}
	if utf8.Valid(b) {
		return PayloadView{Text: clip(string(b)), Encoding: "text"}
	}
	return PayloadView{Text: clip(hex.EncodeToString(b)), Encoding: "hex"}
}

func looksLikeJSON(b []byte) bool {
	t := b
	for len(t) > 0 && isSpace(t[0]) {
		t = t[1:]
	}
	return len(t) > 0 && (t[0] == '{' || t[0] == '[')
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// clip 截断到 maxDisplay 字节，且保证不切断 UTF-8 多字节字符。
func clip(s string) string {
	if len(s) <= maxDisplay {
		return s
	}
	i := maxDisplay
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i] + "\n…（内容过长，已截断）"
}

// Browse 从 topic[partition] 的指定位置读取消息。
// 读取过程中超时或出错时返回已读到的部分，并把原因写入 Note/TimedOut。
func (c *Client) Browse(ctx context.Context, req BrowseRequest) (BrowseResult, error) {
	if req.Topic == "" {
		return BrowseResult{}, errors.New("缺少 topic")
	}
	if req.Count <= 0 {
		req.Count = 20
	}
	if req.Count > MaxBrowseCount {
		req.Count = MaxBrowseCount
	}

	meta, err := c.metadata(ctx, req.Topic)
	if err != nil {
		return BrowseResult{}, err
	}
	topic := findTopic(meta, req.Topic)
	if topic == nil || topic.Error != nil {
		return BrowseResult{}, fmt.Errorf("%w: topic %q", ErrNotFound, req.Topic)
	}
	n := len(topic.Partitions)
	if n == 0 {
		return BrowseResult{}, fmt.Errorf("topic %q 没有可用分区", req.Topic)
	}
	if req.Partition < 0 || req.Partition >= n {
		return BrowseResult{}, fmt.Errorf("%w: 分区 %d 超出范围 [0, %d)", ErrNotFound, req.Partition, n-1)
	}

	first, last, err := c.partitionOffsets(ctx, req.Topic, req.Partition)
	if err != nil {
		return BrowseResult{}, err
	}

	start := req.Offset
	switch {
	case start == OffsetEarliest:
		start = first
	case start == OffsetLatest:
		start = last
	default:
		// 越界绝对 offset 收敛到边界内（可能已被日志清理）。
		if start < first {
			start = first
		}
		if start > last {
			start = last
		}
	}

	res := BrowseResult{
		Partition:      req.Partition,
		PartitionCount: n,
		FirstOffset:    first,
		LastOffset:     last,
		StartOffset:    start,
		NextOffset:     start,
	}
	if start >= last {
		// 该位置没有可读消息。
		return res, nil
	}

	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:     c.brokers,
		Topic:       req.Topic,
		Partition:   req.Partition,
		StartOffset: start,
		MinBytes:    1,
		MaxBytes:    8 << 20,
		// 必须保持小值：reader 后台循环交付批次后会立刻发起下一次长轮询
		// fetch（ReadDeadline = now+MaxWait），而 Close() 的 join.Wait()
		// 要等它到期才能退出。MaxWait=300ms 会让每次浏览多等约 250ms
		//（页面"点按钮才响应"的第二段根因）。有数据时 broker 立即响应，
		// 此值不增加正常读取延迟。
		MaxWait:         25 * time.Millisecond,
		Dialer:          c.dialer,
		ReadLagInterval: -1, // 不需要 reader 自己统计 lag
	})
	defer func() { _ = reader.Close() }()

	next := start
	// 只在 [next, last) 范围内拉取：读到已知的 log end offset 立即返回。
	// 否则消息不足 Count 条时会一直阻塞到 ctx 超时，页面表现为
	// "点浏览按钮要等好几秒才响应"。
	for len(res.Messages) < req.Count && next < last {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				res.TimedOut = true
			default:
				if len(res.Messages) == 0 {
					return BrowseResult{}, fmt.Errorf("读取消息失败: %w", err)
				}
				res.Note = fmt.Sprintf("读取中断：%v", err)
			}
			break
		}
		res.Messages = append(res.Messages, viewOfMessage(m))
		next = m.Offset + 1
	}
	res.NextOffset = next
	res.Truncated = next < last && len(res.Messages) == req.Count
	return res, nil
}

func viewOfMessage(m kgo.Message) MessageView {
	key, value := m.Key, m.Value
	mv := MessageView{
		Partition: m.Partition,
		Offset:    m.Offset,
		Time:      m.Time,
		Key:       FormatPayload(key, false),
		Value:     FormatPayload(value, true),
		Size:      len(key) + len(value),
	}
	for _, h := range m.Headers {
		mv.Headers = append(mv.Headers, HeaderView{
			Key:   h.Key,
			Value: FormatPayload(h.Value, false),
		})
	}
	return mv
}

// ProduceRequest 是发送单条消息的请求。
type ProduceRequest struct {
	Topic string
	// Partition 为 -1 表示自动选择（有 key 按 CRC32，无 key 轮询）。
	Partition int
	Key       string
	Value     string
	Headers   []ProduceHeader
}

// ProduceHeader 是发送消息时附带的一条 header。
type ProduceHeader struct {
	Key   string
	Value string
}

// ProduceResult 描述已发送消息的落点。
type ProduceResult struct {
	Partition int
	Offset    int64
	Time      time.Time
}

// Produce 发送一条消息并返回 broker 确认的分区与 offset。
func (c *Client) Produce(ctx context.Context, req ProduceRequest) (ProduceResult, error) {
	if req.Topic == "" {
		return ProduceResult{}, errors.New("缺少 topic")
	}

	partition := req.Partition
	if partition < 0 {
		meta, err := c.metadata(ctx, req.Topic)
		if err != nil {
			return ProduceResult{}, err
		}
		topic := findTopic(meta, req.Topic)
		if topic == nil || topic.Error != nil || len(topic.Partitions) == 0 {
			return ProduceResult{}, fmt.Errorf("%w: topic %q", ErrNotFound, req.Topic)
		}
		partition = pickPartition(req.Key, len(topic.Partitions), c.rrSeq.Add(1)-1)
	} else {
		// 校验分区范围，给出比 broker 更友好的错误。
		meta, err := c.metadata(ctx, req.Topic)
		if err != nil {
			return ProduceResult{}, err
		}
		topic := findTopic(meta, req.Topic)
		if topic == nil || topic.Error != nil {
			return ProduceResult{}, fmt.Errorf("%w: topic %q", ErrNotFound, req.Topic)
		}
		if partition >= len(topic.Partitions) {
			return ProduceResult{}, fmt.Errorf("分区 %d 超出范围 [0, %d)", partition, len(topic.Partitions)-1)
		}
	}

	var key kgo.Bytes
	if req.Key != "" {
		key = kgo.NewBytes([]byte(req.Key))
	}
	headers := make([]kgo.Header, 0, len(req.Headers))
	for _, h := range req.Headers {
		if h.Key == "" {
			continue
		}
		headers = append(headers, kgo.Header{Key: h.Key, Value: []byte(h.Value)})
	}

	res, err := c.api.Produce(ctx, &kgo.ProduceRequest{
		Topic:        req.Topic,
		Partition:    partition,
		RequiredAcks: kgo.RequireAll,
		Records: kgo.NewRecordReader(kgo.Record{
			Key:     key,
			Value:   kgo.NewBytes([]byte(req.Value)),
			Headers: headers,
			Time:    time.Now(),
		}),
	})
	if err != nil {
		return ProduceResult{}, fmt.Errorf("发送消息失败: %w", err)
	}
	if res == nil {
		return ProduceResult{}, errors.New("发送消息失败: broker 未确认")
	}
	if res.Error != nil {
		return ProduceResult{}, fmt.Errorf("发送消息失败: %w", res.Error)
	}
	for i, e := range res.RecordErrors {
		return ProduceResult{}, fmt.Errorf("发送消息失败: 第 %d 条记录 %v", i, e)
	}
	ts := res.LogAppendTime
	if ts.IsZero() {
		ts = time.Now()
	}
	return ProduceResult{Partition: partition, Offset: res.BaseOffset, Time: ts}, nil
}
