// Package kafka 封装对 Kafka 集群的只读查询与消息发送，
// 基于 github.com/segmentio/kafka-go，向 web 层暴露面向页面的数据结构。
package kafka

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

// Offset 哨兵值：用于 BrowseRequest.Offset 表示按位置而非绝对 offset 读取。
const (
	OffsetEarliest int64 = kgo.FirstOffset // 最早可用 offset
	OffsetLatest   int64 = kgo.LastOffset  // 下一个写入位置（log end offset）
)

// MaxBrowseCount 是单次浏览消息数的上限。
const MaxBrowseCount = 1000

// ErrNotFound 表示请求的 topic / consumer group 不存在。
var ErrNotFound = errors.New("not found")

// Client 是并发安全的 Kafka 访问入口。
type Client struct {
	brokers []string
	timeout time.Duration
	api     *kgo.Client
	dialer  *kgo.Dialer
	// rrSeq 是自动分区时的轮询计数。用 atomic.Uint64 而非 AddUint64：
	// 32 位平台（windows/386）要求 64 位原子操作 8 字节对齐，
	// 结构体末尾字段不保证对齐，会触发 unaligned panic。
	rrSeq atomic.Uint64
}

// New 创建 Client。timeout 作用于每一轮 Kafka RPC。
func New(brokers []string, timeout time.Duration) *Client {
	addrs := append([]string(nil), brokers...)
	return &Client{
		brokers: addrs,
		timeout: timeout,
		api:     &kgo.Client{Addr: kgo.TCP(addrs...), Timeout: timeout},
		dialer:  &kgo.Dialer{Timeout: timeout, DualStack: true, ClientID: "kafka-ui-go"},
	}
}

// Brokers 返回 bootstrap 地址列表副本。
func (c *Client) Brokers() []string {
	return append([]string(nil), c.brokers...)
}

// metadata 获取集群元数据；topics 为空表示全部 topic。
func (c *Client) metadata(ctx context.Context, topics ...string) (*kgo.MetadataResponse, error) {
	resp, err := c.api.Metadata(ctx, &kgo.MetadataRequest{Topics: topics})
	if err != nil {
		return nil, fmt.Errorf("获取元数据失败: %w", err)
	}
	return resp, nil
}

func findTopic(meta *kgo.MetadataResponse, name string) *kgo.Topic {
	for i := range meta.Topics {
		if meta.Topics[i].Name == name {
			return &meta.Topics[i]
		}
	}
	return nil
}

func brokerAddrMap(meta *kgo.MetadataResponse) map[int]string {
	m := make(map[int]string, len(meta.Brokers))
	for _, b := range meta.Brokers {
		m[b.ID] = joinHostPort(b)
	}
	return m
}

func joinHostPort(b kgo.Broker) string {
	return net.JoinHostPort(b.Host, strconv.Itoa(b.Port))
}

// partitionOffsets 一次性读取某分区的首尾 offset（log start / log end）。
func (c *Client) partitionOffsets(ctx context.Context, topic string, partition int) (first, last int64, err error) {
	resp, err := c.api.ListOffsets(ctx, &kgo.ListOffsetsRequest{
		Topics: map[string][]kgo.OffsetRequest{
			topic: {kgo.FirstOffsetOf(partition), kgo.LastOffsetOf(partition)},
		},
	})
	if err != nil {
		return 0, 0, fmt.Errorf("读取 offset 失败: %w", err)
	}
	for _, po := range resp.Topics[topic] {
		if po.Partition != partition {
			continue
		}
		if po.Error != nil {
			return 0, 0, fmt.Errorf("读取 offset 失败: %w", po.Error)
		}
		if po.FirstOffset < 0 || po.LastOffset < 0 {
			return 0, 0, errors.New("broker 未返回完整的 offset 边界")
		}
		return po.FirstOffset, po.LastOffset, nil
	}
	return 0, 0, fmt.Errorf("topic %q 分区 %d 的 offset 不存在", topic, partition)
}

// pickPartition 计算自动分区：有 key 时用 CRC32（与 kafka-go 的 Hash
// balancer 一致），无 key 时按 seq 轮询。
func pickPartition(key string, n int, seq uint64) int {
	if n <= 0 {
		return 0
	}
	if key != "" {
		return int(crc32.ChecksumIEEE([]byte(key)) % uint32(n))
	}
	return int(seq % uint64(n))
}

func sortByName(topics []TopicSummary) {
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
}
