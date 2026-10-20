package kafka

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestFormatPayloadEmpty(t *testing.T) {
	got := FormatPayload(nil, true)
	if got.Encoding != "empty" || got.Text != "" {
		t.Errorf("FormatPayload(nil) = %+v", got)
	}
	got = FormatPayload([]byte{}, false)
	if got.Encoding != "empty" {
		t.Errorf("FormatPayload(empty) = %+v", got)
	}
}

func TestFormatPayloadJSON(t *testing.T) {
	got := FormatPayload([]byte(`{"b":1,"a":2}`), true)
	if got.Encoding != "json" {
		t.Fatalf("Encoding = %q", got.Encoding)
	}
	if !strings.Contains(got.Text, "\n  \"b\": 1") {
		t.Errorf("Text = %q, want indented json", got.Text)
	}
}

func TestFormatPayloadJSONArray(t *testing.T) {
	got := FormatPayload([]byte(`[1,2,3]`), true)
	if got.Encoding != "json" {
		t.Errorf("Encoding = %q", got.Encoding)
	}
}

func TestFormatPayloadScalarNotJSON(t *testing.T) {
	got := FormatPayload([]byte(`123`), true)
	if got.Encoding != "text" {
		t.Errorf("Encoding = %q, want text for json scalar", got.Encoding)
	}
	if got.Text != "123" {
		t.Errorf("Text = %q", got.Text)
	}
}

func TestFormatPayloadText(t *testing.T) {
	got := FormatPayload([]byte("hello 世界"), false)
	if got.Encoding != "text" || got.Text != "hello 世界" {
		t.Errorf("got %+v", got)
	}
}

func TestFormatPayloadHex(t *testing.T) {
	got := FormatPayload([]byte{0xff, 0x00, 0xfe}, true)
	if got.Encoding != "hex" || got.Text != "ff00fe" {
		t.Errorf("got %+v", got)
	}
}

func TestFormatPayloadClipKeepsValidUTF8(t *testing.T) {
	// 全部由多字节字符组成，远超展示上限。
	b := []byte(strings.Repeat("中", maxDisplay))
	got := FormatPayload(b, false)
	if !utf8.ValidString(got.Text) {
		t.Fatal("clipped text is not valid utf8")
	}
	if !strings.Contains(got.Text, "已截断") {
		t.Errorf("missing truncation marker: %q", got.Text[len(got.Text)-20:])
	}
}

func TestFormatPayloadClipHex(t *testing.T) {
	b := make([]byte, maxDisplay+100)
	for i := range b {
		b[i] = 0xab
	}
	got := FormatPayload(b, true)
	if len(got.Text) > maxDisplay+64 {
		t.Errorf("hex text not clipped: %d bytes", len(got.Text))
	}
}

func TestPickPartitionWithKey(t *testing.T) {
	a := pickPartition("user-1", 8, 0)
	b := pickPartition("user-1", 8, 99)
	if a != b {
		t.Errorf("key-based partition must be deterministic: %d != %d", a, b)
	}
	if a < 0 || a >= 8 {
		t.Errorf("partition %d out of range", a)
	}
	// 不同 key 大概率落到不同分区。
	diff := 0
	for _, k := range []string{"a", "b", "c", "d"} {
		if pickPartition(k, 8, 0) != pickPartition("user-1", 8, 0) {
			diff++
		}
	}
	if diff == 0 {
		t.Error("all keys mapped to the same partition")
	}
}

func TestPickPartitionNoKeyRoundRobin(t *testing.T) {
	seen := map[int]bool{}
	for seq := uint64(0); seq < 8; seq++ {
		p := pickPartition("", 8, seq)
		if p < 0 || p >= 8 {
			t.Fatalf("partition %d out of range", p)
		}
		seen[p] = true
	}
	if len(seen) != 8 {
		t.Errorf("round robin covered %d partitions, want 8", len(seen))
	}
}

func TestCalcLag(t *testing.T) {
	cases := []struct {
		committed, end, want int64
	}{
		{10, 25, 15},
		{10, 10, 0},
		{10, 5, 0},
		{-1, 10, 0},
		{0, 0, 0},
	}
	for _, c := range cases {
		if got := calcLag(c.committed, c.end); got != c.want {
			t.Errorf("calcLag(%d, %d) = %d, want %d", c.committed, c.end, got, c.want)
		}
	}
}

// brokerAddr 返回集成测试用的 broker 地址（KAFKA_BROKERS 首个，默认 localhost:9092）。
func brokerAddr() string {
	if v := strings.TrimSpace(os.Getenv("KAFKA_BROKERS")); v != "" {
		return strings.Split(v, ",")[0]
	}
	return "localhost:9092"
}

// requireBroker 在 broker 不可达时跳过当前测试。
func requireBroker(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", brokerAddr(), 500*time.Millisecond)
	if err != nil {
		t.Skipf("no kafka at %s: %v", brokerAddr(), err)
	}
	_ = conn.Close()
}

// pickBrowseTopic 选一个可用于浏览测试的 topic（KAFKA_UI_TEST_TOPIC 优先）。
func pickBrowseTopic(t *testing.T, ctx context.Context, client *Client) string {
	t.Helper()
	name := strings.TrimSpace(os.Getenv("KAFKA_UI_TEST_TOPIC"))
	if name == "" {
		topics, err := client.ListTopics(ctx)
		if err != nil {
			t.Skipf("cannot list topics: %v", err)
		}
		for _, tt := range topics {
			if !tt.Internal && tt.Partitions > 0 {
				name = tt.Name
				break
			}
		}
	}
	if name == "" {
		t.Skip("no suitable topic to browse")
	}
	return name
}

// TestBrowseIntegrationStopsAtLogEnd 需要真实 Kafka（不可达则跳过）：
// 消息不足 Count 条时，Browse 必须在读到 log end offset 后立即返回。
// 回归背景：旧实现会继续 FetchMessage 空等，直到请求超时才响应，
// 页面表现为"点浏览按钮要等好几秒"。
func TestBrowseIntegrationStopsAtLogEnd(t *testing.T) {
	requireBroker(t)
	broker := brokerAddr()
	client := New([]string{broker}, 20*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	name := pickBrowseTopic(t, ctx, client)

	start := time.Now()
	res, err := client.Browse(ctx, BrowseRequest{
		Topic:     name,
		Partition: 0,
		Offset:    OffsetEarliest,
		Count:     1000, // 刻意远大于实际消息数
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Browse(%s) error: %v", name, err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Browse took %v — likely blocked waiting for messages beyond log end offset", elapsed)
	}
	// 读到的条数不可能超过已知的 offset 边界。
	if int64(len(res.Messages)) > res.LastOffset-res.FirstOffset {
		t.Errorf("read %d messages but bounds are [%d, %d)",
			len(res.Messages), res.FirstOffset, res.LastOffset)
	}
	// 边界内明明有消息却一条都没读到，说明读取逻辑有问题
	//（边界内消息被压实移除时会走 TimedOut/Note 分支，不在此列）。
	if len(res.Messages) == 0 && res.LastOffset > res.FirstOffset &&
		!res.TimedOut && res.Note == "" && !res.Truncated {
		t.Errorf("no messages read although bounds are [%d, %d)",
			res.FirstOffset, res.LastOffset)
	}
	t.Logf("topic=%s bounds=[%d,%d) read=%d elapsed=%v",
		name, res.FirstOffset, res.LastOffset, len(res.Messages), elapsed.Round(time.Millisecond))
}
