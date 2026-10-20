package web

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"kafka-ui-go/internal/kafka"
)

// fakeProbe 实现 Probe，按需注入返回值与错误。
type fakeProbe struct {
	overview        func(ctx context.Context) (kafka.ClusterInfo, error)
	listTopics      func(ctx context.Context) ([]kafka.TopicSummary, error)
	topicPartitions func(ctx context.Context, name string) (int, error)
	topicDetail     func(ctx context.Context, name string) (kafka.TopicDetail, error)
	browse          func(ctx context.Context, req kafka.BrowseRequest) (kafka.BrowseResult, error)
	produce         func(ctx context.Context, req kafka.ProduceRequest) (kafka.ProduceResult, error)
	listGroups      func(ctx context.Context) ([]kafka.GroupSummary, error)
	groupDetail     func(ctx context.Context, id string) (kafka.GroupDetail, error)
}

func (f *fakeProbe) Overview(ctx context.Context) (kafka.ClusterInfo, error) {
	if f.overview == nil {
		return kafka.ClusterInfo{}, nil
	}
	return f.overview(ctx)
}

func (f *fakeProbe) ListTopics(ctx context.Context) ([]kafka.TopicSummary, error) {
	if f.listTopics == nil {
		return nil, nil
	}
	return f.listTopics(ctx)
}

func (f *fakeProbe) TopicPartitions(ctx context.Context, name string) (int, error) {
	if f.topicPartitions == nil {
		return 0, nil
	}
	return f.topicPartitions(ctx, name)
}

func (f *fakeProbe) TopicDetail(ctx context.Context, name string) (kafka.TopicDetail, error) {
	if f.topicDetail == nil {
		return kafka.TopicDetail{}, nil
	}
	return f.topicDetail(ctx, name)
}

func (f *fakeProbe) Browse(ctx context.Context, req kafka.BrowseRequest) (kafka.BrowseResult, error) {
	if f.browse == nil {
		return kafka.BrowseResult{}, nil
	}
	return f.browse(ctx, req)
}

func (f *fakeProbe) Produce(ctx context.Context, req kafka.ProduceRequest) (kafka.ProduceResult, error) {
	if f.produce == nil {
		return kafka.ProduceResult{}, nil
	}
	return f.produce(ctx, req)
}

func (f *fakeProbe) ListGroups(ctx context.Context) ([]kafka.GroupSummary, error) {
	if f.listGroups == nil {
		return nil, nil
	}
	return f.listGroups(ctx)
}

func (f *fakeProbe) GroupDetail(ctx context.Context, id string) (kafka.GroupDetail, error) {
	if f.groupDetail == nil {
		return kafka.GroupDetail{}, nil
	}
	return f.groupDetail(ctx, id)
}

func newTestServer(t *testing.T, f *fakeProbe) http.Handler {
	t.Helper()
	s, err := New(f, 3*time.Second)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return s.Handler()
}

func doGet(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// body 返回响应内容；使用 String() 避免消费缓冲导致后续断言读到空串。
func body(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	return rec.Body.String()
}

func mustContain(t *testing.T, rec *httptest.ResponseRecorder, wants ...string) {
	t.Helper()
	if rec.Code != http.StatusOK && len(wants) > 0 {
		// 状态码由调用方单独断言；这里只查内容。
	}
	s := body(t, rec)
	for _, w := range wants {
		if !strings.Contains(s, w) {
			t.Errorf("response missing %q (status %d)", w, rec.Code)
		}
	}
}

func TestHomeRendersClusterInfo(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		overview: func(ctx context.Context) (kafka.ClusterInfo, error) {
			return kafka.ClusterInfo{
				ClusterID:      "cluster-x",
				ControllerID:   1,
				TopicCount:     3,
				PartitionCount: 9,
				GroupCount:     2,
				Brokers: []kafka.BrokerInfo{
					{ID: 1, Addr: "127.0.0.1:9092"},
				},
			}, nil
		},
	})
	rec := doGet(t, h, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	mustContain(t, rec, "集群概览", "cluster-x", "127.0.0.1:9092", "Consumer Groups")
}

func TestHomeShowsBannerOnProbeError(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		overview: func(ctx context.Context) (kafka.ClusterInfo, error) {
			return kafka.ClusterInfo{}, errors.New("connection refused")
		},
	})
	rec := doGet(t, h, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	mustContain(t, rec, "读取集群概览失败", "connection refused")
}

func TestTopicsListAndSearch(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		listTopics: func(ctx context.Context) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 6, ReplicationFactor: 3},
				{Name: "users", Partitions: 3, ReplicationFactor: 1, Internal: true, UnderReplicated: true},
			}, nil
		},
	})

	rec := doGet(t, h, "/topics")
	mustContain(t, rec, "orders", "users", "internal", "副本不足")

	rec = doGet(t, h, "/topics?q=ord")
	mustContain(t, rec, "orders")
	if strings.Contains(body(t, rec), ">users<") {
		t.Error("search should exclude users")
	}

	rec = doGet(t, h, "/topics?q=zzz")
	mustContain(t, rec, "没有匹配")
}

func TestTopicDetail(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		topicDetail: func(ctx context.Context, name string) (kafka.TopicDetail, error) {
			return kafka.TopicDetail{
				Name:              "orders",
				ReplicationFactor: 3,
				Partitions: []kafka.PartitionInfo{
					{ID: 0, LeaderID: 1, LeaderAddr: "b1:9092", Replicas: []int{1, 2, 3}, ISR: []int{1, 2, 3},
						FirstOffset: 10, LastOffset: 60},
				},
				Config: []kafka.ConfigEntry{
					{Name: "retention.ms", Value: "604800000"},
				},
				TotalMessages: 50,
			}, nil
		},
	})
	rec := doGet(t, h, "/topics/orders")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	mustContain(t, rec, "orders", "retention.ms", "50", "b1:9092")
}

func TestTopicNotFound(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		topicDetail: func(ctx context.Context, name string) (kafka.TopicDetail, error) {
			return kafka.TopicDetail{}, kafka.ErrNotFound
		},
	})
	rec := doGet(t, h, "/topics/nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	mustContain(t, rec, "Topic 不存在")
}

func TestTopicProbeError502(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		topicDetail: func(ctx context.Context, name string) (kafka.TopicDetail, error) {
			return kafka.TopicDetail{}, errors.New("dial tcp timeout")
		},
	})
	rec := doGet(t, h, "/topics/orders")
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	mustContain(t, rec, "dial tcp timeout")
}

func TestMessagesBrowse(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		browse: func(ctx context.Context, req kafka.BrowseRequest) (kafka.BrowseResult, error) {
			if req.Topic != "orders" || req.Partition != 2 || req.Count != 10 {
				t.Errorf("unexpected request: %+v", req)
			}
			return kafka.BrowseResult{
				Messages: []kafka.MessageView{
					{
						Partition: 2, Offset: 42, Time: time.Now(),
						Key:   kafka.PayloadView{Text: "user-1", Encoding: "text"},
						Value: kafka.PayloadView{Text: "{\n  \"total\": 9\n}", Encoding: "json"},
						Size:  32,
						Headers: []kafka.HeaderView{
							{Key: "trace-id", Value: kafka.PayloadView{Text: "abc", Encoding: "text"}},
						},
					},
				},
				Partition:      2,
				PartitionCount: 6,
				FirstOffset:    0,
				LastOffset:     100,
				StartOffset:    42,
				NextOffset:     43,
				Truncated:      true,
			}, nil
		},
	})
	rec := doGet(t, h, "/topics/orders/messages?partition=2&offset=42&count=10")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	mustContain(t, rec, "#42", "user-1", "trace-id", "继续向后读取", "&#34;total&#34;: 9")
	s := body(t, rec)
	if !strings.Contains(s, `value="2" selected`) {
		t.Error("partition 2 should be selected")
	}
	if !strings.Contains(s, `value="custom" checked`) {
		t.Error("numeric offset should render custom mode")
	}
}

func TestMessagesFlash(t *testing.T) {
	h := newTestServer(t, &fakeProbe{})
	rec := doGet(t, h, "/topics/orders/messages?produced=3:128")
	mustContain(t, rec, "消息已发送", "partition 3", "offset 128")
}

func TestMessagesInvalidOffset(t *testing.T) {
	h := newTestServer(t, &fakeProbe{})
	rec := doGet(t, h, "/topics/orders/messages?offset=custom&custom_offset=abc")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	mustContain(t, rec, "offset 必须是非负整数")
}

func TestMessagesProbeErrorStillRenders(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		browse: func(ctx context.Context, req kafka.BrowseRequest) (kafka.BrowseResult, error) {
			return kafka.BrowseResult{}, errors.New("no leader")
		},
	})
	rec := doGet(t, h, "/topics/orders/messages")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	mustContain(t, rec, "读取消息失败", "no leader")
}

func TestProduceSuccessRedirectsToMessage(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		topicPartitions: func(ctx context.Context, name string) (int, error) {
			return 6, nil
		},
		produce: func(ctx context.Context, req kafka.ProduceRequest) (kafka.ProduceResult, error) {
			if req.Topic != "orders" || req.Partition != 1 || req.Key != "k1" || req.Value != "hello" {
				t.Errorf("unexpected produce request: %+v", req)
			}
			if len(req.Headers) != 1 || req.Headers[0].Key != "h1" {
				t.Errorf("headers = %+v", req.Headers)
			}
			return kafka.ProduceResult{Partition: 1, Offset: 100, Time: time.Now()}, nil
		},
	})
	form := url.Values{
		"partition":    {"1"},
		"key":          {"k1"},
		"value":        {"hello"},
		"header_key":   {"h1"},
		"header_value": {"hv"},
	}
	req := httptest.NewRequest(http.MethodPost, "/topics/orders/produce",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303; body=%s", rec.Code, body(t, rec))
	}
	loc := rec.Header().Get("Location")
	if !strings.Contains(loc, "/topics/orders/messages") ||
		!strings.Contains(loc, "partition=1") ||
		!strings.Contains(loc, "offset=100") ||
		!strings.Contains(loc, "produced=1:100") {
		t.Errorf("Location = %q", loc)
	}
}

func TestProduceEmptyValueRejected(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		topicPartitions: func(ctx context.Context, name string) (int, error) { return 6, nil },
	})
	form := url.Values{"partition": {"-1"}, "key": {"keepme"}, "value": {""}}
	req := httptest.NewRequest(http.MethodPost, "/topics/orders/produce",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	mustContain(t, rec, "不能为空", "keepme") // 用户输入被保留
}

func TestProducePartitionOutOfRange(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		topicPartitions: func(ctx context.Context, name string) (int, error) { return 3, nil },
	})
	form := url.Values{"partition": {"9"}, "value": {"x"}}
	req := httptest.NewRequest(http.MethodPost, "/topics/orders/produce",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	mustContain(t, rec, "超出范围")
}

func TestProduceFormPage(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		topicPartitions: func(ctx context.Context, name string) (int, error) { return 4, nil },
	})
	rec := doGet(t, h, "/topics/orders/produce")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	mustContain(t, rec, "发送消息", "自动（key 哈希 / 轮询）", `value="3"`)
}

func TestGroupsList(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		listGroups: func(ctx context.Context) ([]kafka.GroupSummary, error) {
			return []kafka.GroupSummary{
				{ID: "billing", State: "Stable", Members: 2, Topics: 1, Lag: 900, Coordinator: "b1:9092"},
				{ID: "idle-group", State: "Empty", Members: 0, CoordinatorID: 2},
			}, nil
		},
	})
	rec := doGet(t, h, "/groups")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	mustContain(t, rec, "billing", "idle-group", "900", "Stable", "Empty")
}

func TestGroupDetail(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		groupDetail: func(ctx context.Context, id string) (kafka.GroupDetail, error) {
			return kafka.GroupDetail{
				ID:           "billing",
				State:        "Stable",
				ProtocolType: "consumer",
				Coordinator:  "b1:9092",
				Members: []kafka.GroupMember{
					{
						MemberID: "m-1", ClientID: "app-1", Host: "/10.0.0.5",
						Assignments: []kafka.Assignment{{Topic: "orders", Partitions: []int{0, 1}}},
					},
				},
				Partitions: []kafka.GroupPartition{
					{Topic: "orders", Partition: 0, HasCommit: true, CommittedOffset: 90, EndOffset: 100, Lag: 10},
					{Topic: "orders", Partition: 1, HasCommit: false, EndOffset: 55},
				},
				TotalLag: 10,
			}, nil
		},
	})
	rec := doGet(t, h, "/groups/billing")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	mustContain(t, rec, "billing", "app-1", "orders: 0, 1", "总 lag 10", "Stable")
}

func TestGroupNotFound(t *testing.T) {
	h := newTestServer(t, &fakeProbe{
		groupDetail: func(ctx context.Context, id string) (kafka.GroupDetail, error) {
			return kafka.GroupDetail{}, kafka.ErrNotFound
		},
	})
	rec := doGet(t, h, "/groups/ghost")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	mustContain(t, rec, "Consumer Group 不存在")
}

func TestHealthz(t *testing.T) {
	h := newTestServer(t, &fakeProbe{})
	rec := doGet(t, h, "/healthz")
	if rec.Code != http.StatusOK || body(t, rec) != "ok" {
		t.Errorf("healthz = %d %q", rec.Code, body(t, rec))
	}
}

func TestStaticCSS(t *testing.T) {
	h := newTestServer(t, &fakeProbe{})
	rec := doGet(t, h, "/static/style.css")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/css") {
		t.Errorf("Content-Type = %q", ct)
	}
	if !strings.Contains(body(t, rec), ".topbar") {
		t.Error("css content missing")
	}
}

func TestUnknownRoute404(t *testing.T) {
	h := newTestServer(t, &fakeProbe{})
	rec := doGet(t, h, "/no/such/page")
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}
