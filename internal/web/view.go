package web

import (
	"errors"
	"fmt"
	"html/template"
	"strconv"
	"strings"
	"time"

	"kafka-ui-go/internal/kafka"
)

// base 是所有页面共享的模板数据（嵌入各页面 data 结构体顶层）。
type base struct {
	Title  string
	Active string // home | topics | groups
	Err    string // 非致命错误横幅
	Flash  string // 成功提示横幅
}

type homeData struct {
	base
	Info *kafka.ClusterInfo
}

type topicsData struct {
	base
	Topics []kafka.TopicSummary
	Query  string
}

type topicData struct {
	base
	Detail *kafka.TopicDetail
}

type messagesData struct {
	base
	Topic          string
	PartitionCount int
	Partition      int
	OffsetMode     string // earliest | latest | custom
	OffsetInput    string // custom 模式回显
	Count          int
	Result         *kafka.BrowseResult
}

type produceData struct {
	base
	Topic           string
	PartitionCount  int
	FormKey         string
	FormValue       string
	FormPartition   string
	FormHeaderKey   string
	FormHeaderValue string
}

type groupsData struct {
	base
	Groups []kafka.GroupSummary
}

type groupData struct {
	base
	Detail *kafka.GroupDetail
}

type errorData struct {
	base
	Heading string
	Message string
}

func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"humanNum":   humanNum,
		"humanBytes": humanBytes,
		"ts":         formatTime,
		"trunc":      trunc,
		"seq":        seq,
		"stateClass": stateClass,
		"groupCount": groupCount,
		"sub":        func(a, b int64) int64 { return a - b },
	}
}

// humanNum 千分位格式化整数；无法转换时原样输出。
func humanNum(v any) string {
	n, ok := toInt64(v)
	if !ok {
		return fmt.Sprint(v)
	}
	neg := n < 0
	var u uint64
	if neg {
		u = uint64(-(n + 1)) + 1
	} else {
		u = uint64(n)
	}
	s := strconv.FormatUint(u, 10)
	var b strings.Builder
	b.Grow(len(s) + len(s)/3)
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteByte(s[i])
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

// humanBytes 把字节数格式化为人类可读单位。
func humanBytes(v any) string {
	n, ok := toInt64(v)
	if !ok {
		return fmt.Sprint(v)
	}
	if n < 0 {
		return "0 B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 5; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// formatTime 展示本地时间；零值显示 —。
func formatTime(v any) string {
	t, ok := v.(time.Time)
	if !ok || t.IsZero() {
		return "—"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

// trunc 按 rune 截断，超出加省略号。
func trunc(s string, n int) string {
	if n <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// seq 返回 [0, n)。
func seq(n int) []int {
	if n <= 0 {
		return nil
	}
	if n > 4096 {
		n = 4096
	}
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// stateClass 把 group 状态映射为 badge 颜色类。
func stateClass(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "stable":
		return "green"
	case "empty":
		return "gray"
	case "preparingrebalance", "completingrebalance", "rebalancing":
		return "amber"
	case "dead":
		return "red"
	case "unknown":
		return "gray"
	default:
		return "blue"
	}
}

// groupCount 渲染 group 数；-1 表示不可知。
func groupCount(n int) string {
	if n < 0 {
		return "—"
	}
	return humanNum(n)
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	default:
		return 0, false
	}
}

// parseOffsetParams 解析 offset 查询参数。
// raw ∈ {"", "earliest", "latest", "custom", "<数字>"}；
// mode 返回 earliest/latest/custom；custom_offset 是 custom 模式的值。
func parseOffsetParams(raw, custom string) (off int64, mode, input string, err error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "earliest":
		return kafka.OffsetEarliest, "earliest", "", nil
	case "latest":
		return kafka.OffsetLatest, "latest", "", nil
	case "custom":
		v, e := strconv.ParseInt(strings.TrimSpace(custom), 10, 64)
		if e != nil || v < 0 {
			return kafka.OffsetEarliest, "earliest", "", errors.New("offset 必须是非负整数")
		}
		return v, "custom", strconv.FormatInt(v, 10), nil
	}
	v, e := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if e != nil || v < 0 {
		return kafka.OffsetEarliest, "earliest", "", errors.New("offset 必须是 earliest、latest 或非负整数")
	}
	return v, "custom", strconv.FormatInt(v, 10), nil
}

// parseProduced 解析 produced=P:O 闪存参数。
func parseProduced(v string) (partition, offset string, ok bool) {
	i := strings.IndexByte(v, ':')
	if i <= 0 || i == len(v)-1 {
		return "", "", false
	}
	return v[:i], v[i+1:], true
}

func intParam(raw string, def int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return def
	}
	return n
}

func bannerErr(prefix string, err error) string {
	return fmt.Sprintf("%s：%v", prefix, err)
}
