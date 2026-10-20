package web

import (
	"strings"
	"testing"

	"kafka-ui-go/internal/kafka"
)

func TestHumanNum(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
		{-1234, "-1,234"},
		{int64(987654321), "987,654,321"},
		{"raw", "raw"},
	}
	for _, c := range cases {
		if got := humanNum(c.in); got != c.want {
			t.Errorf("humanNum(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1 << 20, "1.0 MB"},
		{int64(5) << 30, "5.0 GB"},
		{-1, "0 B"},
	}
	for _, c := range cases {
		if got := humanBytes(c.in); got != c.want {
			t.Errorf("humanBytes(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestTrunc(t *testing.T) {
	if got := trunc("hello", 10); got != "hello" {
		t.Errorf("trunc short = %q", got)
	}
	if got := trunc("hello world", 5); got != "hello…" {
		t.Errorf("trunc = %q", got)
	}
	if got := trunc("中文测试", 2); got != "中文…" {
		t.Errorf("trunc runes = %q", got)
	}
	if got := trunc("anything", 0); got != "anything" {
		t.Errorf("trunc n=0 = %q", got)
	}
}

func TestSeq(t *testing.T) {
	if got := seq(3); len(got) != 3 || got[0] != 0 || got[2] != 2 {
		t.Errorf("seq(3) = %v", got)
	}
	if got := seq(0); got != nil {
		t.Errorf("seq(0) = %v", got)
	}
	if got := seq(-5); got != nil {
		t.Errorf("seq(-5) = %v", got)
	}
	if got := seq(10000); len(got) != 4096 {
		t.Errorf("seq cap = %d", len(got))
	}
}

func TestStateClass(t *testing.T) {
	cases := map[string]string{
		"Stable":              "green",
		"Empty":               "gray",
		"PreparingRebalance":  "amber",
		"CompletingRebalance": "amber",
		"Dead":                "red",
		"Unknown":             "gray",
		"SomeNewState":        "blue",
	}
	for in, want := range cases {
		if got := stateClass(in); got != want {
			t.Errorf("stateClass(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestGroupCount(t *testing.T) {
	if got := groupCount(-1); got != "—" {
		t.Errorf("groupCount(-1) = %q", got)
	}
	if got := groupCount(12); got != "12" {
		t.Errorf("groupCount(12) = %q", got)
	}
}

func TestParseOffsetParams(t *testing.T) {
	cases := []struct {
		raw, custom string
		wantOff     int64
		wantMode    string
		wantErr     bool
	}{
		{"", "", kafka.OffsetEarliest, "earliest", false},
		{"earliest", "", kafka.OffsetEarliest, "earliest", false},
		{"latest", "", kafka.OffsetLatest, "latest", false},
		{"custom", "55", 55, "custom", false},
		{"123", "", 123, "custom", false},
		{"custom", "abc", 0, "earliest", true},
		{"custom", "-3", 0, "earliest", true},
		{"abc", "", 0, "earliest", true},
	}
	for _, c := range cases {
		off, mode, input, err := parseOffsetParams(c.raw, c.custom)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseOffsetParams(%q,%q) expected error", c.raw, c.custom)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseOffsetParams(%q,%q) error: %v", c.raw, c.custom, err)
			continue
		}
		if off != c.wantOff || mode != c.wantMode {
			t.Errorf("parseOffsetParams(%q,%q) = %d/%s, want %d/%s",
				c.raw, c.custom, off, mode, c.wantOff, c.wantMode)
		}
		if c.wantMode == "custom" && input == "" {
			t.Errorf("custom mode should echo input, got empty")
		}
	}
}

func TestParseProduced(t *testing.T) {
	if p, o, ok := parseProduced("3:128"); !ok || p != "3" || o != "128" {
		t.Errorf("parseProduced = %q/%q/%v", p, o, ok)
	}
	for _, bad := range []string{"", "128", ":5", "3:"} {
		if _, _, ok := parseProduced(bad); ok {
			t.Errorf("parseProduced(%q) should fail", bad)
		}
	}
}

func TestBrowseCount(t *testing.T) {
	cases := map[string]int{
		"":      20,
		"10":    10,
		"0":     20,
		"-3":    20,
		"abc":   20,
		"99999": kafka.MaxBrowseCount,
		"1000":  1000,
	}
	for in, want := range cases {
		if got := browseCount(in); got != want {
			t.Errorf("browseCount(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestBannerErr(t *testing.T) {
	got := bannerErr("读取失败", errFake("boom"))
	if !strings.Contains(got, "读取失败") || !strings.Contains(got, "boom") {
		t.Errorf("bannerErr = %q", got)
	}
}

type errFake string

func (e errFake) Error() string { return string(e) }
