package kafka

import (
	"testing"
	"time"
)

// TestClientAtomicAlignment 在 32 位平台（windows/386）上，
// 64 位原子操作要求 8 字节对齐；若 rrSeq 是未对齐的字段会直接 panic。
// 该测试在 64 位平台上是无害的冒烟。
func TestClientAtomicAlignment(t *testing.T) {
	c := New([]string{"localhost:9092"}, time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			c.rrSeq.Add(1)
		}
	}()
	<-done
	if got := c.rrSeq.Load(); got != 1000 {
		t.Errorf("rrSeq = %d, want 1000", got)
	}
}

func TestBrokersCopy(t *testing.T) {
	c := New([]string{"a:9092", "b:9092"}, time.Second)
	got := c.Brokers()
	if len(got) != 2 || got[0] != "a:9092" || got[1] != "b:9092" {
		t.Errorf("Brokers() = %v", got)
	}
	got[0] = "mutated"
	if c.Brokers()[0] != "a:9092" {
		t.Error("Brokers() should return a copy")
	}
}
