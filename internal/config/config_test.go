package config

import (
	"reflect"
	"testing"
	"time"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse(nil) error: %v", err)
	}
	if !reflect.DeepEqual(cfg.Brokers, []string{"localhost:9092"}) {
		t.Errorf("Brokers = %v", cfg.Brokers)
	}
	if cfg.Addr != ":8080" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	if cfg.Timeout != 10*time.Second {
		t.Errorf("Timeout = %v", cfg.Timeout)
	}
}

func TestParseFlags(t *testing.T) {
	cfg, err := Parse([]string{
		"-brokers", " b1:9092 , b2:9092 ,",
		"-addr", "127.0.0.1:9999",
		"-timeout", "5s",
	})
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	want := []string{"b1:9092", "b2:9092"}
	if !reflect.DeepEqual(cfg.Brokers, want) {
		t.Errorf("Brokers = %v, want %v", cfg.Brokers, want)
	}
	if cfg.Addr != "127.0.0.1:9999" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	if cfg.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v", cfg.Timeout)
	}
}

func TestParseEnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "env1:9092,env2:9092")
	t.Setenv("KAFKA_UI_ADDR", ":7070")
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	want := []string{"env1:9092", "env2:9092"}
	if !reflect.DeepEqual(cfg.Brokers, want) {
		t.Errorf("Brokers = %v, want %v", cfg.Brokers, want)
	}
	if cfg.Addr != ":7070" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
}

func TestParseEmptyBrokers(t *testing.T) {
	if _, err := Parse([]string{"-brokers", " , "}); err == nil {
		t.Fatal("expected error for empty brokers")
	}
}

func TestParseInvalidTimeout(t *testing.T) {
	if _, err := Parse([]string{"-timeout", "-1s"}); err == nil {
		t.Fatal("expected error for negative timeout")
	}
}
