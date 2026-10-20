package config

import (
	"errors"
	"flag"
	"os"
	"strings"
	"time"
)

// Config 是 kafka-ui-go 的运行时配置。
type Config struct {
	// Brokers 是逗号分隔的 bootstrap broker 地址列表。
	Brokers []string
	// Addr 是 HTTP 服务监听地址。
	Addr string
	// Timeout 是单次 Kafka 请求的超时时间。
	Timeout time.Duration
}

// Parse 解析命令行参数，环境变量作为默认值：
//
//	KAFKA_BROKERS   bootstrap brokers，默认 localhost:9092
//	KAFKA_UI_ADDR   监听地址，默认 :8080
//	KAFKA_UI_TIMEOUT 请求超时，默认 10s
func Parse(args []string) (*Config, error) {
	fs := flag.NewFlagSet("kafka-ui-go", flag.ContinueOnError)
	brokers := fs.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "comma separated kafka bootstrap brokers")
	addr := fs.String("addr", envOr("KAFKA_UI_ADDR", ":8080"), "http listen address")
	timeout := fs.Duration("timeout", envDurationOr("KAFKA_UI_TIMEOUT", 10*time.Second), "kafka request timeout")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg := &Config{Addr: *addr, Timeout: *timeout}
	for _, b := range strings.Split(*brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.Brokers = append(cfg.Brokers, b)
		}
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("至少需要一个 broker 地址（-brokers 或 KAFKA_BROKERS）")
	}
	if cfg.Addr == "" {
		return nil, errors.New("监听地址不能为空（-addr）")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("请求超时必须为正数（-timeout）")
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}
