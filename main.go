// kafka-ui-go 是一个单二进制的 Kafka Web UI：
// 服务端渲染 + embed 静态资源，基于 segmentio/kafka-go。
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"kafka-ui-go/internal/config"
	"kafka-ui-go/internal/kafka"
	"kafka-ui-go/internal/web"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		log.Fatalf("配置错误: %v", err)
	}

	probe := kafka.New(cfg.Brokers, cfg.Timeout)
	srv, err := web.New(probe, cfg.Timeout)
	if err != nil {
		log.Fatalf("初始化失败: %v", err)
	}

	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("Kafka UI 启动: http://%s   brokers=%s", displayAddr(cfg.Addr), strings.Join(cfg.Brokers, ","))
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if info, err := probe.Overview(pingCtx); err != nil {
			log.Printf("警告: 暂时无法连接 Kafka（%v），页面将显示错误提示", err)
		} else {
			log.Printf("已连接集群 %q: %d brokers, %d topics",
				orDash(info.ClusterID), len(info.Brokers), info.TopicCount)
		}
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务异常退出: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("正在关闭…")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		log.Printf("关闭 HTTP 服务失败: %v", err)
	}
}

// displayAddr 把 ":8080" 形式补全为可点击的 host:port。
func displayAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "localhost" + addr
	}
	return addr
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}
