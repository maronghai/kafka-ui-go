package web

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"time"

	"kafka-ui-go/internal/kafka"
)

//go:embed templates/*.gohtml static/*
var assets embed.FS

// Probe 是 web 层依赖的 Kafka 查询接口；测试中用 fake 实现替换。
type Probe interface {
	Overview(ctx context.Context) (kafka.ClusterInfo, error)
	ListTopics(ctx context.Context) ([]kafka.TopicSummary, error)
	TopicPartitions(ctx context.Context, name string) (int, error)
	TopicDetail(ctx context.Context, name string) (kafka.TopicDetail, error)
	Browse(ctx context.Context, req kafka.BrowseRequest) (kafka.BrowseResult, error)
	Produce(ctx context.Context, req kafka.ProduceRequest) (kafka.ProduceResult, error)
	ListGroups(ctx context.Context) ([]kafka.GroupSummary, error)
	GroupDetail(ctx context.Context, id string) (kafka.GroupDetail, error)
}

// Server 提供 HTTP 接口与页面渲染。
type Server struct {
	probe   Probe
	tmpl    *template.Template
	timeout time.Duration
}

// New 解析内嵌模板并构造 Server。timeout 作用于每个 handler 的 Kafka 调用。
func New(probe Probe, timeout time.Duration) (*Server, error) {
	tmpl, err := template.New("").Funcs(templateFuncs()).ParseFS(assets, "templates/*.gohtml")
	if err != nil {
		return nil, fmt.Errorf("解析模板失败: %w", err)
	}
	return &Server{probe: probe, tmpl: tmpl, timeout: timeout}, nil
}

// Handler 返回完整路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleHome)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /topics", s.handleTopics)
	mux.HandleFunc("GET /topics/{topic}", s.handleTopic)
	mux.HandleFunc("GET /topics/{topic}/messages", s.handleMessages)
	mux.HandleFunc("GET /topics/{topic}/produce", s.handleProduceForm)
	mux.HandleFunc("POST /topics/{topic}/produce", s.handleProduce)
	mux.HandleFunc("GET /groups", s.handleGroups)
	mux.HandleFunc("GET /groups/{group}", s.handleGroup)
	if static, err := fs.Sub(assets, "static"); err == nil {
		mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))
	}
	return mux
}

// reqCtx 为一次请求派生带超时的 Kafka 调用上下文。
func (s *Server) reqCtx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), s.timeout)
}

// render 先在缓冲区渲染模板，成功后才写出状态码。
func (s *Server) render(w http.ResponseWriter, status int, name string, data any) {
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		http.Error(w, "render error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

// renderError 渲染统一错误页。
func (s *Server) renderError(w http.ResponseWriter, status int, heading, message string) {
	s.render(w, status, "error", errorData{
		base:    base{Title: heading},
		Heading: heading,
		Message: message,
	})
}
