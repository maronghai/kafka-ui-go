package web

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"kafka-ui-go/internal/kafka"
)

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	data := homeData{base: base{Title: "概览", Active: "home"}}
	ctx, cancel := s.reqCtx(r)
	defer cancel()
	info, err := s.probe.Overview(ctx)
	if err != nil {
		data.base.Err = bannerErr("读取集群概览失败", err)
	} else {
		data.Info = &info
	}
	s.render(w, http.StatusOK, "home", data)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	data := topicsData{
		base:  base{Title: "Topics", Active: "topics"},
		Query: strings.TrimSpace(r.URL.Query().Get("q")),
	}
	ctx, cancel := s.reqCtx(r)
	defer cancel()
	list, err := s.probe.ListTopics(ctx)
	if err != nil {
		data.base.Err = bannerErr("读取 topic 列表失败", err)
	} else if q := data.Query; q != "" {
		ql := strings.ToLower(q)
		for _, t := range list {
			if strings.Contains(strings.ToLower(t.Name), ql) {
				data.Topics = append(data.Topics, t)
			}
		}
	} else {
		data.Topics = list
	}
	s.render(w, http.StatusOK, "topics", data)
}

func (s *Server) handleTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("topic")
	ctx, cancel := s.reqCtx(r)
	defer cancel()
	detail, err := s.probe.TopicDetail(ctx, name)
	if err != nil {
		if errors.Is(err, kafka.ErrNotFound) {
			s.renderError(w, http.StatusNotFound, "Topic 不存在", fmt.Sprintf("找不到 topic %q。", name))
			return
		}
		s.renderError(w, http.StatusBadGateway, "无法读取 Topic", bannerErr(fmt.Sprintf("读取 topic %q 失败", name), err))
		return
	}
	s.render(w, http.StatusOK, "topic", topicData{
		base:   base{Title: name, Active: "topics"},
		Detail: &detail,
	})
}

// browseCount 从查询参数读取条数并收敛到合法范围。
func browseCount(raw string) int {
	n := intParam(raw, 20)
	if n < 1 {
		n = 20
	}
	if n > kafka.MaxBrowseCount {
		n = kafka.MaxBrowseCount
	}
	return n
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	q := r.URL.Query()
	partition := intParam(q.Get("partition"), 0)
	if partition < 0 {
		partition = 0
	}
	count := browseCount(q.Get("count"))
	off, mode, input, offErr := parseOffsetParams(q.Get("offset"), q.Get("custom_offset"))

	data := messagesData{
		base:        base{Title: "消息浏览", Active: "topics"},
		Topic:       topic,
		Partition:   partition,
		OffsetMode:  mode,
		OffsetInput: input,
		Count:       count,
	}
	if p, o, ok := parseProduced(q.Get("produced")); ok {
		data.base.Flash = fmt.Sprintf("消息已发送 · partition %s · offset %s", p, o)
	}
	if offErr != nil {
		data.base.Err = offErr.Error()
	}

	ctx, cancel := s.reqCtx(r)
	defer cancel()
	res, err := s.probe.Browse(ctx, kafka.BrowseRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    off,
		Count:     count,
	})
	switch {
	case err != nil && errors.Is(err, kafka.ErrNotFound):
		s.renderError(w, http.StatusNotFound, "无法浏览消息", err.Error())
		return
	case err != nil:
		if data.base.Err == "" {
			data.base.Err = bannerErr("读取消息失败", err)
		}
		// 元数据不可用时也渲染出分区下拉框，便于重试。
		data.PartitionCount = partition + 1
	default:
		data.Result = &res
		data.PartitionCount = res.PartitionCount
		data.Partition = res.Partition
		if res.Note != "" && data.base.Err == "" {
			data.base.Err = res.Note
		}
	}
	s.render(w, http.StatusOK, "messages", data)
}

func (s *Server) handleProduceForm(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	data := produceData{
		base:          base{Title: "发送消息", Active: "topics"},
		Topic:         topic,
		FormPartition: "-1",
	}
	if e := strings.TrimSpace(r.URL.Query().Get("err")); e != "" {
		data.base.Err = e
	}
	ctx, cancel := s.reqCtx(r)
	defer cancel()
	n, err := s.probe.TopicPartitions(ctx, topic)
	if err != nil {
		if errors.Is(err, kafka.ErrNotFound) {
			s.renderError(w, http.StatusNotFound, "Topic 不存在", fmt.Sprintf("找不到 topic %q。", topic))
			return
		}
		s.renderError(w, http.StatusBadGateway, "无法读取 Topic", bannerErr(fmt.Sprintf("读取 topic %q 失败", topic), err))
		return
	}
	data.PartitionCount = n
	s.render(w, http.StatusOK, "produce", data)
}

// renderProduceWithForm POST 失败时重渲染表单，保留用户输入。
func (s *Server) renderProduceWithForm(w http.ResponseWriter, r *http.Request, status int, data produceData) {
	ctx, cancel := s.reqCtx(r)
	defer cancel()
	if n, err := s.probe.TopicPartitions(ctx, data.Topic); err == nil {
		data.PartitionCount = n
	}
	s.render(w, status, "produce", data)
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "表单解析失败: "+err.Error(), http.StatusBadRequest)
		return
	}

	data := produceData{
		base:            base{Title: "发送消息", Active: "topics"},
		Topic:           topic,
		FormKey:         r.FormValue("key"),
		FormValue:       r.FormValue("value"),
		FormPartition:   r.FormValue("partition"),
		FormHeaderKey:   r.FormValue("header_key"),
		FormHeaderValue: r.FormValue("header_value"),
	}
	if data.FormPartition == "" {
		data.FormPartition = "-1"
	}

	// ---- 表单校验（400） ----
	if data.FormValue == "" {
		data.base.Err = "消息内容（value）不能为空"
		s.renderProduceWithForm(w, r, http.StatusBadRequest, data)
		return
	}
	partition := -1
	if p := data.FormPartition; p != "" && p != "-1" {
		v, err := parsePositiveInt(p)
		if err != nil {
			data.base.Err = "分区号必须是非负整数"
			s.renderProduceWithForm(w, r, http.StatusBadRequest, data)
			return
		}
		partition = v
	}

	ctx, cancel := s.reqCtx(r)
	defer cancel()

	// 校验 topic 与分区范围，错误信息比 broker 原始报错更友好。
	n, err := s.probe.TopicPartitions(ctx, topic)
	if err != nil {
		if errors.Is(err, kafka.ErrNotFound) {
			s.renderError(w, http.StatusNotFound, "Topic 不存在", fmt.Sprintf("找不到 topic %q。", topic))
			return
		}
		s.renderError(w, http.StatusBadGateway, "无法读取 Topic", bannerErr(fmt.Sprintf("读取 topic %q 失败", topic), err))
		return
	}
	if partition >= n {
		data.base.Err = fmt.Sprintf("分区 %d 超出范围 [0, %d)", partition, n-1)
		s.renderProduceWithForm(w, r, http.StatusBadRequest, data)
		return
	}

	req := kafka.ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Key:       data.FormKey,
		Value:     data.FormValue,
	}
	if hk := data.FormHeaderKey; hk != "" {
		req.Headers = append(req.Headers, kafka.ProduceHeader{Key: hk, Value: data.FormHeaderValue})
	}
	res, err := s.probe.Produce(ctx, req)
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, kafka.ErrNotFound) {
			status = http.StatusNotFound
		}
		data.base.Err = bannerErr("发送消息失败", err)
		s.renderProduceWithForm(w, r, status, data)
		return
	}

	target := fmt.Sprintf("/topics/%s/messages?partition=%d&offset=%d&count=20&produced=%d:%d",
		url.PathEscape(topic), res.Partition, res.Offset, res.Partition, res.Offset)
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (s *Server) handleGroups(w http.ResponseWriter, r *http.Request) {
	data := groupsData{base: base{Title: "Consumer Groups", Active: "groups"}}
	ctx, cancel := s.reqCtx(r)
	defer cancel()
	list, err := s.probe.ListGroups(ctx)
	if err != nil {
		data.base.Err = bannerErr("读取 consumer group 列表失败", err)
	} else {
		data.Groups = list
	}
	s.render(w, http.StatusOK, "groups", data)
}

func (s *Server) handleGroup(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("group")
	ctx, cancel := s.reqCtx(r)
	defer cancel()
	detail, err := s.probe.GroupDetail(ctx, id)
	if err != nil {
		if errors.Is(err, kafka.ErrNotFound) {
			s.renderError(w, http.StatusNotFound, "Consumer Group 不存在", fmt.Sprintf("找不到 group %q。", id))
			return
		}
		s.renderError(w, http.StatusBadGateway, "无法读取 Consumer Group", bannerErr(fmt.Sprintf("读取 group %q 失败", id), err))
		return
	}
	s.render(w, http.StatusOK, "group", groupData{
		base:   base{Title: id, Active: "groups"},
		Detail: &detail,
	})
}

func parsePositiveInt(s string) (int, error) {
	var n int
	if s == "" {
		return 0, errors.New("empty")
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errors.New("not a number")
		}
		n = n*10 + int(c-'0')
		if n > 1<<30 {
			return 0, errors.New("out of range")
		}
	}
	return n, nil
}
