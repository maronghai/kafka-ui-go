package kafka

import (
	"context"
	"fmt"
	"sort"

	kgo "github.com/segmentio/kafka-go"
)

// BrokerInfo 描述单个 broker。
type BrokerInfo struct {
	ID   int
	Addr string
	Rack string
}

// ClusterInfo 是首页展示的集群概览。
type ClusterInfo struct {
	ClusterID      string
	ControllerID   int
	Brokers        []BrokerInfo
	TopicCount     int
	PartitionCount int
	// GroupCount 为 -1 表示无法统计（查询失败）。
	GroupCount int
}

// TopicSummary 是 topic 列表页的一行。
type TopicSummary struct {
	Name              string
	Internal          bool
	Partitions        int
	ReplicationFactor int
	UnderReplicated   bool
}

// PartitionInfo 描述一个分区的拓扑与 offset 边界。
type PartitionInfo struct {
	ID          int
	LeaderID    int
	LeaderAddr  string
	Replicas    []int
	ISR         []int
	Offline     []int
	FirstOffset int64
	LastOffset  int64
}

// ConfigEntry 是 topic 的一项配置。
type ConfigEntry struct {
	Name      string
	Value     string
	IsDefault bool
	ReadOnly  bool
	Sensitive bool
}

// TopicDetail 是 topic 详情页的数据。
type TopicDetail struct {
	Name              string
	Internal          bool
	ReplicationFactor int
	UnderReplicated   bool
	Partitions        []PartitionInfo
	Config            []ConfigEntry
	// Notes 记录非致命错误（如 offset / 配置读取失败）。
	Notes         []string
	TotalMessages int64
}

// Overview 返回集群概览；group 统计为 best effort，失败不影响其余数据。
func (c *Client) Overview(ctx context.Context) (ClusterInfo, error) {
	meta, err := c.metadata(ctx)
	if err != nil {
		return ClusterInfo{}, err
	}
	info := ClusterInfo{
		ClusterID:    meta.ClusterID,
		ControllerID: meta.Controller.ID,
		TopicCount:   len(meta.Topics),
		GroupCount:   -1,
	}
	for _, b := range meta.Brokers {
		info.Brokers = append(info.Brokers, BrokerInfo{ID: b.ID, Addr: joinHostPort(b), Rack: b.Rack})
	}
	for _, t := range meta.Topics {
		info.PartitionCount += len(t.Partitions)
	}
	sort.Slice(info.Brokers, func(i, j int) bool { return info.Brokers[i].ID < info.Brokers[j].ID })
	if refs, err := c.listGroups(ctx, meta); err == nil {
		info.GroupCount = len(refs)
	}
	return info, nil
}

// ListTopics 返回全部 topic（按名称排序）。
func (c *Client) ListTopics(ctx context.Context) ([]TopicSummary, error) {
	meta, err := c.metadata(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]TopicSummary, 0, len(meta.Topics))
	for _, t := range meta.Topics {
		if t.Error != nil {
			continue
		}
		s := TopicSummary{Name: t.Name, Internal: t.Internal, Partitions: len(t.Partitions)}
		for _, p := range t.Partitions {
			if n := len(p.Replicas); n > s.ReplicationFactor {
				s.ReplicationFactor = n
			}
			if len(p.Isr) < len(p.Replicas) {
				s.UnderReplicated = true
			}
		}
		out = append(out, s)
	}
	sortByName(out)
	return out, nil
}

// TopicPartitions 返回 topic 的分区数（轻量查询，供表单渲染使用）。
func (c *Client) TopicPartitions(ctx context.Context, name string) (int, error) {
	meta, err := c.metadata(ctx, name)
	if err != nil {
		return 0, err
	}
	topic := findTopic(meta, name)
	if topic == nil || topic.Error != nil {
		return 0, fmt.Errorf("%w: topic %q", ErrNotFound, name)
	}
	return len(topic.Partitions), nil
}

// TopicDetail 返回 topic 详情：分区拓扑 + offset 边界 + 配置。
// offset 或配置读取失败只记入 Notes，不视为致命错误。
func (c *Client) TopicDetail(ctx context.Context, name string) (TopicDetail, error) {
	meta, err := c.metadata(ctx, name)
	if err != nil {
		return TopicDetail{}, err
	}
	topic := findTopic(meta, name)
	if topic == nil || topic.Error != nil {
		return TopicDetail{}, fmt.Errorf("%w: topic %q", ErrNotFound, name)
	}

	d := TopicDetail{Name: name, Internal: topic.Internal}
	for _, p := range topic.Partitions {
		pi := PartitionInfo{ID: p.ID, LeaderID: p.Leader.ID, LeaderAddr: joinHostPort(p.Leader)}
		for _, o := range p.OfflineReplicas {
			pi.Offline = append(pi.Offline, o.ID)
		}
		for _, r := range p.Replicas {
			pi.Replicas = append(pi.Replicas, r.ID)
		}
		for _, i := range p.Isr {
			pi.ISR = append(pi.ISR, i.ID)
		}
		if n := len(p.Replicas); n > d.ReplicationFactor {
			d.ReplicationFactor = n
		}
		if len(p.Isr) < len(p.Replicas) {
			d.UnderReplicated = true
		}
		d.Partitions = append(d.Partitions, pi)
	}
	sort.Slice(d.Partitions, func(i, j int) bool { return d.Partitions[i].ID < d.Partitions[j].ID })

	if len(d.Partitions) > 0 {
		req := make([]kgo.OffsetRequest, 0, len(d.Partitions)*2)
		for _, p := range d.Partitions {
			req = append(req, kgo.FirstOffsetOf(p.ID), kgo.LastOffsetOf(p.ID))
		}
		resp, err := c.api.ListOffsets(ctx, &kgo.ListOffsetsRequest{
			Topics: map[string][]kgo.OffsetRequest{name: req},
		})
		if err != nil {
			d.Notes = append(d.Notes, fmt.Sprintf("读取分区 offset 失败：%v", err))
		} else {
			for _, po := range resp.Topics[name] {
				if po.Error != nil {
					d.Notes = append(d.Notes, fmt.Sprintf("分区 %d 读取 offset 失败：%v", po.Partition, po.Error))
					continue
				}
				for i := range d.Partitions {
					if d.Partitions[i].ID != po.Partition {
						continue
					}
					if po.FirstOffset >= 0 {
						d.Partitions[i].FirstOffset = po.FirstOffset
					}
					if po.LastOffset >= 0 {
						d.Partitions[i].LastOffset = po.LastOffset
					}
				}
			}
		}
	}
	for _, p := range d.Partitions {
		if n := p.LastOffset - p.FirstOffset; n > 0 {
			d.TotalMessages += n
		}
	}

	cResp, err := c.api.DescribeConfigs(ctx, &kgo.DescribeConfigsRequest{
		Resources: []kgo.DescribeConfigRequestResource{
			{ResourceType: kgo.ResourceTypeTopic, ResourceName: name},
		},
	})
	if err != nil {
		d.Notes = append(d.Notes, fmt.Sprintf("读取 topic 配置失败：%v", err))
	} else {
		for _, res := range cResp.Resources {
			if res.Error != nil {
				d.Notes = append(d.Notes, fmt.Sprintf("读取 topic 配置失败：%v", res.Error))
				continue
			}
			for _, e := range res.ConfigEntries {
				d.Config = append(d.Config, ConfigEntry{
					Name:      e.ConfigName,
					Value:     e.ConfigValue,
					IsDefault: e.IsDefault,
					ReadOnly:  e.ReadOnly,
					Sensitive: e.IsSensitive,
				})
			}
		}
		sort.Slice(d.Config, func(i, j int) bool { return d.Config[i].Name < d.Config[j].Name })
	}
	return d, nil
}
