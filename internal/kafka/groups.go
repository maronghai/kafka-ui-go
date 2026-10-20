package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"

	kgo "github.com/segmentio/kafka-go"
)

// groupRef 是 consumer group 的最小标识。
type groupRef struct {
	ID            string
	CoordinatorID int
	ProtocolType  string
}

// GroupSummary 是 consumer group 列表页的一行。
type GroupSummary struct {
	ID            string
	State         string
	Members       int
	ProtocolType  string
	CoordinatorID int
	Coordinator   string
	Topics        int
	Lag           int64
}

// Assignment 是单个成员的分区分配。
type Assignment struct {
	Topic      string
	Partitions []int
}

// GroupMember 描述 consumer group 的一个成员。
type GroupMember struct {
	MemberID    string
	ClientID    string
	Host        string
	Assignments []Assignment
}

// GroupPartition 是 group 在一个分区上的消费进度。
type GroupPartition struct {
	Topic     string
	Partition int
	// HasCommit 为 false 表示该分区尚无提交 offset。
	HasCommit       bool
	CommittedOffset int64
	EndOffset       int64
	Lag             int64
	Metadata        string
}

// GroupDetail 是 consumer group 详情页的数据。
type GroupDetail struct {
	ID            string
	State         string
	ProtocolType  string
	CoordinatorID int
	Coordinator   string
	Members       []GroupMember
	Partitions    []GroupPartition
	TotalLag      int64
	Notes         []string
}

// groupStateInfo 是 DescribeGroups 的聚合结果。
type groupStateInfo struct {
	State   string
	Members int
}

// listGroups 逐 broker 发起 ListGroups 并按 group ID 去重，
// 返回 group 标识（含 coordinator broker ID）。个别 broker 失败可容忍。
func (c *Client) listGroups(ctx context.Context, meta *kgo.MetadataResponse) ([]groupRef, error) {
	if len(meta.Brokers) == 0 {
		return nil, errors.New("集群中没有可用 broker")
	}
	seen := make(map[string]bool)
	var out []groupRef
	var errs []error
	ok := 0

	for _, b := range meta.Brokers {
		resp, err := c.api.ListGroups(ctx, &kgo.ListGroupsRequest{Addr: kgo.TCP(joinHostPort(b))})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if resp.Error != nil {
			errs = append(errs, resp.Error)
			continue
		}
		ok++
		for _, g := range resp.Groups {
			if seen[g.GroupID] {
				continue
			}
			seen[g.GroupID] = true
			out = append(out, groupRef{ID: g.GroupID, CoordinatorID: g.Coordinator, ProtocolType: g.ProtocolType})
		}
	}
	if ok == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// describeStates 批量读取 group 状态与成员数：按 coordinator 分组，
// 每个 coordinator 一次 DescribeGroups 调用。
func (c *Client) describeStates(ctx context.Context, refs []groupRef, addrs map[int]string) map[string]groupStateInfo {
	byCoord := make(map[int][]string)
	for _, r := range refs {
		byCoord[r.CoordinatorID] = append(byCoord[r.CoordinatorID], r.ID)
	}
	out := make(map[string]groupStateInfo, len(refs))
	for coord, ids := range byCoord {
		addr, ok := addrs[coord]
		if !ok {
			for _, id := range ids {
				out[id] = groupStateInfo{State: "Unknown"}
			}
			continue
		}
		resp, err := c.api.DescribeGroups(ctx, &kgo.DescribeGroupsRequest{
			Addr:     kgo.TCP(addr),
			GroupIDs: ids,
		})
		if err != nil {
			for _, id := range ids {
				out[id] = groupStateInfo{State: "Unknown"}
			}
			continue
		}
		for _, g := range resp.Groups {
			// kafka-go 在解析部分 consumer 协议元数据时可能报错，
			// 但 GroupState 往往是有效的：优先采用 state，错误仅意味着
			// 成员信息可能不完整，不应把状态回退成 Unknown。
			state := g.GroupState
			if state == "" {
				state = "Unknown"
			}
			out[g.GroupID] = groupStateInfo{State: state, Members: len(g.Members)}
		}
	}
	return out
}

// fetchCommits 读取 group 已提交的 offset（Topics=nil 表示全部 topic）。
func (c *Client) fetchCommits(ctx context.Context, groupID string) (map[string][]kgo.OffsetFetchPartition, error) {
	resp, err := c.api.OffsetFetch(ctx, &kgo.OffsetFetchRequest{GroupID: groupID})
	if err != nil {
		return nil, fmt.Errorf("读取提交 offset 失败: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("读取提交 offset 失败: %w", resp.Error)
	}
	return resp.Topics, nil
}

// endOffsetsFor 基于元数据中已知分区，一次性读取这些 topic 的 log end offset。
func (c *Client) endOffsetsFor(ctx context.Context, meta *kgo.MetadataResponse, topics []string) (map[string]map[int]int64, error) {
	out := make(map[string]map[int]int64, len(topics))
	if len(topics) == 0 {
		return out, nil
	}
	want := make(map[string]bool, len(topics))
	for _, t := range topics {
		want[t] = true
	}
	req := make(map[string][]kgo.OffsetRequest, len(topics))
	for _, t := range meta.Topics {
		if !want[t.Name] {
			continue
		}
		for _, p := range t.Partitions {
			req[t.Name] = append(req[t.Name], kgo.LastOffsetOf(p.ID))
		}
	}
	if len(req) == 0 {
		return out, nil
	}
	resp, err := c.api.ListOffsets(ctx, &kgo.ListOffsetsRequest{Topics: req})
	if err != nil {
		return nil, fmt.Errorf("读取 LEO 失败: %w", err)
	}
	for t, parts := range resp.Topics {
		m := make(map[int]int64, len(parts))
		for _, po := range parts {
			if po.Error != nil || po.LastOffset < 0 {
				continue
			}
			m[po.Partition] = po.LastOffset
		}
		out[t] = m
	}
	return out, nil
}

// ListGroups 返回全部 consumer group（含状态与总 lag），按 lag 降序、ID 升序。
// lag 统计失败时展示为 0，不影响列表本身。
func (c *Client) ListGroups(ctx context.Context) ([]GroupSummary, error) {
	meta, err := c.metadata(ctx)
	if err != nil {
		return nil, err
	}
	refs, err := c.listGroups(ctx, meta)
	if err != nil {
		return nil, err
	}
	addrs := brokerAddrMap(meta)
	states := c.describeStates(ctx, refs, addrs)

	// 逐 group 读提交 offset，同时收集涉及的 topic 集合。
	type entry struct {
		summary GroupSummary
		commits map[string][]kgo.OffsetFetchPartition
	}
	entries := make([]entry, 0, len(refs))
	wantTopics := make(map[string]bool)
	for _, r := range refs {
		s := GroupSummary{
			ID:            r.ID,
			State:         "Unknown",
			ProtocolType:  r.ProtocolType,
			CoordinatorID: r.CoordinatorID,
			Coordinator:   addrs[r.CoordinatorID],
		}
		if st, ok := states[r.ID]; ok {
			s.State = st.State
			s.Members = st.Members
		}
		cm, err := c.fetchCommits(ctx, r.ID)
		if err == nil {
			s.Topics = len(cm)
			for t := range cm {
				wantTopics[t] = true
			}
		}
		entries = append(entries, entry{summary: s, commits: cm})
	}

	topics := make([]string, 0, len(wantTopics))
	for t := range wantTopics {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	ends, endErr := c.endOffsetsFor(ctx, meta, topics)

	out := make([]GroupSummary, 0, len(entries))
	for i := range entries {
		if endErr == nil {
			var lag int64
			for t, parts := range entries[i].commits {
				for _, p := range parts {
					if p.Error != nil || p.CommittedOffset < 0 {
						continue
					}
					if end, ok := ends[t][p.Partition]; ok && end > p.CommittedOffset {
						lag += end - p.CommittedOffset
					}
				}
			}
			entries[i].summary.Lag = lag
		}
		out = append(out, entries[i].summary)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Lag != out[j].Lag {
			return out[i].Lag > out[j].Lag
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// GroupDetail 返回 consumer group 详情（状态、成员、逐分区进度）。
func (c *Client) GroupDetail(ctx context.Context, id string) (GroupDetail, error) {
	meta, err := c.metadata(ctx)
	if err != nil {
		return GroupDetail{}, err
	}
	refs, err := c.listGroups(ctx, meta)
	if err != nil {
		return GroupDetail{}, err
	}
	var ref *groupRef
	for i := range refs {
		if refs[i].ID == id {
			ref = &refs[i]
			break
		}
	}
	if ref == nil {
		return GroupDetail{}, fmt.Errorf("%w: consumer group %q", ErrNotFound, id)
	}

	addrs := brokerAddrMap(meta)
	d := GroupDetail{
		ID:            id,
		State:         "Unknown",
		ProtocolType:  ref.ProtocolType,
		CoordinatorID: ref.CoordinatorID,
		Coordinator:   addrs[ref.CoordinatorID],
	}

	// 状态 + 成员详情。
	if addr, ok := addrs[ref.CoordinatorID]; ok {
		resp, err := c.api.DescribeGroups(ctx, &kgo.DescribeGroupsRequest{
			Addr:     kgo.TCP(addr),
			GroupIDs: []string{id},
		})
		if err != nil {
			d.Notes = append(d.Notes, fmt.Sprintf("读取成员信息失败：%v", err))
		} else {
			for _, g := range resp.Groups {
				if g.GroupState != "" {
					d.State = g.GroupState
				}
				if g.Error != nil {
					d.Notes = append(d.Notes,
						fmt.Sprintf("读取成员信息不完整：%v（状态与消费进度不受影响）", g.Error))
				}
				for _, m := range g.Members {
					gm := GroupMember{
						MemberID: m.MemberID,
						ClientID: m.ClientID,
						Host:     m.ClientHost,
					}
					for _, t := range m.MemberAssignments.Topics {
						gm.Assignments = append(gm.Assignments, Assignment{Topic: t.Topic, Partitions: t.Partitions})
					}
					sort.Slice(gm.Assignments, func(i, j int) bool {
						return gm.Assignments[i].Topic < gm.Assignments[j].Topic
					})
					d.Members = append(d.Members, gm)
				}
			}
		}
	} else {
		d.Notes = append(d.Notes, fmt.Sprintf("找不到 coordinator broker %d", ref.CoordinatorID))
	}

	commits, err := c.fetchCommits(ctx, id)
	if err != nil {
		d.Notes = append(d.Notes, err.Error())
		commits = nil
	}

	topics := make([]string, 0, len(commits))
	for t := range commits {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	ends, err := c.endOffsetsFor(ctx, meta, topics)
	if err != nil && len(topics) > 0 {
		d.Notes = append(d.Notes, err.Error())
		ends = nil
	}

	// 行集合 = 已提交过的 topic 的全部分区；未提交的分区也展示（lag 显示 —）。
	partitionCount := make(map[string]int, len(meta.Topics))
	for _, t := range meta.Topics {
		partitionCount[t.Name] = len(t.Partitions)
	}
	for _, t := range topics {
		committed := make(map[int]kgo.OffsetFetchPartition, len(commits[t]))
		for _, p := range commits[t] {
			committed[p.Partition] = p
		}
		maxP := -1
		for p := range committed {
			if p > maxP {
				maxP = p
			}
		}
		if n := partitionCount[t] - 1; n > maxP {
			maxP = n
		}
		for p := 0; p <= maxP; p++ {
			gp := GroupPartition{Topic: t, Partition: p}
			if cp, ok := committed[p]; ok && cp.Error == nil && cp.CommittedOffset >= 0 {
				gp.HasCommit = true
				gp.CommittedOffset = cp.CommittedOffset
				gp.Metadata = cp.Metadata
				if ends != nil {
					if end, ok := ends[t][p]; ok {
						gp.EndOffset = end
						gp.Lag = calcLag(cp.CommittedOffset, end)
						d.TotalLag += gp.Lag
					}
				}
			} else if ends != nil {
				if end, ok := ends[t][p]; ok {
					gp.EndOffset = end
				}
			}
			d.Partitions = append(d.Partitions, gp)
		}
	}
	sort.Slice(d.Partitions, func(i, j int) bool {
		if d.Partitions[i].Topic != d.Partitions[j].Topic {
			return d.Partitions[i].Topic < d.Partitions[j].Topic
		}
		return d.Partitions[i].Partition < d.Partitions[j].Partition
	})
	return d, nil
}

// calcLag 计算 lag：LEO 已推进过提交位置时为差值，否则为 0。
func calcLag(committed, end int64) int64 {
	if committed < 0 || end <= committed {
		return 0
	}
	return end - committed
}
