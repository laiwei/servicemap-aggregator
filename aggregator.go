package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// P2PEdge 表示一条 process/container → process/container 的拓扑边
type P2PEdge struct {
	// 发起方（client）信息
	ClientID   string `json:"client_id"`
	ClientName string `json:"client_name"`
	ClientType string `json:"client_type"`
	Namespace  string `json:"namespace,omitempty"`
	PodName    string `json:"pod_name,omitempty"`

	// 目标方（server）信息
	ServerID        string `json:"server_id"`
	ServerName      string `json:"server_name"`
	ServerType      string `json:"server_type"`
	ServerNamespace string `json:"server_namespace,omitempty"`
	ServerPodName   string `json:"server_pod_name,omitempty"`

	// 连接数（来自 edge 指标）
	ActiveConnections float64 `json:"active_connections"`

	// 中间链路信息（可选，便于调试）
	DestinationHost string `json:"destination_host,omitempty"`
	DestinationPort string `json:"destination_port,omitempty"`

	// 时间戳
	UpdatedAt time.Time `json:"updated_at"`
}

// TopologySnapshot 当前拓扑快照
type TopologySnapshot struct {
	Edges     []P2PEdge `json:"edges"`
	NodeCount int       `json:"node_count"`
	EdgeCount int       `json:"edge_count"`
	UpdatedAt time.Time `json:"updated_at"`
}

// listenKey JOIN 查找键：目标 IP + 目标端口
type listenKey struct {
	IP   string
	Port string
}

// listenInfo 监听端点对应的进程/容器信息
type listenCandidate struct {
	ServerID      string
	ServerName    string
	ServerType    string
	Namespace     string
	PodName       string
	AgentHostname string
	ListenIP      string
	Port          string
	Cluster       string
	Env           string
}

type edgeObservation struct {
	ClientID          string
	ClientName        string
	ClientType        string
	Namespace         string
	PodName           string
	AgentHostname     string
	DestinationHost   string
	DestinationPort   string
	Cluster           string
	Env               string
	ActiveConnections float64
}

type serverIdentity struct {
	ServerID   string
	ServerName string
	ServerType string
	Namespace  string
	PodName    string
}

type joinStatus string

const (
	joinStatusMatched   joinStatus = "matched"
	joinStatusNotFound  joinStatus = "not_found"
	joinStatusAmbiguous joinStatus = "ambiguous"
	joinStatusRejected  joinStatus = "rejected"
)

type joinDecision struct {
	Status         joinStatus
	Path           string
	Server         serverIdentity
	CandidateCount int
	Reason         string
}

type joinStats struct {
	Matched              int
	NotFound             int
	Ambiguous            int
	Rejected             int
	ClusterIPMatched     int
	ClusterIPNotFound    int
	ClusterIPAmbiguous   int
	ClusterIPNoBackends  int
	DirectIPMatched      int
	DirectIPNotFound     int
	DirectIPAmbiguous    int
	IgnoredLoopbackEdges int
}

// Aggregator 核心 JOIN 逻辑
type Aggregator struct {
	cfg         *Config
	querier     *PrometheusQuerier
	k8sResolver *K8sResolver
	writer      *RemoteWriter

	mu       sync.RWMutex
	snapshot *TopologySnapshot
}

func NewAggregator(cfg *Config, querier *PrometheusQuerier, k8sResolver *K8sResolver, writer *RemoteWriter) *Aggregator {
	return &Aggregator{
		cfg:         cfg,
		querier:     querier,
		k8sResolver: k8sResolver,
		writer:      writer,
		snapshot:    &TopologySnapshot{UpdatedAt: time.Now()},
	}
}

// GetSnapshot 返回当前拓扑快照（线程安全）
func (a *Aggregator) GetSnapshot() *TopologySnapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.snapshot
}

// Aggregate 执行一次完整的聚合：查询 → JOIN → （可选）写回 → 更新快照
func (a *Aggregator) Aggregate() {
	start := time.Now()
	// 每个 I/O 阶段使用独立 context，互不挤占超时预算（M1）。

	// ── Step 1: 查询监听端点表 ─────────────────────────────────────────────
	q1Ctx, q1Cancel := context.WithTimeout(context.Background(), a.cfg.QueryTimeout)
	listenSamples, err := a.querier.QueryInstant(q1Ctx, "servicemap_listen_endpoint")
	q1Cancel()
	if err != nil {
		log.Printf("E! aggregator: query listen_endpoint failed: %v", err)
		return
	}

	listenByAddr, listenCandidateCount := buildListenIndex(listenSamples)

	// ── Step 2（可选）：构建 K8s Service ClusterIP → Pod 后端索引 ─────────────
	serviceByAddr := make(map[listenKey][]ServiceBackendRef)
	if a.k8sResolver != nil {
		k8sCtx, k8sCancel := context.WithTimeout(context.Background(), a.cfg.QueryTimeout)
		var k8sErr error
		serviceByAddr, k8sErr = a.buildK8sServiceIndex(k8sCtx)
		k8sCancel()
		if k8sErr != nil {
			log.Printf("W! aggregator: build k8s service index failed: %v", k8sErr)
		}
	}

	// ── Step 3: 查询 edge 指标 ──────────────────────────────────────────────
	q2Ctx, q2Cancel := context.WithTimeout(context.Background(), a.cfg.QueryTimeout)
	edgeSamples, err := a.querier.QueryInstant(q2Ctx, "servicemap_edge_active_connections > 0")
	q2Cancel()
	if err != nil {
		log.Printf("E! aggregator: query edge_active_connections failed: %v", err)
		return
	}

	// ── Step 4: JOIN ─────────────────────────────────────────────────────────
	var edges []P2PEdge
	nodeSet := make(map[string]struct{})
	stats := joinStats{}

	for _, s := range edgeSamples {
		edgeObs, ok := buildEdgeObservation(s)
		if !ok {
			stats.Rejected++
			continue
		}

		var decision joinDecision
		if a.k8sResolver != nil {
			decision = joinEdgeK8s(edgeObs, listenByAddr, serviceByAddr)
		} else {
			decision = joinEdgeHost(edgeObs, listenByAddr)
		}

		switch decision.Status {
		case joinStatusMatched:
			stats.Matched++
			if decision.Path == "cluster_ip" {
				stats.ClusterIPMatched++
			} else {
				stats.DirectIPMatched++
			}
		case joinStatusNotFound:
			stats.NotFound++
			if decision.Path == "cluster_ip" {
				if decision.Reason == "no_pod_listen_endpoint" {
					// K8s 有就绪 Pod，但 Prometheus 未采集到其 listen_endpoint
					stats.ClusterIPNoBackends++
				} else {
					// listen_endpoint 存在但被 cluster/env 标签过滤掉
					stats.ClusterIPNotFound++
				}
			} else {
				stats.DirectIPNotFound++
			}
			continue
		case joinStatusAmbiguous:
			stats.Ambiguous++
			if decision.Path == "cluster_ip" {
				stats.ClusterIPAmbiguous++
			} else {
				stats.DirectIPAmbiguous++
			}
			log.Printf("W! aggregator: ambiguous join path=%s client=%s dst=%s:%s candidates=%d reason=%s",
				decision.Path, edgeObs.ClientName, edgeObs.DestinationHost, edgeObs.DestinationPort, decision.CandidateCount, decision.Reason)
			continue
		case joinStatusRejected:
			stats.Rejected++
			if decision.Reason == "ignored loopback destination" {
				stats.IgnoredLoopbackEdges++
			}
			continue
		default:
			stats.Rejected++
			continue
		}

		edge := buildP2PEdge(edgeObs, decision.Server)
		edges = append(edges, edge)

		nodeSet[edge.ClientID] = struct{}{}
		nodeSet[edge.ServerID] = struct{}{}
	}

	snap := &TopologySnapshot{
		Edges:     edges,
		NodeCount: len(nodeSet),
		EdgeCount: len(edges),
		UpdatedAt: time.Now(),
	}

	// ── Step 5（可选）：Remote Write 写回 Prometheus ─────────────────────────
	// 使用独立 context，与查询阶段的超时互不干扰（M5）。
	if a.writer != nil {
		writeCtx, writeCancel := context.WithTimeout(context.Background(), a.cfg.WriteTimeout)
		if err := a.writer.WriteP2PEdges(writeCtx, edges); err != nil {
			log.Printf("W! aggregator: remote write failed: %v", err)
		}
		writeCancel()
	}

	// ── Step 6: 更新快照 ─────────────────────────────────────────────────────
	a.mu.Lock()
	a.snapshot = snap
	a.mu.Unlock()

	log.Printf("I! aggregator: done — listen_keys=%d listen_candidates=%d service_keys=%d edges=%d nodes=%d matched=%d not_found=%d ambiguous=%d rejected=%d clusterip_matched=%d clusterip_not_found=%d clusterip_no_pod_listen=%d clusterip_ambiguous=%d directip_matched=%d directip_not_found=%d directip_ambiguous=%d ignored_loopback=%d duration=%s",
		len(listenByAddr), listenCandidateCount, len(serviceByAddr), len(edges), len(nodeSet),
		stats.Matched, stats.NotFound, stats.Ambiguous, stats.Rejected,
		stats.ClusterIPMatched, stats.ClusterIPNotFound, stats.ClusterIPNoBackends, stats.ClusterIPAmbiguous,
		stats.DirectIPMatched, stats.DirectIPNotFound, stats.DirectIPAmbiguous,
		stats.IgnoredLoopbackEdges, time.Since(start))
}

func buildListenIndex(samples []promSample) (map[listenKey][]listenCandidate, int) {
	index := make(map[listenKey][]listenCandidate, len(samples))
	seen := make(map[listenKey]map[string]struct{}, len(samples))
	total := 0

	for _, s := range samples {
		m := s.Metric
		ip := m["listen_ip"]
		port := m["port"]
		if ip == "" || port == "" {
			continue
		}

		key := listenKey{IP: ip, Port: port}
		candidate := listenCandidate{
			ServerID:      m["server_id"],
			ServerName:    m["server_name"],
			ServerType:    m["server_type"],
			Namespace:     m["namespace"],
			PodName:       m["pod_name"],
			AgentHostname: m["agent_hostname"],
			ListenIP:      ip,
			Port:          port,
			Cluster:       m["cluster"],
			Env:           m["env"],
		}

		sig := candidate.signature()
		if _, ok := seen[key]; !ok {
			seen[key] = make(map[string]struct{})
		}
		if _, ok := seen[key][sig]; ok {
			continue
		}
		seen[key][sig] = struct{}{}
		index[key] = append(index[key], candidate)
		total++
	}

	return index, total
}

func (a *Aggregator) buildK8sServiceIndex(ctx context.Context) (map[listenKey][]ServiceBackendRef, error) {
	if a.k8sResolver == nil {
		return nil, nil
	}

	backends, err := a.k8sResolver.ListServiceBackends(ctx)
	if err != nil {
		return nil, err
	}

	index := make(map[listenKey][]ServiceBackendRef)
	seen := make(map[listenKey]map[string]struct{})
	for _, backend := range backends {
		key := listenKey{IP: backend.ClusterIP, Port: fmt.Sprintf("%d", backend.ServicePort)}
		sig := backend.signature()
		if _, ok := seen[key]; !ok {
			seen[key] = make(map[string]struct{})
		}
		if _, ok := seen[key][sig]; ok {
			continue
		}
		seen[key][sig] = struct{}{}
		index[key] = append(index[key], backend)
	}

	return index, nil
}

func buildEdgeObservation(s promSample) (edgeObservation, bool) {
	m := s.Metric
	dstHost := m["destination_host"]
	dstPort := m["destination_port"]
	if dstHost == "" || dstPort == "" {
		return edgeObservation{}, false
	}

	return edgeObservation{
		ClientID:          m["client_id"],
		ClientName:        m["client_name"],
		ClientType:        m["client_type"],
		Namespace:         m["namespace"],
		PodName:           m["pod_name"],
		AgentHostname:     m["agent_hostname"],
		DestinationHost:   dstHost,
		DestinationPort:   dstPort,
		Cluster:           m["cluster"],
		Env:               m["env"],
		ActiveConnections: sampleValue(s),
	}, true
}

func joinEdgeHost(edge edgeObservation, listenByAddr map[listenKey][]listenCandidate) joinDecision {
	if shouldIgnoreDestination(edge.DestinationHost) {
		return joinDecision{Status: joinStatusRejected, Path: "direct_ip", Reason: "ignored loopback destination"}
	}
	return joinByDirectIP(edge, listenByAddr, "host_ip")
}

func joinEdgeK8s(edge edgeObservation, listenByAddr map[listenKey][]listenCandidate, serviceByAddr map[listenKey][]ServiceBackendRef) joinDecision {
	if shouldIgnoreDestination(edge.DestinationHost) {
		return joinDecision{Status: joinStatusRejected, Path: "direct_ip", Reason: "ignored loopback destination"}
	}

	key := listenKey{IP: edge.DestinationHost, Port: edge.DestinationPort}
	if backends := serviceByAddr[key]; len(backends) > 0 {
		return joinByClusterIP(edge, listenByAddr, backends)
	}

	return joinByDirectIP(edge, listenByAddr, "pod_or_host_ip")
}

func joinByDirectIP(edge edgeObservation, listenByAddr map[listenKey][]listenCandidate, path string) joinDecision {
	key := listenKey{IP: edge.DestinationHost, Port: edge.DestinationPort}
	rawCandidates := listenByAddr[key]
	filtered := filterCandidatesBySoftChecks(edge, rawCandidates)
	identities := mergeByInstanceIdentity(filtered)

	switch len(identities) {
	case 0:
		// M3：区分两种根因截然不同的失败场景，便于排障
		// no_listen_endpoint    → 该 IP:Port 根本没有进程监听（categraf 未采集到）
		// label_mismatch_filtered → 有监听端点，但 cluster/env 标签不匹配被过滤
		if len(rawCandidates) == 0 {
			return joinDecision{Status: joinStatusNotFound, Path: path, Reason: "no_listen_endpoint", CandidateCount: 0}
		}
		return joinDecision{Status: joinStatusNotFound, Path: path, Reason: "label_mismatch_filtered", CandidateCount: len(rawCandidates)}
	case 1:
		return joinDecision{Status: joinStatusMatched, Path: path, Server: identities[0], CandidateCount: len(filtered)}
	default:
		// M2：tiebreaker —— 优先选与 edge 上报方同主机的候选。
		// 触发场景：多台主机上恰好有相同 IP 段的进程监听同一端口（跨主机 IP 碰撞）。
		if edge.AgentHostname != "" {
			hostFiltered := filterByAgentHostname(edge.AgentHostname, filtered)
			hostIdentities := mergeByInstanceIdentity(hostFiltered)
			if len(hostIdentities) == 1 {
				return joinDecision{Status: joinStatusMatched, Path: path, Server: hostIdentities[0], CandidateCount: len(filtered), Reason: "tiebreak_by_hostname"}
			}
		}
		return joinDecision{Status: joinStatusAmbiguous, Path: path, Reason: "multiple instance identities", CandidateCount: len(identities)}
	}
}

func joinByClusterIP(edge edgeObservation, listenByAddr map[listenKey][]listenCandidate, backends []ServiceBackendRef) joinDecision {
	// 注：调用方 joinEdgeK8s 已确保 backends 非空，此处无需再守卫。
	allCandidates := make([]listenCandidate, 0)
	seen := make(map[string]struct{})
	for _, backend := range backends {
		if backend.PodIP == "" || backend.PodPort <= 0 {
			continue
		}
		podKey := listenKey{IP: backend.PodIP, Port: fmt.Sprintf("%d", backend.PodPort)}
		for _, candidate := range listenByAddr[podKey] {
			sig := candidate.signature()
			if _, ok := seen[sig]; ok {
				continue
			}
			seen[sig] = struct{}{}
			allCandidates = append(allCandidates, candidate)
		}
	}

	// 两种不同的 not-found 原因，排障含义截然不同：
	// no_pod_listen_endpoint  → K8s Endpoints 有就绪 Pod，但 Prometheus 未采集到其 listen_endpoint
	//                           根因：Pod 未部署 categraf / scrape 配置缺失
	// label_mismatch_filtered → listen_endpoint 存在，但 cluster/env 标签不匹配
	//                           根因：标签配置不一致
	if len(allCandidates) == 0 {
		return joinDecision{Status: joinStatusNotFound, Path: "cluster_ip", Reason: "no_pod_listen_endpoint", CandidateCount: 0}
	}

	filtered := filterCandidatesBySoftChecks(edge, allCandidates)
	identities := mergeByLogicalServiceIdentity(filtered, backends)

	switch len(identities) {
	case 0:
		return joinDecision{Status: joinStatusNotFound, Path: "cluster_ip", Reason: "label_mismatch_filtered", CandidateCount: len(allCandidates)}
	case 1:
		return joinDecision{Status: joinStatusMatched, Path: "cluster_ip", Server: identities[0], CandidateCount: len(filtered)}
	default:
		return joinDecision{Status: joinStatusAmbiguous, Path: "cluster_ip", Reason: "multiple logical service identities", CandidateCount: len(identities)}
	}
}

func filterCandidatesBySoftChecks(edge edgeObservation, candidates []listenCandidate) []listenCandidate {
	if len(candidates) == 0 {
		return nil
	}

	filtered := make([]listenCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if edge.Cluster != "" && candidate.Cluster != "" && edge.Cluster != candidate.Cluster {
			continue
		}
		if edge.Env != "" && candidate.Env != "" && edge.Env != candidate.Env {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}

// filterByAgentHostname 从候选列表中保留 agent_hostname 匹配的项，用于多候选 tiebreaker（M2）。
func filterByAgentHostname(hostname string, candidates []listenCandidate) []listenCandidate {
	result := make([]listenCandidate, 0, len(candidates))
	for _, c := range candidates {
		if c.AgentHostname == hostname {
			result = append(result, c)
		}
	}
	return result
}

func mergeByInstanceIdentity(candidates []listenCandidate) []serverIdentity {
	groups := make(map[string]serverIdentity)
	for _, candidate := range candidates {
		id := serverIdentity{
			ServerID:   candidate.ServerID,
			ServerName: candidate.ServerName,
			ServerType: candidate.ServerType,
			Namespace:  candidate.Namespace,
			PodName:    candidate.PodName,
		}
		groups[id.signature()] = id
	}

	identities := make([]serverIdentity, 0, len(groups))
	for _, id := range groups {
		identities = append(identities, id)
	}
	return identities
}

// mergeByLogicalServiceIdentity 将多个后端 Pod 候选合并为 K8s Service 级别的唯一身份。
//
// 设计原则：只要流量经过 ClusterIP，访问的语义单元就是 Service，而不是某个具体 Pod。
// 因此无论当前 Ready 副本数是 1 还是 N，ServerID 始终使用 syntheticServiceID，
// PodName 始终清空。这样可以保证节点身份在扩/缩容、Pod 重建时保持稳定。
func mergeByLogicalServiceIdentity(candidates []listenCandidate, backends []ServiceBackendRef) []serverIdentity {
	if len(candidates) == 0 {
		return nil
	}

	fallbackNS := ""
	fallbackName := ""
	if len(backends) > 0 {
		fallbackNS = backends[0].ServiceNamespace
		fallbackName = backends[0].ServiceName
	}

	// key = namespace \x00 service_name \x00 server_type
	// 同一 key 对应的所有 Pod 候选合并为一个 Service 级别身份
	groups := make(map[string]serverIdentity)
	for _, candidate := range candidates {
		ns := firstNonEmpty(candidate.Namespace, fallbackNS)
		name := firstNonEmpty(candidate.ServerName, fallbackName)
		typ := firstNonEmpty(candidate.ServerType, "k8s_service")
		key := ns + "\x00" + name + "\x00" + typ

		if _, ok := groups[key]; ok {
			// 同一 Service 的其余 Pod 候选已经建立过 identity，跳过
			continue
		}
		groups[key] = serverIdentity{
			// 始终使用合成 ID，不依赖具体 Pod ID，保证身份稳定
			ServerID:   syntheticServiceID(ns, name, typ),
			ServerName: name,
			ServerType: typ,
			Namespace:  ns,
			PodName:    "", // ClusterIP 路径不暴露具体 Pod，避免副本变化时身份跳变
		}
	}

	identities := make([]serverIdentity, 0, len(groups))
	for _, id := range groups {
		identities = append(identities, id)
	}
	return identities
}

func buildP2PEdge(edge edgeObservation, server serverIdentity) P2PEdge {
	return P2PEdge{
		ClientID:          edge.ClientID,
		ClientName:        edge.ClientName,
		ClientType:        edge.ClientType,
		Namespace:         edge.Namespace,
		PodName:           edge.PodName,
		ServerID:          server.ServerID,
		ServerName:        server.ServerName,
		ServerType:        server.ServerType,
		ServerNamespace:   server.Namespace,
		ServerPodName:     server.PodName,
		ActiveConnections: edge.ActiveConnections,
		DestinationHost:   edge.DestinationHost,
		DestinationPort:   edge.DestinationPort,
		UpdatedAt:         time.Now(),
	}
}

func shouldIgnoreDestination(host string) bool {
	if host == "" {
		return true
	}
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

func syntheticServiceID(namespace, name, serverType string) string {
	if namespace == "" {
		return fmt.Sprintf("svc:%s:%s", name, serverType)
	}
	return fmt.Sprintf("svc:%s/%s:%s", namespace, name, serverType)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func (c listenCandidate) signature() string {
	return c.ServerID + "\x00" + c.ServerName + "\x00" + c.ServerType + "\x00" + c.Namespace + "\x00" + c.PodName + "\x00" + c.AgentHostname + "\x00" + c.ListenIP + "\x00" + c.Port + "\x00" + c.Cluster + "\x00" + c.Env
}

func (s serverIdentity) signature() string {
	return s.ServerID + "\x00" + s.ServerName + "\x00" + s.ServerType + "\x00" + s.Namespace + "\x00" + s.PodName
}
