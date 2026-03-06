package main

import (
	"context"
	"fmt"
	"log"
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
type listenInfo struct {
	ServerID   string
	ServerName string
	ServerType string
	Namespace  string
	PodName    string
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
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.QueryTimeout)
	defer cancel()

	// ── Step 1: 查询监听端点表 ─────────────────────────────────────────────
	listenSamples, err := a.querier.QueryInstant(ctx, "servicemap_listen_endpoint")
	if err != nil {
		log.Printf("E! aggregator: query listen_endpoint failed: %v", err)
		return
	}

	// 构建 listenMap: ip:port → listenInfo（去重取一个）
	listenMap := make(map[listenKey]listenInfo, len(listenSamples))
	for _, s := range listenSamples {
		m := s.Metric
		key := listenKey{IP: m["listen_ip"], Port: m["port"]}
		if _, exists := listenMap[key]; !exists {
			listenMap[key] = listenInfo{
				ServerID:   m["server_id"],
				ServerName: m["server_name"],
				ServerType: m["server_type"],
				Namespace:  m["namespace"],
				PodName:    m["pod_name"],
			}
		}
	}

	// ── Step 2（可选）：K8s Service IP → Pod IP 扩展 ────────────────────────
	// 将 K8s Service IP 解析为 Pod IP，扩充 listenMap 使 edge 能命中
	if a.k8sResolver != nil {
		a.expandK8sServiceIPs(ctx, listenMap)
	}

	// ── Step 3: 查询 edge 指标 ──────────────────────────────────────────────
	edgeSamples, err := a.querier.QueryInstant(ctx, "servicemap_edge_active_connections > 0")
	if err != nil {
		log.Printf("E! aggregator: query edge_active_connections failed: %v", err)
		return
	}

	// ── Step 4: JOIN ─────────────────────────────────────────────────────────
	var edges []P2PEdge
	nodeSet := make(map[string]struct{})

	for _, s := range edgeSamples {
		m := s.Metric
		dstHost := m["destination_host"]
		dstPort := m["destination_port"]

		if dstHost == "" || dstPort == "" {
			continue
		}

		key := listenKey{IP: dstHost, Port: dstPort}
		info, found := listenMap[key]
		if !found {
			// 目标进程在 Prometheus 中没有对应的 listen_endpoint（外部服务或未采集节点）
			// 跳过，或者可在此做"未知目标"处理
			continue
		}

		edge := P2PEdge{
			ClientID:          m["client_id"],
			ClientName:        m["client_name"],
			ClientType:        m["client_type"],
			Namespace:         m["namespace"],
			PodName:           m["pod_name"],
			ServerID:          info.ServerID,
			ServerName:        info.ServerName,
			ServerType:        info.ServerType,
			ServerNamespace:   info.Namespace,
			ServerPodName:     info.PodName,
			ActiveConnections: sampleValue(s),
			DestinationHost:   dstHost,
			DestinationPort:   dstPort,
			UpdatedAt:         time.Now(),
		}
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
	if a.writer != nil {
		if err := a.writer.WriteP2PEdges(ctx, edges); err != nil {
			log.Printf("W! aggregator: remote write failed: %v", err)
		}
	}

	// ── Step 6: 更新快照 ─────────────────────────────────────────────────────
	a.mu.Lock()
	a.snapshot = snap
	a.mu.Unlock()

	log.Printf("I! aggregator: done — %d listen endpoints, %d edges, %d nodes, duration=%s",
		len(listenMap), len(edges), len(nodeSet), time.Since(start))
}

// expandK8sServiceIPs 将 K8s Service ClusterIP → PodIP 映射注入 listenMap。
// 这样当 edge 的 destination_host 是 ClusterIP 时，也能命中对应 Pod 的监听端点。
func (a *Aggregator) expandK8sServiceIPs(ctx context.Context, listenMap map[listenKey]listenInfo) {
	if a.k8sResolver == nil {
		return
	}

	// 收集当前 listenMap 中所有出现的端口
	portSet := make(map[string]struct{})
	for k := range listenMap {
		portSet[k.Port] = struct{}{}
	}

	// 从 K8s API 获取 Service → Endpoints 映射
	svcMap, err := a.k8sResolver.ListServiceEndpoints(ctx)
	if err != nil {
		log.Printf("W! aggregator: list service endpoints failed: %v", err)
		return
	}

	expanded := 0
	for svcIP, endpoints := range svcMap {
		for _, ep := range endpoints {
			podIP := ep.PodIP
			port := fmt.Sprintf("%d", ep.Port)
			// 检查该 PodIP:port 是否在 listenMap 中
			podKey := listenKey{IP: podIP, Port: port}
			info, found := listenMap[podKey]
			if !found {
				continue
			}
			// 将 ServiceIP:port 也映射到同一个 listenInfo
			svcKey := listenKey{IP: svcIP, Port: port}
			if _, exists := listenMap[svcKey]; !exists {
				listenMap[svcKey] = info
				expanded++
			}
		}
	}

	if expanded > 0 {
		log.Printf("I! aggregator: K8s expansion added %d service-IP entries", expanded)
	}
}
