package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// APIServer 暴露 HTTP API
type APIServer struct {
	listen string
	agg    *Aggregator
	server *http.Server
}

func NewAPIServer(listen string, agg *Aggregator) *APIServer {
	s := &APIServer{
		listen: listen,
		agg:    agg,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/topology", s.handleTopology)
	mux.HandleFunc("/api/v1/edges", s.handleEdges)
	mux.HandleFunc("/healthz", s.handleHealth)
	s.server = &http.Server{
		Addr:         listen,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return s
}

func (s *APIServer) Run() error {
	log.Printf("I! servicemap-aggregator: API server listening on %s", s.listen)
	return s.server.ListenAndServe()
}

func (s *APIServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// handleTopology 返回完整拓扑快照（JSON）
//
// GET /api/v1/topology
// 可选参数：
//
//	?client_name=nginx    — 过滤源服务名
//	?server_name=mysql    — 过滤目标服务名
//	?namespace=production — 过滤命名空间
func (s *APIServer) handleTopology(w http.ResponseWriter, r *http.Request) {
	snap := s.agg.GetSnapshot()

	q := r.URL.Query()
	srcFilter := q.Get("client_name")
	dstFilter := q.Get("server_name")
	nsFilter := q.Get("namespace")

	filtered := snap
	if srcFilter != "" || dstFilter != "" || nsFilter != "" {
		var edges []P2PEdge
		for _, e := range snap.Edges {
			if srcFilter != "" && e.ClientName != srcFilter {
				continue
			}
			if dstFilter != "" && e.ServerName != dstFilter {
				continue
			}
			if nsFilter != "" && e.ServerNamespace != nsFilter {
				continue
			}
			edges = append(edges, e)
		}
		nodeSet := make(map[string]struct{})
		for _, e := range edges {
			nodeSet[e.ClientID] = struct{}{}
			nodeSet[e.ServerID] = struct{}{}
		}
		filtered = &TopologySnapshot{
			Edges:     edges,
			NodeCount: len(nodeSet),
			EdgeCount: len(edges),
			UpdatedAt: snap.UpdatedAt,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(filtered); err != nil {
		log.Printf("W! api: encode response: %v", err)
	}
}

// handleEdges 返回 Grafana Node Graph 所需的 nodes + edges 格式
//
// GET /api/v1/edges
// Grafana Node Graph Panel 要求：
//
//	nodes frame: fields [id, title, mainStat, secondaryStat, arc__*]
//	edges frame: fields [id, source, target, mainStat]
func (s *APIServer) handleEdges(w http.ResponseWriter, r *http.Request) {
	snap := s.agg.GetSnapshot()

	type nodeItem struct {
		ID            string  `json:"id"`
		Title         string  `json:"title"`
		MainStat      float64 `json:"mainStat"`      // 出/入连接数（聚合）
		SecondaryStat string  `json:"secondaryStat"` // 类型：bare_process / container
	}
	type edgeItem struct {
		ID       string  `json:"id"`
		Source   string  `json:"source"`
		Target   string  `json:"target"`
		MainStat float64 `json:"mainStat"` // 连接数
	}

	nodeMap := make(map[string]*nodeItem)
	var edges []edgeItem

	for i, e := range snap.Edges {
		// 节点：源
		if _, exists := nodeMap[e.ClientID]; !exists {
			nodeMap[e.ClientID] = &nodeItem{
				ID:            e.ClientID,
				Title:         e.ClientName,
				SecondaryStat: e.ClientType,
			}
		}
		nodeMap[e.ClientID].MainStat += e.ActiveConnections

		// 节点：目标
		if _, exists := nodeMap[e.ServerID]; !exists {
			nodeMap[e.ServerID] = &nodeItem{
				ID:            e.ServerID,
				Title:         e.ServerName,
				SecondaryStat: e.ServerType,
			}
		}

		// 边
		edges = append(edges, edgeItem{
			ID:       generateEdgeID(i, e.ClientID, e.ServerID),
			Source:   e.ClientID,
			Target:   e.ServerID,
			MainStat: e.ActiveConnections,
		})
	}

	var nodes []nodeItem
	for _, n := range nodeMap {
		nodes = append(nodes, *n)
	}

	resp := map[string]interface{}{
		"nodes": nodes,
		"edges": edges,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("W! api: encode response: %v", err)
	}
}

// handleHealth 健康检查
func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	snap := s.agg.GetSnapshot()
	age := time.Since(snap.UpdatedAt)

	// 超过 3 个聚合周期未更新，返回 503
	if age > 3*s.agg.cfg.Interval {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"unhealthy","reason":"stale data"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func generateEdgeID(i int, src, dst string) string {
	// 简单用索引，保证同一次快照内唯一
	return src + "->" + dst
}
