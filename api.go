package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
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
	mux.HandleFunc("/api/v1/topology/text", s.handleTopologyText)
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
	filtered, _, _, _ := s.filterTopologySnapshot(r)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(filtered); err != nil {
		log.Printf("W! api: encode response: %v", err)
	}
}

// handleTopologyText 返回适合人工阅读和 LLM 理解的拓扑文本快照
//
// GET /api/v1/topology/text
// 可选参数同 /api/v1/topology：
//
//	?client_name=nginx    — 过滤源服务名
//	?server_name=mysql    — 过滤目标服务名
//	?namespace=production — 过滤命名空间
func (s *APIServer) handleTopologyText(w http.ResponseWriter, r *http.Request) {
	filtered, srcFilter, dstFilter, nsFilter := s.filterTopologySnapshot(r)
	text := renderTopologyText(filtered, srcFilter, dstFilter, nsFilter)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if _, err := w.Write([]byte(text)); err != nil {
		log.Printf("W! api: write text response: %v", err)
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

func (s *APIServer) filterTopologySnapshot(r *http.Request) (*TopologySnapshot, string, string, string) {
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

	return filtered, srcFilter, dstFilter, nsFilter
}

type topologyTextNode struct {
	ID        string
	Name      string
	Type      string
	Namespace string
	PodName   string
}

func renderTopologyText(snap *TopologySnapshot, srcFilter, dstFilter, nsFilter string) string {
	nodes := collectTopologyTextNodes(snap)
	edges := append([]P2PEdge(nil), snap.Edges...)

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Name != nodes[j].Name {
			return nodes[i].Name < nodes[j].Name
		}
		return nodes[i].ID < nodes[j].ID
	})

	sort.Slice(edges, func(i, j int) bool {
		if edges[i].ClientName != edges[j].ClientName {
			return edges[i].ClientName < edges[j].ClientName
		}
		if edges[i].ServerName != edges[j].ServerName {
			return edges[i].ServerName < edges[j].ServerName
		}
		if edges[i].DestinationHost != edges[j].DestinationHost {
			return edges[i].DestinationHost < edges[j].DestinationHost
		}
		if edges[i].DestinationPort != edges[j].DestinationPort {
			return edges[i].DestinationPort < edges[j].DestinationPort
		}
		if edges[i].ClientID != edges[j].ClientID {
			return edges[i].ClientID < edges[j].ClientID
		}
		return edges[i].ServerID < edges[j].ServerID
	})

	var b strings.Builder
	b.WriteString("TOPOLOGY_SNAPSHOT\n")
	fmt.Fprintf(&b, "updated_at: %s\n", snap.UpdatedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "node_count: %d\n", snap.NodeCount)
	fmt.Fprintf(&b, "edge_count: %d\n", snap.EdgeCount)
	b.WriteString("filters:\n")
	fmt.Fprintf(&b, "  client_name: %s\n", quotedOrDash(srcFilter))
	fmt.Fprintf(&b, "  server_name: %s\n", quotedOrDash(dstFilter))
	fmt.Fprintf(&b, "  namespace: %s\n", quotedOrDash(nsFilter))

	b.WriteString("\nNODES\n")
	if len(nodes) == 0 {
		b.WriteString("- none\n")
	} else {
		for _, n := range nodes {
			fmt.Fprintf(&b, "- id=%s name=%s type=%s namespace=%s pod_name=%s\n",
				quotedOrDash(n.ID),
				quotedOrDash(n.Name),
				quotedOrDash(n.Type),
				quotedOrDash(n.Namespace),
				quotedOrDash(n.PodName),
			)
		}
	}

	b.WriteString("\nEDGES\n")
	if len(edges) == 0 {
		b.WriteString("- none\n")
	} else {
		for i, e := range edges {
			fmt.Fprintf(&b, "- index=%d client_id=%s client_name=%s client_type=%s client_namespace=%s client_pod_name=%s server_id=%s server_name=%s server_type=%s server_namespace=%s server_pod_name=%s active_connections=%s destination_host=%s destination_port=%s updated_at=%s\n",
				i+1,
				quotedOrDash(e.ClientID),
				quotedOrDash(e.ClientName),
				quotedOrDash(e.ClientType),
				quotedOrDash(e.Namespace),
				quotedOrDash(e.PodName),
				quotedOrDash(e.ServerID),
				quotedOrDash(e.ServerName),
				quotedOrDash(e.ServerType),
				quotedOrDash(e.ServerNamespace),
				quotedOrDash(e.ServerPodName),
				strconv.FormatFloat(e.ActiveConnections, 'f', -1, 64),
				quotedOrDash(e.DestinationHost),
				quotedOrDash(e.DestinationPort),
				quotedOrDash(e.UpdatedAt.UTC().Format(time.RFC3339)),
			)
		}
	}

	return b.String()
}

func collectTopologyTextNodes(snap *TopologySnapshot) []topologyTextNode {
	nodeMap := make(map[string]topologyTextNode)
	upsert := func(id, name, nodeType, namespace, podName string) {
		if id == "" {
			return
		}
		n := nodeMap[id]
		if n.ID == "" {
			n = topologyTextNode{
				ID:        id,
				Name:      name,
				Type:      nodeType,
				Namespace: namespace,
				PodName:   podName,
			}
		} else {
			if n.Name == "" {
				n.Name = name
			}
			if n.Type == "" {
				n.Type = nodeType
			}
			if n.Namespace == "" {
				n.Namespace = namespace
			}
			if n.PodName == "" {
				n.PodName = podName
			}
		}
		nodeMap[id] = n
	}

	for _, e := range snap.Edges {
		upsert(e.ClientID, e.ClientName, e.ClientType, e.Namespace, e.PodName)
		upsert(e.ServerID, e.ServerName, e.ServerType, e.ServerNamespace, e.ServerPodName)
	}

	nodes := make([]topologyTextNode, 0, len(nodeMap))
	for _, n := range nodeMap {
		nodes = append(nodes, n)
	}
	return nodes
}

func quotedOrDash(v string) string {
	if v == "" {
		return strconv.Quote("-")
	}
	return strconv.Quote(v)
}
