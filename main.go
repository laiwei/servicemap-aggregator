package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Version 可通过 -ldflags 注入。
var Version = "dev"

// servicemap-aggregator
//
// 功能：定期从 Prometheus 查询 servicemap_edge_active_connections 和
// servicemap_listen_endpoint，在内存中 JOIN 出 process/container →
// process/container 的 P2P 拓扑，支持 K8s Service IP 解析，并可：
// 1) HTTP API 暴露拓扑
// 2) Remote Write 回写聚合结果
func main() {
	cfg := &Config{}
	flag.StringVar(&cfg.PrometheusURL, "prometheus-url", "http://localhost:9090", "Prometheus HTTP API 地址")
	flag.StringVar(&cfg.RemoteWriteURL, "remote-write-url", "", "Prometheus Remote Write 地址（为空则不写回）")
	flag.StringVar(&cfg.Listen, "listen", ":9098", "HTTP API 监听地址")
	flag.DurationVar(&cfg.Interval, "interval", 60*time.Second, "聚合周期")
	flag.BoolVar(&cfg.EnableK8s, "enable-k8s", false, "启用 K8s Service IP 解析（需要 kubeconfig 或 in-cluster 权限）")
	flag.StringVar(&cfg.KubeConfig, "kubeconfig", "", "kubeconfig 文件路径（为空则使用 in-cluster 配置）")
	flag.DurationVar(&cfg.QueryTimeout, "query-timeout", 30*time.Second, "Prometheus 查询超时（每次查询独立计时）")
	flag.DurationVar(&cfg.WriteTimeout, "write-timeout", 30*time.Second, "Remote Write 写入超时（独立于查询超时）")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("I! servicemap-aggregator v%s starting, prometheus=%s listen=%s interval=%s", Version, cfg.PrometheusURL, cfg.Listen, cfg.Interval)

	querier := NewPrometheusQuerier(cfg.PrometheusURL, cfg.QueryTimeout)

	var k8sResolver *K8sResolver
	if cfg.EnableK8s {
		var err error
		k8sResolver, err = NewK8sResolver(cfg.KubeConfig)
		if err != nil {
			log.Printf("W! servicemap-aggregator: K8s resolver init failed: %v, continuing without K8s resolution", err)
		} else {
			log.Printf("I! servicemap-aggregator: K8s resolver initialized")
		}
	}

	var writer *RemoteWriter
	if cfg.RemoteWriteURL != "" {
		writer = NewRemoteWriter(cfg.RemoteWriteURL, cfg.QueryTimeout)
		log.Printf("I! servicemap-aggregator: remote write enabled -> %s", cfg.RemoteWriteURL)
	}

	agg := NewAggregator(cfg, querier, k8sResolver, writer)
	apiServer := NewAPIServer(cfg.Listen, agg)

	go func() {
		if err := apiServer.Run(); err != nil {
			log.Printf("E! servicemap-aggregator: API server error: %v", err)
			os.Exit(1)
		}
	}()

	// 启动后立即执行一次，再进入定时循环。
	agg.Aggregate()
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-ticker.C:
			agg.Aggregate()
		case sig := <-quit:
			log.Printf("I! servicemap-aggregator: received signal %v, shutting down", sig)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = apiServer.Shutdown(ctx)
			cancel()
			return
		}
	}
}
