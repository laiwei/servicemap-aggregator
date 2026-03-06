package main

import "time"

// Config 服务配置
type Config struct {
	// PrometheusURL Prometheus HTTP API 根地址
	PrometheusURL string

	// RemoteWriteURL Prometheus Remote Write 地址（为空则不写回）
	RemoteWriteURL string

	// Listen HTTP API 监听地址，如 ":9098"
	Listen string

	// Interval 聚合周期
	Interval time.Duration

	// QueryTimeout Prometheus 查询及 Remote Write 超时
	QueryTimeout time.Duration

	// EnableK8s 是否启用 K8s Service IP 解析
	EnableK8s bool

	// KubeConfig kubeconfig 文件路径（为空则尝试 in-cluster）
	KubeConfig string
}
