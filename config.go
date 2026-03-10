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

	// QueryTimeout 单次 Prometheus 查询超时（每阶段独立计时，互不挤占预算）
	QueryTimeout time.Duration

	// WriteTimeout Remote Write 写入超时（独立于查询超时，避免查询耗时导致写入静默失败）
	WriteTimeout time.Duration

	// EnableK8s 是否启用 K8s Service IP 解析
	EnableK8s bool

	// KubeConfig kubeconfig 文件路径（为空则尝试 in-cluster）
	KubeConfig string
}
