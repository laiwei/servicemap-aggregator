package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// ServiceEndpoint 表示一个 K8s Endpoints 条目（PodIP + Port）
type ServiceEndpoint struct {
	PodIP string
	Port  int32
}

// K8sResolver 解析 K8s Service ClusterIP → Pod IP 映射
type K8sResolver struct {
	client kubernetes.Interface
}

// NewK8sResolver 创建 K8s 解析器（优先 in-cluster，fallback kubeconfig）
func NewK8sResolver(kubeconfigPath string) (*K8sResolver, error) {
	var cfg *rest.Config
	var err error

	// 优先使用 in-cluster 配置（运行在 K8s Pod 中时）
	cfg, err = rest.InClusterConfig()
	if err != nil {
		// 回退到 kubeconfig 文件
		if kubeconfigPath == "" {
			if home := homedir.HomeDir(); home != "" {
				kubeconfigPath = filepath.Join(home, ".kube", "config")
			}
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("build kubeconfig: %w", err)
		}
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create k8s client: %w", err)
	}

	return &K8sResolver{client: client}, nil
}

// ListServiceEndpoints 返回所有命名空间的 Service ClusterIP → []ServiceEndpoint 映射。
// key 是 ClusterIP，value 是该 Service 下所有就绪 Endpoints 的 PodIP+Port 列表。
func (r *K8sResolver) ListServiceEndpoints(ctx context.Context) (map[string][]ServiceEndpoint, error) {
	result := make(map[string][]ServiceEndpoint)

	// 列出所有 Service（取 ClusterIP）
	services, err := r.client.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list services: %w", err)
	}

	// 构建 svcName+ns → ClusterIP 映射
	type svcKey struct{ Name, Namespace string }
	svcIPs := make(map[svcKey]string, len(services.Items))
	for _, svc := range services.Items {
		if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == "None" {
			continue // 跳过 Headless Service
		}
		svcIPs[svcKey{Name: svc.Name, Namespace: svc.Namespace}] = svc.Spec.ClusterIP
	}

	// 列出所有 Endpoints
	endpointsList, err := r.client.CoreV1().Endpoints("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}

	for _, ep := range endpointsList.Items {
		clusterIP, ok := svcIPs[svcKey{Name: ep.Name, Namespace: ep.Namespace}]
		if !ok {
			continue
		}

		var endpoints []ServiceEndpoint
		for _, subset := range ep.Subsets {
			for _, addr := range subset.Addresses {
				if addr.IP == "" {
					continue
				}
				for _, port := range subset.Ports {
					endpoints = append(endpoints, ServiceEndpoint{
						PodIP: addr.IP,
						Port:  port.Port,
					})
				}
			}
		}

		if len(endpoints) > 0 {
			result[clusterIP] = append(result[clusterIP], endpoints...)
		}
	}

	log.Printf("I! k8s-resolver: found %d services with endpoints", len(result))
	return result, nil
}
