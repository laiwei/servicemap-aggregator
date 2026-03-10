package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// ServiceBackendRef 表示一个 K8s Service 入口对应的单个后端 Pod 端点。
// 含义是：ClusterIP:ServicePort -> PodIP:PodPort。
type ServiceBackendRef struct {
	ServiceNamespace string
	ServiceName      string
	ClusterIP        string
	ServicePort      int32
	PodIP            string
	PodPort          int32
}

type servicePortMeta struct {
	Name       string
	Port       int32
	TargetPort intstr.IntOrString
}

type serviceMeta struct {
	ClusterIP string
	Ports     []servicePortMeta
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

// ListServiceBackends 返回所有命名空间的 Service 后端列表。
// 每一项表示一条 ClusterIP:ServicePort -> PodIP:PodPort 的映射关系。
func (r *K8sResolver) ListServiceBackends(ctx context.Context) ([]ServiceBackendRef, error) {
	result := make([]ServiceBackendRef, 0)

	// 列出所有 Service（取 ClusterIP）
	services, err := r.client.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list services: %w", err)
	}

	// 构建 svcName+ns -> Service 元信息映射
	type svcKey struct{ Name, Namespace string }
	svcMeta := make(map[svcKey]serviceMeta, len(services.Items))
	for _, svc := range services.Items {
		if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == "None" {
			continue // 跳过 Headless Service
		}

		ports := make([]servicePortMeta, 0, len(svc.Spec.Ports))
		for _, port := range svc.Spec.Ports {
			ports = append(ports, servicePortMeta{
				Name:       port.Name,
				Port:       port.Port,
				TargetPort: port.TargetPort,
			})
		}

		svcMeta[svcKey{Name: svc.Name, Namespace: svc.Namespace}] = serviceMeta{
			ClusterIP: svc.Spec.ClusterIP,
			Ports:     ports,
		}
	}

	// 列出所有 Endpoints
	endpointsList, err := r.client.CoreV1().Endpoints("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}

	seen := make(map[string]struct{})
	for _, ep := range endpointsList.Items {
		meta, ok := svcMeta[svcKey{Name: ep.Name, Namespace: ep.Namespace}]
		if !ok {
			continue
		}

		for _, subset := range ep.Subsets {
			if len(subset.Addresses) == 0 || len(subset.Ports) == 0 {
				continue
			}

			endpointPortsByName := make(map[string]int32, len(subset.Ports))
			for _, p := range subset.Ports {
				if p.Name != "" {
					endpointPortsByName[p.Name] = p.Port
				}
			}

			for _, svcPort := range meta.Ports {
				backendPorts := resolveBackendPorts(svcPort, subset, endpointPortsByName)
				if len(backendPorts) == 0 {
					continue
				}

				for _, addr := range subset.Addresses {
					if addr.IP == "" {
						continue
					}
					for _, podPort := range backendPorts {
						backend := ServiceBackendRef{
							ServiceNamespace: ep.Namespace,
							ServiceName:      ep.Name,
							ClusterIP:        meta.ClusterIP,
							ServicePort:      svcPort.Port,
							PodIP:            addr.IP,
							PodPort:          podPort,
						}
						sig := backend.signature()
						if _, exists := seen[sig]; exists {
							continue
						}
						seen[sig] = struct{}{}
						result = append(result, backend)
					}
				}
			}
		}
	}

	log.Printf("I! k8s-resolver: found %d service backends", len(result))
	return result, nil
}

func resolveBackendPorts(svcPort servicePortMeta, subset corev1.EndpointSubset, portsByName map[string]int32) []int32 {
	unique := make(map[int32]struct{})
	add := func(port int32) {
		if port > 0 {
			unique[port] = struct{}{}
		}
	}

	switch svcPort.TargetPort.Type {
	case intstr.Int:
		if svcPort.TargetPort.IntVal > 0 {
			add(svcPort.TargetPort.IntVal)
		}
	case intstr.String:
		if name := svcPort.TargetPort.StrVal; name != "" {
			if port, ok := portsByName[name]; ok {
				add(port)
			}
		}
	}

	if len(unique) == 0 && svcPort.Name != "" {
		if port, ok := portsByName[svcPort.Name]; ok {
			add(port)
		}
	}

	if len(unique) == 0 {
		for _, port := range subset.Ports {
			if port.Port == svcPort.Port {
				add(port.Port)
			}
		}
	}

	if len(unique) == 0 && len(subset.Ports) == 1 {
		add(subset.Ports[0].Port)
	}

	ports := make([]int32, 0, len(unique))
	for port := range unique {
		ports = append(ports, port)
	}
	return ports
}

func (b ServiceBackendRef) signature() string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%s\x00%d", b.ServiceNamespace, b.ServiceName, b.ClusterIP, b.ServicePort, b.PodIP, b.PodPort)
}
