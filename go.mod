module flashcat.cloud/categraf/servicemap-aggregator

go 1.21

require (
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/prometheus/prometheus v0.48.0
	k8s.io/apimachinery v0.29.0
	k8s.io/client-go v0.29.0
)

// 运行以下命令补全依赖：
//   cd servicemap-aggregator
//   go mod tidy
