module github.com/Alluxio/integration/csi

go 1.15

require (
	github.com/container-storage-interface/spec v1.1.0
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529
	github.com/kubernetes-csi/csi-lib-utils v0.7.0 // indirect
	github.com/kubernetes-csi/drivers v1.0.2
	github.com/pkg/errors v0.8.1
	github.com/spf13/cobra v1.1.3
	golang.org/x/net v0.7.0
	google.golang.org/grpc v1.37.1
	k8s.io/api v0.17.0
	k8s.io/apimachinery v0.17.1-beta.0
	k8s.io/client-go v0.17.0
	k8s.io/mount-utils v0.21.0
)
