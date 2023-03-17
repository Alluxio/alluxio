module github.com/Alluxio/alluxio

go 1.18

require (
	github.com/docker/go-units v0.3.3
	github.com/go-logr/logr v0.1.0
	github.com/onsi/ginkgo v1.6.0
	github.com/onsi/gomega v1.4.2
	github.com/pkg/errors v0.8.1
	go.uber.org/zap v1.9.1
	gopkg.in/yaml.v2 v2.2.8
	k8s.io/api v0.0.0
	k8s.io/apimachinery v0.0.0
	k8s.io/client-go v0.0.0
	k8s.io/kubernetes v1.15.4
	sigs.k8s.io/controller-runtime v0.3.0
)

require (
	cloud.google.com/go v0.34.0 // indirect
	github.com/beorn7/perks v1.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/docker/spdystream v0.0.0-20160310174837-449fdfce4d96 // indirect
	github.com/evanphx/json-patch v4.5.0+incompatible // indirect
	github.com/go-logr/zapr v0.1.0 // indirect
	github.com/gogo/protobuf v1.1.1 // indirect
	github.com/golang/groupcache v0.0.0-20180513044358-24b0969c4cb7 // indirect
	github.com/golang/protobuf v1.3.2 // indirect
	github.com/google/go-cmp v0.3.0 // indirect
	github.com/google/gofuzz v0.0.0-20170612174753-24818f796faf // indirect
	github.com/google/uuid v1.0.0 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/hpcloud/tail v1.0.0 // indirect
	github.com/imdario/mergo v0.3.6 // indirect
	github.com/json-iterator/go v1.1.6 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/prometheus/client_golang v1.0.0 // indirect
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90 // indirect
	github.com/prometheus/common v0.4.1 // indirect
	github.com/prometheus/procfs v0.0.2 // indirect
	github.com/spf13/pflag v1.0.2 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/oauth2 v0.0.0-20190402181905-9f3314589c9a // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/term v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2 // indirect
	golang.org/x/xerrors v0.0.0-20190717185122-a985d3407aa7 // indirect
	gomodules.xyz/jsonpatch/v2 v2.0.1 // indirect
	google.golang.org/appengine v1.5.0 // indirect
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	k8s.io/apiextensions-apiserver v0.0.0 // indirect
	k8s.io/klog v0.3.3 // indirect
	k8s.io/kube-openapi v0.0.0-20190228160746-b3a7cee44a30 // indirect
	k8s.io/utils v0.0.0-20190506122338-8fab8cb257d5 // indirect
	sigs.k8s.io/testing_frameworks v0.1.1 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)

replace k8s.io/api => k8s.io/api v0.0.0-20190918195907-bd6ac527cfd2

replace k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20190918201827-3de75813f604

replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20190817020851-f2f3a405f61d

replace k8s.io/apiserver => k8s.io/apiserver v0.0.0-20190918200908-1e17798da8c1

replace k8s.io/cli-runtime => k8s.io/cli-runtime v0.0.0-20190918202139-0b14c719ca62

replace k8s.io/client-go => k8s.io/client-go v0.0.0-20190918200256-06eb1244587a

replace k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20190918203125-ae665f80358a

replace k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.0.0-20190918202959-c340507a5d48

replace k8s.io/code-generator => k8s.io/code-generator v0.15.11-beta.0

replace k8s.io/component-base => k8s.io/component-base v0.0.0-20190918200425-ed2f0867c778

replace k8s.io/cri-api => k8s.io/cri-api v0.15.11-beta.0

replace k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.0.0-20190918203248-97c07dcbb623

replace k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.0.0-20190918201136-c3a845f1fbb2

replace k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.0.0-20190918202837-c54ce30c680e

replace k8s.io/kube-proxy => k8s.io/kube-proxy v0.0.0-20190918202429-08c8357f8e2d

replace k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.0.0-20190918202713-c34a54b3ec8e

replace k8s.io/kubectl => k8s.io/kubectl v0.15.11-beta.0

replace k8s.io/kubelet => k8s.io/kubelet v0.0.0-20190918202550-958285cf3eef

replace k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.0.0-20190918203421-225f0541b3ea

replace k8s.io/metrics => k8s.io/metrics v0.0.0-20190918202012-3c1ca76f5bda

replace k8s.io/node-api => k8s.io/node-api v0.0.0-20190918203548-2c4c2679bece

replace k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.0.0-20190918201353-5cc279503896

replace k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.0.0-20190918202305-ed68a9f09ae1

replace k8s.io/sample-controller => k8s.io/sample-controller v0.0.0-20190918201537-fabef0de90df
