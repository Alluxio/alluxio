---
布局: 全局
标题: 在 Kubernetes 中安装 Alluxio
---

本文档描述如何通过 kubernetes 包管理器 [Helm](https://helm.sh/) 和 kubernetes [Operator](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/),
在 Kubernetes 中安装 Alluxio (Dora)。

我们推荐使用 operator 在 Kubernetes 中部署 Alluxio。如果没有一些必须的权限，可以考虑使用 helm chart.

## 先决条件

- 一个大于等于 1.19 版本且开启了 feature gate 的 Kubernetes 集群。
- Kubernetes 集群可以访问 Alluxio Docker 镜像 [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}/). 
如果使用了 Docker 私有镜像仓库, 请参考 [Kubernetes 私有镜像仓库文档](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/).
- 确保 [Kubernetes 集群的网络策略](https://kubernetes.io/docs/concepts/services-networking/network-policies/) 允许应用 (Alluxio 客户端) 通过指定的端口访问 Alluxio Pod。
- Kubernetes 集群的 control plane 安装了大于等于3.6.0 版本的 [helm 3](https://helm.sh/docs/intro/install/)。

## Operator

### Operator 的其它先决条件
为确保 Operator 正常工作，还需要 Kubernetes 集群的如下 RBAC 权限 ：
1. 拥有创建 CRD (Custom Resource Definition) 的权限;
2. 拥有为 operator pods 创建 ServiceAccount, ClusterRole, 和 ClusterRoleBinding 的权限;
3. 拥有创建 operator 的 namespace 的权限.


### 部署 Alluxio Kubernetes Operator
可以参考如下步骤，通过  Helm Chart 来部署 Alluxio Kubernetes Operator:

#### 1.下载 Alluxio Kubernetes Operator

下载 [Alluxio Kubernetes Operator](https://github.com/Alluxio/k8s-operator) 并进入工程的根目录。

#### 2. 安装 Operator

通过如下命令安装 operator:
```shell
$ helm install operator ./deploy/charts/alluxio-operator
```
该 Operator 会自动创建命名空间 `alluxio-operator` 并在该命名空间下安装所有组件.

#### 3. 运行 Operator

确保 operator 正常运行：
```shell
$ kubectl get pods -n alluxio-operator
```

### 部署 Dataset

#### 1. 准备 Dataset 配置文件

创建 dataset 配置文件 `dataset.yaml`. 其 `apiVersion` 必须是 `k8s-operator.alluxio.com/v1alpha1` 且 `kind` 必须是 `Dataset`. 示例如下:
```yaml
apiVersion: k8s-operator.alluxio.com/v1alpha1
kind: Dataset
metadata:
  name: my-dataset
spec:
  dataset:
    path: <dataset 路径>
    credentials:
      - <访问 dataset 的参数1>
      - <访问 dataset 的参数2>
      - ...
```

#### 2. 部署 Dataset

通过如下命令部署 dataset：
```shell
$ kubectl create -f dataset.yaml
```

#### 3. 检查 Dataset 的状态

通过如下命令检查 dataset 的状态： 
```shell
$ kubectl get dataset <dataset-name>
```

### 部署 Alluxio

#### 1. 配置 Persistent Volumes

为如下组件配置 [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/):

1. (可选) Embedded journal，支持 HostPath；
2. (可选) Worker page store， 支持 HostPath；
3. (可选) Worker metastore，只有在使用 RocksDB 存储 worker 元数据时才需要。

如下是一个配置 Alluxio embedded journal 使用 hostPath 类型 PersistentVolume 的示例:
```yaml
kind: PersistentVolume
apiVersion: v1
metadata:
  name: alluxio-journal-0
  labels:
    type: local
spec:
  storageClassName: standard
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: /tmp/alluxio-journal-0
```
注意:
- 如果 embedded journal 使用 hostPath 类型的 volume, Alluxio 会使用 root 用户运行 init container 并为自己赋予对应目录的 RWX 权限；
- 每个 journal volume 的容量，至少需要大于等于对应的 persistentVolumeClaim 中申请的容量，该申请的容量可以通过配置文件指定，详情见下文步骤二；
- 如果使用 hostPath 类型的 persistent volume, 需要确保 alluxio 用户拥有 RWX 权限。
  - 默认情况下，Alluxio 容器以用户 `alluxio` 和用户组 `alluxio` 运行，对应 UID 1000 和 GID 1000。

#### 2. 准备资源配置文件

准备资源配置文件 `alluxio-config.yaml`，其 `apiVersion` 必须是 `k8s-operator.alluxio.com/v1alpha1` 且 `kind` 必须是 `AlluxioCluster`. 示例如下:
```yaml
apiVersion: k8s-operator.alluxio.com/v1alpha1
kind: AlluxioCluster
metadata:
  name: my-alluxio-cluster
spec:
  <configurations>
```

上述配置文件的 `spec` 章节 **必须** 指定 `Dataset` 的名称。 

上述配置文件的 `spec` 章节中的其它可配置参数，可以参考 `deploy/charts/alluxio/values.yaml`.

#### 3. 部署 Alluxio 集群

通过如下命令部署 Alluxio 集群：
```shell
$ kubectl create -f alluxio-config.yaml
```

#### 4. 检查 Alluxio 集群的状态

通过如下命令检查 Alluxio 集群的状态:
```shell
$ kubectl get alluxiocluster <alluxio-cluster-name>
```

### 卸载 Dataset 和 Alluxio 集群

通过如下命令卸载 Dataset 和 Alluxio 集群:
```shell
$ kubectl delete dataset <dataset-name>
$ kubectl delete alluxiocluster <alluxio-cluster-name>
```

### [红利] 加载数据到 Alluxio

为加载数据到 Alluxio 以加快应用访问数据的速度， 可以创建资源配置文件 `load.yaml`，示例如下：
```yaml
apiVersion: k8s-operator.alluxio.com/v1alpha1
kind: Load
metadata:
  name: my-load
spec:
  dataset: <dataset-name>
  path: /
```

可以通过如下命令，开始数据的加载：
```shell
$ kubectl create -f load.yaml 
```

可以通过如下命令，查看数据加载的状态：
```shell
$ kubectl get load
```

### [红利]  AI/ML 场景下的 Alluxio 集群配置示例
```yaml
apiVersion: k8s-operator.alluxio.com/v1alpha1
kind: AlluxioCluster
metadata:
  name: my-alluxio-cluster
spec:
  worker:
    count: 4
  pagestore:
    type: hostPath
    quota: 512Gi
    hostPath: /mnt/alluxio
  csi:
    enabled: true
```

## Helm

### 部署 Alluxio

可以参考如下步骤，在 Kubernetes 集群中部署 Dora:

#### 1. 下载 Helm Chart

下载 [Helm chart](https://github.com/Alluxio/k8s-operator/deploy/charts/alluxio)，并进入 helm chart 目录。

#### 2. 配置 Persistent Volumes

为如下组件配置 [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) :

1. (可选) Embedded journal，支持 HostPath；
2. (可选) Worker page store， 支持 HostPath；
3. (可选) Worker metastore，只有在使用 RocksDB 存储 worker 元数据时才需要。

如下是一个配置 Alluxio embedded journal 使用 hostPath 类型 PersistentVolume 的示例:

```yaml
kind: PersistentVolume
apiVersion: v1
metadata:
  name: alluxio-journal-0
  labels:
    type: local
spec:
  storageClassName: standard
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: /tmp/alluxio-journal-0
```
注意:
- 如果 embedded journal 使用 hostPath 类型的 volume, Alluxio 会使用 root 用户运行 init container 并为自己赋予对应目录的 RWX 权限；
- 每个 journal volume 的容量，至少需要大于等于对应的 persistentVolumeClaim 申请的容量，该申请的容量可以通过配置文件指定，详情见下文步骤三；
- 如果使用 hostPath 类型的 persistent volume, 需要确保 alluxio 用户拥有 RWX 权限。
  - 默认情况下，Alluxio 容器以用户 `alluxio` 和用户组 `alluxio` 运行，对应 UID 1000 和 GID 1000。
#### 3. 准备配置文件

准备配置文件 `config.yaml`, 所有可配置属性可以在上文步骤一中下载的文件 `values.yaml` 中找到。

用户 **必须** 在 `config.yaml`中指定自己的 dataset 相关配置。确切来讲，需要在如下章节中指定：
```yaml
## Dataset ##

dataset:
  # dataset 的路径，比如：s3://my-bucket/dataset
  path:
  # 任何 Alluxio 访问 dataset 的凭证，比如：
  # credentials:
  #   aws.accessKeyId: XXXXX
  #   aws.secretKey: xxxx
  credentials:
```

#### 4. 安装 Dora 集群

通过如下命令安装 Dora 集群： 
```shell
$ helm install dora -f config.yaml .
```
等待一段时间后，Dora 集群就启动成功了。可以通过如下命令检查 pod 和 container 的状态： 
```shell
$ kubectl get po
```

### 卸载

可以通过如下命令卸载 Dora 集群:
```shell
$ helm delete dora
```

## metrics

关于如何为 Kubernetes 集群中的 Alluxio，配置和获取各种 metrics sink 中的 metrics， 请参考 [Metrics On Kubernetes]({{ '/en/kubernetes/Metrics-On-Kubernetes.html' | relativize_url }})。