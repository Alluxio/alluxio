---
layout: global
title: 在Kubernetes上部署Alluxio
nickname: Kubernetes上的Alluxio
group: Install Alluxio
priority: 4
---

Alluxio可以在Kubernetes上运行。本指南演示了如何使用Docker映像或`helm`中包含的规范在Kubernetes上运行Alluxio

*目录
{:toc}

## 先决条件

一个Kubernetes集群(版本> = 1.8)。在默认规范下，Alluxio workers可以通过设置`sizeLimit`参数来决定`emptyDir`卷的大小。这是Kubernetes 1.8版本中的一个Alpha特性。在使用前请确保此功能已启用。

一个Alluxio Docker镜像[alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}/)。如果使用私有Docker注册表，请参阅Kubernetes [documentation](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/)。
确保[Kubernetes网络策略](https://kubernetes.io/docs/concepts/services-networking/network-policies/)允许应用程序(Alluxio客户端)和Alluxio Pods之间在已定义端口上的连接。

## 基本设置

本教程介绍了在Kubernetes上的基本Alluxio安装。 Alluxio支持在Kubernetes上两种安装方法:使用[helm](https://helm.sh/docs/)图表或使用`kubectl`。如果可选，`helm`是首选安装Alluxio方法。如果没法使用`helm`安装或需要额外定制化部署，则可以直接通过原生Kubernetes资源规范使用`kubectl`。

>注意:从Alluxio 2.3起，Alluxio仅支持helm 3。
>参阅如何[从helm 2迁移到helm 3](https://helm.sh/docs/topics/v2_v3_migration/)。

{% accordion setup %}
  {% collapsible (Optional) Extract Kubernetes Specifications %}

如果使用私有`helm` 仓库或使用原生Kubernetes规范，从Docker镜像中提取部署Alluxio所需的Kubernetes规范。

```console
$ id=$(docker create alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}:{{site.ALLUXIO_VERSION_STRING}})
$ docker cp $id:/opt/alluxio/integration/kubernetes/ - > kubernetes.tar
$ docker rm -v $id 1>/dev/null
$ tar -xvf kubernetes.tar
$ cd kubernetes
```

 {% endcollapsible %}
  {% collapsible (Optional) Provision a Persistent Volume %}
注意:[嵌入式日志]({{ '/en/operation/Journal.html' | relativize_url}}＃embedded-journal-configuration)
需要为每个要发放的 master Pod设置一个持久卷，这是Alluxio运行在kubernetes上的首选HA机制。一旦创建了该卷，即使master进程重启不会影响持久卷的内容。

当使用[UFS日志]({{ '/en/operation/Journal.html' | relativize_url}}＃ufs-journal-configuration)时，Alluxio master也可以配置为使用[持久卷](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
来存储日志。如果你在用UFS日志并使用外部日志存储位置(例如HDFS)，可以跳过此节所余部分。

有多种创建持久卷的方法。
下面是一个用`hostPath`来定义的示例: 

```yaml
# Name the file alluxio-master-journal-pv.yaml
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


>注意:默认每个日志卷应至少1GI，因为每个alluxio master Pod将有一个请求1Gi存储的PersistentVolumeClaim。后面部分会介绍如何设置日志大小。

然后使用`kubectl`来创建持久卷:

```console
$ kubectl create -f alluxio-master-journal-pv.yaml
```


还有其他方法来创建持久卷如[文档](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
  {% endcollapsible %}
{% endaccordion %}

### 部署

{% navtabs deploy %} 
{% navtab helm %}

#### 前提条件

A.安装Helm

你应该已经安装了helm 3.X。
可以按照[此处](https://helm.sh/docs/intro/install/)中的说明安装helm。

B.必须有包括Alluxio helm chart的helm repro

```console
$ helm repo add alluxio-charts https://alluxio-charts.storage.googleapis.com/openSource/{{site.ALLUXIO_VERSION_STRING}}
```
 
#### 配置

一旦helm repository可用，就可以准备Alluxio配置。
最小配置必须包含底层存储地址:

```properties
properties:
  alluxio.master.mount.table.root.ufs: "<under_storage_address>"
```

 >注意:必须修改底层文件系统地址。任何凭证都必须修改。

要查看完整的支持属性列表，请运行`helm inspect`命令

```console
$ helm inspect values alluxio-charts/alluxio
```

本节的剩余部分通过例子描述各种配置选项。

{% accordion helmConfig %}
  {% collapsible Example: Amazon S3 as the under store %}
To [mount S3]({{ '/en/ufs/S3.html' | relativize_url }}#root-mount-point)
在Alluxio根名称空间以key-value pair方式指定所有必须属性。

```properties
properties:
  alluxio.master.mount.table.root.ufs: "s3a://<bucket>"
  alluxio.master.mount.table.root.option.aws.accessKeyId: "<accessKey>"
  alluxio.master.mount.table.root.option.aws.secretKey: "<secretKey>"
```

{% endcollapsible %}

  {% collapsible Example: Single Master and Journal in a Persistent Volume %}
 The following configures [UFS Journal]({{ '/en/operation/Journal.html' | relativize_url}}#ufs-journal-configuration) 将一个持久卷本地挂载在master Pod的位置 `/journal`。

```properties
master:
  count: 1 # For multiMaster mode increase this to >1

journal:
  type: "UFS"
  ufsType: "local"
  folder: "/journal"
  size: 1Gi
  # volumeType controls the type of journal volume.
  # It can be "persistentVolumeClaim" or "emptyDir"
  volumeType: persistentVolumeClaim
  # Attributes to use when the journal is persistentVolumeClaim
  storageClass: "standard"
  accessModes:
    - ReadWriteOnce
```

{% endcollapsible %}

   {% collapsible Example: 下方举例说明如何将一个持久卷挂载在本地master pod %}
'/journal'位置来配置 [UFS Journal]({{ '/en/operation/Journal.html' | relativize_url}}#ufs-journal-configuration)
 将一个`emptyDir` 卷本地挂载在master Pod的位置`/journal` 


```properties
master:
  count: 1 # For multiMaster mode increase this to >1

journal:
  type: "UFS"
  ufsType: "local"
  folder: "/journal"
  size: 1Gi
  # volumeType controls the type of journal volume.
  # It can be "persistentVolumeClaim" or "emptyDir"
  volumeType: emptyDir
  # Attributes to use when the journal is emptyDir
  medium: ""
```


>注意:`emptyDir`卷的寿命与Pod寿命相同。
 它不是持久性存储。
当Pod重新启动或被重新调度时，Alluxio日志将丢失。请仅在实验用例中使用。检查[emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir)了解更详细信息。

  {% endcollapsible %}

{% collapsible Example: HDFS as Journal %} 

首先为HDFS客户端所需的任何配置创建secrets。它们将挂载在`/secrets`下。

```console
$ kubectl create secret generic alluxio-hdfs-config --from-file=${HADOOP_CONF_DIR}/core-site.xml --from-file=${HADOOP_CONF_DIR}/hdfs-site.xml
```
 
```properties
journal:
  type: "UFS"
  ufsType: "HDFS"
  folder: "hdfs://{$hostname}:{$hostport}/journal"
 
properties:
  alluxio.master.mount.table.root.ufs: "hdfs://<ns>"
  alluxio.master.journal.ufs.option.alluxio.underfs.hdfs.configuration: "/secrets/hdfsConfig/core-site.xml:/secrets/hdfsConfig/hdfs-site.xml"
 
secrets:
  master:
    alluxio-hdfs-config: hdfsConfig
  worker:
    alluxio-hdfs-config: hdfsConfig
```
 
{% endcollapsible %}

{% collapsible Example: Multi-master with Embedded Journal in Persistent Volumes %}

 ```properties
master:
  count: 3

journal:
  type: "EMBEDDED"
  folder: "/journal"
  # volumeType controls the type of journal volume.
  # It can be "persistentVolumeClaim" or "emptyDir"
  volumeType: persistentVolumeClaim
  size: 1Gi
  # Attributes to use when the journal is persistentVolumeClaim
  storageClass: "standard"
  accessModes:
    - ReadWriteOnce
```

{% endcollapsible %}
{% collapsible Example: Multi-master with Embedded Journal in emptyDir Volumes %}

```properties
master:
  count: 3
  
journal:
  type: "UFS"
  ufsType: "local"
  folder: "/journal"
  size: 1Gi
  # volumeType controls the type of journal volume.
  # It can be "persistentVolumeClaim" or "emptyDir"
  volumeType: emptyDir
  # Attributes to use when the journal is emptyDir
  medium: ""
```


>注意:`emptyDir`卷的寿命与Pod寿命相同。 它不是持久性存储。当Pod重新启动或被重新调度时，Alluxio日志将丢失。请仅在实验用例中使用。检查[emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir)了解更详细信息。

{% endcollapsible %}
{% collapsible Example: HDFS as the under store %}

首先为HDFS客户端所需的任何配置创建secrets。它们将挂载在`/secrets`下。

```console
$ kubectl create secret generic alluxio-hdfs-config --from-file=${HADOOP_CONF_DIR}/core-site.xml --from-file=${HADOOP_CONF_DIR}/hdfs-site.xml
```

```properties
properties:
  alluxio.master.mount.table.root.ufs: "hdfs://<ns>"
  alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration: "/secrets/hdfsConfig/core-site.xml:/secrets/hdfsConfig/hdfs-site.xml"
secrets:
  master:
    alluxio-hdfs-config: hdfsConfig
  worker:
    alluxio-hdfs-config: hdfsConfig
```

{% endcollapsible %}
{% collapsible Example: Off-heap Metastore Management in Persistent Volumes %} 

以下配置为每个Alluxio master Pod创建一个`PersistentVolumeClaim`，并将Pod
配置为使用该卷作为一个on-disk基于RocksDB的metastore。

```properties
properties:
  alluxio.master.metastore: ROCKS
  alluxio.master.metastore.dir: /metastore

metastore:
  volumeType: persistentVolumeClaim # Options: "persistentVolumeClaim" or "emptyDir"
  size: 1Gi
  mountPath: /metastore
  # Attributes to use when the metastore is persistentVolumeClaim
  storageClass: "standard"
  accessModes:
   - ReadWriteOnce
```
  
{% endcollapsible %}

 {% collapsible Example: Off-heap Metastore Management in `emptyDir` Volumes %} 

以下配置为每个Alluxio master Pod创建一个`emptyDir`卷，并将Pod配置为使用该卷作为一个on-disk基于RocksDB的metastore。

```properties
properties:
  alluxio.master.metastore: ROCKS
  alluxio.master.metastore.dir: /metastore

metastore:
  volumeType: emptyDir # Options: "persistentVolumeClaim" or "emptyDir"
  size: 1Gi
  mountPath: /metastore
  # Attributes to use when the metastore is emptyDir
  medium: ""
```

>注意:`emptyDir`卷的寿命与Pod寿命相同。 
它不是持久性存储。
当Pod重新启动或被重新调度时，Alluxio日志将丢失。请仅在实验用例中使用。检查[emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir)了解更详细信息)。

{% endcollapsible %}

  {% collapsible Example: Multiple Secrets %} 

 可以将多个secrets同时挂载到master和workerPods。每个Pod区域的格式为`<secretName>:<mountPath>`

```properties
secrets:
  master:
    alluxio-hdfs-config: hdfsConfig
    alluxio-ceph-config: cephConfig
  worker:
    alluxio-hdfs-config: hdfsConfig
    alluxio-ceph-config: cephConfig
```


{% endcollapsible %}
  {% collapsible Examples: Alluxio Storage Management %} 

Alluxio在worker Pods上管理本地存储，包括内存。[Multiple-Tier Storage]({{ '/en/core-services/Caching.html#multiple-tier-storage' | relativize_url }}) 可以使用以下参考配置来设置。

支持的3 种卷类型 `type`:[hostPath](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath)，[emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/＃emptydir) 
 和[persistentVolumeClaim](https://kubernetes.io/docs/concepts/storage/volumes/#persistentvolumeclaim)。

**仅内存级**

```properties
tieredstore:
  levels:
  - level: 0
    mediumtype: MEM
    path: /dev/shm
    type: emptyDir
    high: 0.95
    low: 0.7
```

**内存和SSD存储多级**

```properties
tieredstore:
  levels:
  - level: 0
    mediumtype: MEM
    path: /dev/shm
    type: hostPath
    high: 0.95
    low: 0.7
  - level: 1
    mediumtype: SSD
    path: /ssd-disk
    type: hostPath
    high: 0.95
    low: 0.7
```


> 注意:如果在运行时创建了一个`hostPath`文件或目录，只有`root`用户能用。`hostPath`卷没有资源大小限制。你可以使用`root`权限运行Alluxio容器，或者确保存在具有UID和GID 1000 的`alluxio`用户名用户可以进行访问的本地路径。可以在[此处](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath)找到更多详细信息。

**内存和SSD存储 多级存储使用PVC**

你还可以每层使用PVC并发放[PersistentVolume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)。对于worker分层存储，使用`hostPath`或`local`卷，以便worker可以在本地读写，以实现最佳性能。
 
```properties
tieredstore:
  levels:
  - level: 0
    mediumtype: MEM
    path: /dev/shm
    type: persistentVolumeClaim
    name: alluxio-mem
    quota: 1G
    high: 0.95
    low: 0.7
  - level: 1
    mediumtype: SSD
    path: /ssd-disk
    type: persistentVolumeClaim
    name: alluxio-ssd
    quota: 10G
    high: 0.95
    low: 0.7
```
>注意:每一层有一个PVC。 当PVC绑定到类型为`hostPath`或` local`的PV时，每个 worker Pod都将使用Node上的本地路径。`本地`卷需要`nodeAffinity`，并且使用此卷的Pod只能在`nodeAffinity`卷规则中指定的节点上运行。可以在[此处](https://kubernetes.io/docs/concepts/storage/volumes/#local)中找到更多详细信息。

**内存和SSD存储 单级存储**

可以在同一层上拥有多个卷。此配置将为每个卷创建一个`persistentVolumeClaim`。

```properties
tieredstore:
  levels:
  - level: 0
    mediumtype: MEM,SSD
    path: /dev/shm,/alluxio-ssd
    type: persistentVolumeClaim
    name: alluxio-mem,alluxio-ssd
    quota: 1GB,10GB
    high: 0.95
    low: 0.7
```
 {% endcollapsible %}
{% endaccordion %}


#### 安装

一旦完成名为'config.yaml`的文件，运行如下命令安装:

```console
$ helm install alluxio -f config.yaml alluxio-charts/alluxio
```
#### 卸载

运行如下命令卸载Alluxio:

```console
$ helm delete alluxio
```

#### 格式化日志

StatefulSet中的master Pods在启动是使用`initContainer`来格式化日志。`initContainer`由`journal.format.runFormat = true`设置打开。 默认情况下，master启动时日志没有格式化。

可以使用升级现有的helm部署来触发日志格式化
`journal.format.runFormat = true`。

```console
# Use the same config.yaml and switch on journal formatting
$ helm upgrade alluxio -f config.yaml --set journal.format.runFormat=true alluxio-charts/alluxio
```
> 注:'helm upgrade`将重新创建 master Pods。

或者，可以在部署时触发日志格式化。
```console
$ helm install alluxio -f config.yaml --set journal.format.runFormat=true alluxio-charts/alluxio
```

{% endnavtab %}
 {% navtab kubectl %}

#### 选择YAML模板样例

规范目录下的子目录包含一组常见部署方案的YAML模板:
*singleMaster-localJournal*, *singleMaster-hdfsJournal* and *multiMaster-embeddedJournal*.

>*singleMaster *意味者模板会产生一个 Alluxio master进程, *multiMaster*意味者 三个. *embedded*和*ufs*是两个[journal modes]({{ '/en/operation/Journal.html' | relativize_url}}) Alluxio 支持.

-*singleMaster-localJournal *目录为你提供必要的Kubernetes ConfigMap，1个Alluxio master进程和一组Alluxio workers。Alluxio master将日志写入`volumeClaimTemplates`请求的日志卷中。
-*multiMaster-EmbeddedJournal*目录为你提供Kubernetes ConfigMap，3个Alluxio masters和
一组Alluxio workers。每个Alluxio master将日志写入由volumeClaimTemplates请求的自己的日志卷中。
-*singleMaster-hdfsJournal*目录为你提供Kubernetes ConfigMap，1个Aluxio master以及一
组workers。日志位于共享的UFS路径。在此模板中，我们将用HDFS作为UFS。

#### 配置

一旦部署选项确定，从相应子目录复制模板

```console
$ cp alluxio-configmap.yaml.template alluxio-configmap.yaml
```

按需要修改或添加任何配置属性。必须修改Alluxi底层文件系统地址。
任何凭证都必须修改。
添加到`ALLUXIO_JAVA_OPTS`:

```properties
-Dalluxio.master.mount.table.root.ufs=<under_storage_address>
```

注:
-用适当的URI替换`<under_storage_address>`，例如s3://my-bucket。如果使用要求凭据的底层存储不足，请确保也指定所需凭据。
-通过主机网络运行Alluxio时，分配给Alluxio服务的端口不得被预先占用。

创建一个ConfigMap。
```console
$ kubectl create -f alluxio-configmap.yaml
```

#### 安装

***准备规范。***基于模板准备Alluxio部署规范。修改任何所需参数，例如Docker镜像的位置以及Pod的CPU和内存要求。

为master(s)，创建`Service`和`StatefulSet`:

```console
$ mv master/alluxio-master-service.yaml.template master/alluxio-master-service.yaml
$ mv master/alluxio-master-statefulset.yaml.template master/alluxio-master-statefulset.yaml
```

>注意:如果需要，`alluxio-master-statefulset.yaml`用`volumeClaimTemplates`为每个master定义日志卷.

为workers，创建`DaemonSet`:

```console
$ mv worker/alluxio-worker-daemonset.yaml.template worker/alluxio-worker-daemonset.yaml
```
注意:确保该Kubernetes规范版本与所使用Alluxio Docker镜像版本是一致的。

{% accordion remoteAccess %}
  {% collapsible (Optional) Remote Storage Access %}
当Alluxio要连接到所部署在的Kubernetes集群之外存储主机时，可能还需要执行额外步骤。本节以下部分将说明如何配置可访问但不受Kubernetes管理的远程HDFS连接。

**步骤1:为HDFS连接添加`hostAliases`。**Kubernetes Pods无法识别不由Kubernetes管理的网络主机名(因为不是一个Kubernetes Service)，除非已经通过hostAliases定义好。
[hostAliases](https://kubernetes.io/docs/concepts/services-networking/add-entries-to-pod-etc-hosts-with-host-aliases/#adding-additional-entries-with-hostaliases)。

例如，如果你的HDFS服务可以通过`hdfs://<namenode>:9000`访问，其中`<namenode>`是
主机名，则需要在`spec`中为所有Alluxio Pod添加`hostAliases`来建立从主机名到IP地址的映射。

```yaml
spec:
  hostAliases:
  - ip: "<namenode_ip>"
    hostnames:
    - "<namenode>"
```

`alluxio-master-statefulset.yaml.template`和`alluxio-worker-daemonset.yaml.template`模板中StatefulSet或DaemonSet类型下,应该按如下所示将`hostAliases`部分添加到`spec.template.spec`的每个部分中。

```yaml
kind: StatefulSet
metadata:
  name: alluxio-master
spec:
  ...
  serviceName: "alluxio-master"
  replicas: 1
  template:
    metadata:
      labels:
        app: alluxio-master
    spec:
      hostAliases:
      - ip: "ip for hdfs-host"
        hostnames:
        - "hdfs-host"
```
**步骤2:为HDFS配置文件创建Kubernetes Secret。**运行以下命令为HDFS客户端配置创建Kubernetes Secret。

```console
kubectl create secret generic alluxio-hdfs-config --from-file=${HADOOP_CONF_DIR}/core-site.xml --from-file=${HADOOP_CONF_DIR}/hdfs-site.xml
```
 
这两个配置文件在`alluxio-master-statefulset.yaml`和`alluxio-worker-daemonset.yaml`中会引用到。Alluxio进程需要HDFS配置文件才能连接，这些文件在容器中的位置由属性`alluxio.underfs.hdfs.configuration`控制。

**步骤3:修改`alluxio-configmap.yaml.template`。现在Pods已经知道如何连接到HDFS服务，下面更新`alluxio.master.journal.folder`和`alluxio.master.mount.table .root.ufs`并指向要连接的目标HDFS服务。
 {% endcollapsible %}
{% endaccordion %}

一旦完成所有先决条件和配置，就可以部署部署Alluxio了。

```console
$ kubectl create -f ./master/
$ kubectl create -f ./worker/
```
#### 卸载

卸载Alluxio如下

```console
$ kubectl delete -f ./worker/
$ kubectl delete -f ./master/
$ kubectl delete configmap alluxio-config
```

>注意:这将删除`./master/`和`./worker/`下的所有资源。 如果在这些目录下由持久卷或其他重要资源请注意不要不小心一并删除。  

#### 格式化日志

您可以手动添加一个`initContainer`以便在Pod创建时格式化日志。`initContainer`将在创建Pod时运行`alluxio formatJournal`并格式化日志。
```yaml
- name: journal-format
  image: alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}:{{site.ALLUXIO_VERSION_STRING}}
  imagePullPolicy: IfNotPresent
  securityContext:
    runAsUser: 1000
  command: ["alluxio","formatJournal"]
  volumeMounts:
    - name: alluxio-journal
      mountPath: /journal
```
> 注:从Alluxio V2.1及更高版本，默认Alluxio Docker容器除了Fuse以外将以非root 具有UID 1000和GID 1000 的用户`alluxio` 身份运行。 确保Alluxio master Pod运行和日志格式化都是以同一用户身份进行的。 

#### 升级

本节将介绍如何使用`kubectl`升级Kubernetes集群中的Alluxio。
{% accordion kubectlUpgrade %}
  {% collapsible Upgrading Alluxio %}
**步骤1:升级docker镜像版本标签**

每个Alluxio版本发布都会有相对应的docker镜像发布在
[dockerhub](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}})。

你应该更新所有Alluxio容器的`image`字段使用目标版本标签。标签`latest`将指向最新的稳定版本。

例如，如果要将Alluxio升级到最新的稳定版本，按如下更新容器:

```yaml
containers:
- name: alluxio-master
  image: alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}:latest
  imagePullPolicy: IfNotPresent
  ...
- name: alluxio-job-master
  image: alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}:latest
  imagePullPolicy: IfNotPresent
  ...
```

**第2步:停止运行Alluxio master和worker Pods**

通过删除其DaemonSet进行来终止所有正在运行的Alluxio worker Pods。

```console
$ kubectl delete daemonset -l app=alluxio
```
然后通过终止每个StatefulSet和每个标签为`app=alluxio`的服务来终止所有正在运行的Alluxio master

```console
$ kubectl delete service -l app=alluxio
$ kubectl delete statefulset -l app=alluxio
```
确保在进行下一步之前所有Pods都已经终止。

**步骤3:如有必要，格式化日志和Alluxio存储**

看下Alluxio升级之指南关于是否要格式化Alluxio master日志。如果不需要格式化，则可以跳过本节的剩余部分直接跳到重新启动所有Alluxio master和worker Pod部分。

您可以按照[formatting journal with kubectl]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html#format-journal-1' | relativize_url}})
来格式化Alluxio日志。 

如果你在使用[分层存储]({{ '/en/core-services/Caching.html#multiple-tier-storage' | relativize_url}})来运行Alluxio workers，并且已为Alluxio配置了持久卷，则也要清除存储。您应该删除现有并重新创建新持久卷。

一旦完成格式化所有日志和Alluxio存储后，就可以重新启动Alluxio master和worker Pods了。

**步骤4:重新启动Alluxio master和worker Pods**

现在Alluxio masters and worker容器都升级到所要的版本。可以重新启动运行了。

从YAML文件中重新启动Alluxio master and worker Pods。
 
```console
$ kubectl create -f ./master/
$ kubectl create -f ./worker/
```

**第5步:验证Alluxio master and worker Pods已经重新启动运行**

你应该确认Alluxio Pods已经重新启动并在运行状态。

```console
# You should see all Alluxio master and worker Pods
$ kubectl get pods
```

你可以根据以下文档做更全面的确认 [Verify Alluxio]({{ '/en/deploy/Running-Alluxio-Locally.html?q=verify#verify-alluxio-is-running' | relativize_url}}).  
{% endcollapsible %}
{% endaccordion %}

{% endnavtab %}
{% endnavtabs %}

### 访问Web UI

可以使用端口转发从kubernetes集群外部访问Alluxio UI。

```console
$ kubectl port-forward alluxio-master-$i 19999:19999
```

注意:第一个master Pod `i = 0`。当运行多个masters时，转发每个主机的端口。仅首席maste为Web UI提供服务。

###验证

一旦准备就绪，就可以从master Pod访问Alluxio CLI并运行基本的I/O测试。

```console
$ kubectl exec -ti alluxio-master-0 /bin/bash
```
从master Pod，执行以下命令

```console
$ alluxio runTests
```

(可选)如果Alluxio master使用持久卷，则卷的状态应更改为`CLAIMED`，卷声明的状态应为 `BOUNDED`。你可以验证其状态如下

```console
$ kubectl get pv
$ kubectl get pvc
```

## 高级设置

### POSIX API

一旦Alluxio部署到Kubernetes上，客户端应用程序可以通过多种方式连接。对于使用[POSIX API]({{ '/en/api/POSIX-API.html' | relativize_url}})的应用程序，应用程序容器可以通过挂载Alluxio FileSystem方式连接。

为了使用POSIX API，首先部署Alluxio FUSE守护程序。

{% navtabs posix %} 
{% navtab helm %}

通过配置以下属性来部署FUSE守护程序:

```properties
fuse:
  enabled: true
  clientEnabled: true
```

默认情况下，装载路径是`/mnt/alluxio-fuse`。如果想修改FUSE装载路径，请更新以下属性

```properties
fuse:
  enabled: true
  clientEnabled: true
  mountPath: /mnt/alluxio-fuse
```

然后按照以下步骤用helm安装Alluxio[此处]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html#deploy-using-helm' | relativize_url}})。

如果已经通过helm部署了Alluxio，现在想启用FUSE，则可以使用`helm upgrade`来添加FUSE守护程序。

```console
$ helm upgrade alluxio -f config.yaml --set fuse.enabled=true --set fuse.clientEnabled=true alluxio-charts/alluxio
```
{% endnavtab %} 
{% navtab kubectl %}

```console
$ cp alluxio-fuse.yaml.template alluxio-fuse.yaml
$ kubectl create -f alluxio-fuse.yaml
```
注:
运行Alluxio FUSE守护程序容器必须有`SYS_ADMIN`能力和`securityContext.privileged = TRUE`。需要Alluxio访问权限的应用程序容器不需要此特权。
需要基于`ubuntu`而不是`alpine`的Docker镜像来运行FUSE守护程序。
[alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}-fuse](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}-fuse/)应用程序容器可以在任何Docker镜像上运行。

验证一个容器可以不需要任何定制二进制代码或能力使用`hostPath`挂载到路径`/alluxio-fuse`的方式简单地挂载Alluxio FileSystem:

```console
$ cp alluxio-fuse-client.yaml.template alluxio-fuse-client.yaml
$ kubectl create -f alluxio-fuse-client.yaml
```
如果使用模板，Alluxio会挂载到`/alluxio-fuse`，可以通过POSIX的API进行跨多个容器访问。

{% endnavtab %}
{% endnavtabs %}

### 短路访问

短路访问使客户端可以绕过网络接口直接对worker执行读写操作。对于性能至关重要的应用程序，建议对Alluxio启用短路操作，因为当与Alluxio worker共置时，可以增加客户端的读写吞吐量。

***启用短路操作的属性。*** 
此特性默认启用的，但在Kubernetes环境下需要额外的配置才能在正常工作。有关如何配置短路访问参见以下各节。

***禁用短路操作。*** 
要禁用短路操作，该操作取决于你是如何部署的Alluxio。

>注意:如前所述，禁用对Alluxio workers短路访问会导致更低I/O吞吐量

{% navtabs shortCircuit %}
{% navtab helm %}


你可以通过设置以下属性来禁用短路:

```properties
shortCircuit:
  enabled: false
```

{% endnavtab %}
{% navtab kubectl %}

您应该在`ALLUXIO_WORKER_JAVA_OPTS`把属性`alluxio.user.short.circuit.enabled`设置为`FALSE`。

```properties
-Dalluxio.user.short.circuit.enabled=false
```

你还应该从Pod定义的卷中的`alluxio-domain`卷和每个容器中的`volumeMounts`，如果存在的话

{% endnavtab %} 
{% endnavtabs %}

***短路模式。***
使用短路访问有两种模式

A.`local` 
在这种模式下，如果客户端主机名与worker主机名匹配，Alluxio客户端和本地Alluxio worker就能互识。
这称为*主机名自省*。
在这种模式下，Alluxio客户端和本地Alluxio worker共享Alluxio worker的分层存储。

{% navtabs modes %}
 {% navtab helm %}

您可以通过如下设置属性来使用`local`政策

```properties
shortCircuit:
  enabled: true
  policy: local
```

{% endnavtab %}
 {% navtab kubectl %}

在你的`alluxio-configmap.yaml`模板中，应将以下属性添加到`ALLUXIO_WORKER_JAVA_OPTS`:

```properties
-Dalluxio.user.short.circuit.enabled=true -Dalluxio.worker.data.server.domain.socket.as.uuid=false
```

同时，你应该删除属性`-Dalluxio.worker.data.server.domain.socket.address`。

{% endnavtab %}
 {% endnavtabs %}

B. `uuid` 这是在Kubernetes中使用短路访问的默认策略

如果客户端或工作容器在使用虚拟网络，则其主机名可能不匹配。在这种情况下，请将以下属性设置为使用文件系统检查来启用短路操作，**并确保客户端容器按域套接字路径挂载目录**。
如果worker UUID位于客户端文件系统上，则启用短路写操作。

***域套接字路径。***
域套接字是一个应挂载在以下上的卷:

- 所有Alluxio workers
- 打算通过Alluxio进行读写的所有应用容器。

该域套接字卷可以是`PersistentVolumeClaim`或一个`hostPath Volume`。

***使用PersistentVolumeClaim。*** 
默认情况下，此域套接字卷为`PersistentVolumeClaim`。
你需要为此`PersistentVolumeClaim`发放一个`PersistentVolume`。这个`PersistentVolume`应该是`local`或`hostPath`。

{% navtabs domainSocketPVC %}
 {% navtab helm %}

你可以通过如下属性设置来使用`uuid`策略:

```properties
# These are the default configurations
shortCircuit:
  enabled: true
  policy: uuid
  size: 1Mi
  # volumeType controls the type of shortCircuit volume.
  # It can be "persistentVolumeClaim" or "hostPath"
  volumeType: persistentVolumeClaim
  # Attributes to use if the domain socket volume is PVC
  pvcName: alluxio-worker-domain-socket
  accessModes:
    - ReadWriteOnce
  storageClass: standard
```

`shortCircuit.pvcName`字段定义域套接字的`PersistentVolumeClaim`名称。该PVC将作为`helm install`的一部分被创建。

{% endnavtab %}
{% navtab kubectl %}

您应该在`ALLUXIO_WORKER_JAVA_OPTS`中验证以下属性。
实际它们被默认设置为这些值:
```properties
-Dalluxio.worker.data.server.domain.socket.address=/opt/domain -Dalluxio.worker.data.server.domain.socket.as.uuid=true
```
 
你还应该确保worker Pods在`volumes`中有定义域套接字，以及所有相关容器域套接字卷已挂载。该域套接字默认定义如下:

```properties
volumes:
  - name: alluxio-domain
    persistentVolumeClaim:
      claimName: "alluxio-worker-domain-socket"
```

>注:计算应用程序容器**必须**将域套接字卷挂载到为Alluxio workers配置的相同路径
(`/opt/domain`)。

`PersistenceVolumeClaim`定义于`worker/alluxio-worker-pvc.yaml.template`模板。

{% endnavtab %}
{% endnavtabs %}

***使用hostPath卷。***
 您也可以直接定义workers将`hostPath Volume`用于域套接字。

{% navtabs domainSocketHostPath %}
{% navtab helm %}

您可以切换为直接将`hostPath`卷用于域套接字。这可以通过将`shortCircuit.volumeType`字段更改为`hostPath`来完成的。注意，你还需要定义`hostPath`卷用的路径。

```properties
shortCircuit:
  enabled: true
  policy: uuid
  size: 1Mi
  # volumeType controls the type of shortCircuit volume.
  # It can be "persistentVolumeClaim" or "hostPath"
  volumeType: hostPath
  # Attributes to use if the domain socket volume is hostPath
  hostPath: "/tmp/alluxio-domain" # The hostPath directory to use
```

{% endnavtab %}
{% navtab kubectl %}

你应该以与使用`PersistentVolumeClaim`相同的方式来验证`ALLUXIO_WORKER_JAVA_OPTS`中的属性。

另外，您还应确保worker Pod在`volumes`中定义了域套接字，并且所有相关的容器都已挂载了域套接字卷。域套接字默认定义如下:

```properties
volumes:
  - name: alluxio-domain
    hostPath:
      path: /tmp/alluxio-domain
      type: DirectoryOrCreate
```


注意:计算应用容器必须将域套接字卷挂载到为Alluxio workers配置的相同路径(`/opt/domain`)。

{% endnavtab %}
{% endnavtabs %}

***验证。*** 要验证短路读取和写入，请监控以下显示的指标
1. Web UI的指标`Domain Socket Alluxio Read` 和 `Domain Socket Alluxio`Write
1.或[metrics json]({{ '/en/operation/Metrics-System.html' | relativize_url}}) as `cluster.BytesReadDomain` 和 `cluster.BytesWrittenDomain`
1.或the [fsadmin metrics CLI]({{ '/en/operation/Admin-CLI.html' | relativize_url}}) as `Short-circuit Read (Domain Socket)` 和` Alluxio Write (Domain Socket)`

## 故障排除

{% accordion worker_host %} 
   {% collapsible Worker Host Unreachable %} 
Alluxio worker使用以物理主机IP作为主机名的主机网络。检查集群防火墙是否遇到诸如以下错误:
```
Caused by: io.netty.channel.AbstractChannel$AnnotatedConnectException: finishConnect(..) failed: Host is unreachable: <host>/<IP>:29999
```

-检查`<host>`与物理主机地址匹配，而不是一个虚拟容器主机名。从远程客户端ping以检查地址是否可解析。

```console
$ ping <host>
```

-验证客户端可以按worker部署规范指定的端口上连接到workers。默认端口为`[29998，29999，29996，30001，30002，30003]`。使用网络工具如`ncat`来检查是否可以从远程客户端访问特定的端口:

```console
$ nc -zv <IP> 29999
```

{% endcollapsible %}

{% collapsible Permission Denied %} 
从alluxio V2.1及更高版本，默认alluxio Docker容器除了Fuse以外将以非root 具有UID 1000和GID 1000 的用户`alluxio`身份运行。
Kubernetes [`hostPath`](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath)卷
只能由root写入，因此你需要相应地更新权限。 
  {% endcollapsible %}

{% collapsible Enable Debug Logging %} 
要更改Alluxio服务器(master and workers)的日志级别，使用CLI命令`logLevel`，如下所示:

从master Pod访问Alluxio CLI。

```console
$ kubectl exec -ti alluxio-master-0 /bin/bash
```

从master Pod，执行以下命令:

```console
$ alluxio logLevel --level DEBUG --logName alluxio
```

{% endcollapsible %}

{% collapsible Accessing Logs %} 
Alluxio master 和 job master作为 master Pod的单独容器分别独立运行。同样，Alluxio worker和job worker作为worker Pod的单独容器分别独立运行。可以按以下方式访问各个容器日志。

Master:

```console
$ kubectl logs -f alluxio-master-0 -c alluxio-master
```

Worker:

```console
$ kubectl logs -f alluxio-worker-<id> -c alluxio-worker
```

Job Master:

```console
$ kubectl logs -f alluxio-master-0 -c alluxio-job-master
```

Job Worker:

```console
$ kubectl logs -f alluxio-worker-<id> -c alluxio-job-worker
```

{% endcollapsible %}

{% collapsible POSIX API %} 
为了让一个应用程序容器挂载`hostPath`卷，运行容器的节点
必须有Alluxio FUSE守护程序已在运行。默认规范`alluxio-fuse.yaml`作为DaemonSet运行
，在集群的每个节点上启动Alluxio FUSE守护程序。

如果在使用POSIX API访问Alluxio时遇到问题:
1.使用命令`kubectl describe pods`或仪表板确定应用容器运行的节点。
1.使用命令`kubectl describe nodes <node>`来识别在节点上运行的`alluxio-fuse`Pod。
1.尾部记录已识别Pod的日志以查看遇到的任何错误:
`kubectl logs -f alluxio-fuse- <id>`。
  {% endcollapsible %}
{% endaccordion %}
