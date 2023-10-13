---
layout: global
title: 在Kubernetes上通过Alluxio运行Spark
nickname: Spark on Kubernetes
group: Compute Integrations
priority: 1
---

Alluxio可以在Kubernetes上运行。本指南介绍了如何在Kubernetes环境中利用Alluxio运行Spark作业。

* Table of Contents
{:toc}

## 概览
在Kubernetes上运行Spark时可将Alluxio作为数据访问层。本指南介绍了在Kubernetes中的Alluxio上运行Spark作业的示例。教程中使用的示例是一个计算文件行数的作业。下文中称该作业为`count`。

## 部署条件

- 已安装一个Kubernetes集群（版本不低于1.8）
- Alluxio部署在Kubernetes集群上。有关如何部署Alluxio，请参见[此页]({{ '/cn/kubernetes/Running-Alluxio-On-Kubernetes.html' | relativize_url }})。

## 基本设置

首先，我们准备一个包含Alluxio client和其他所需jar包的Spark Docker镜像。此镜像应在所有Kubernetes节点上可用。

### 下载Spark软件
[下载](https://spark.apache.org/downloads.html)所需的Spark版本。我们将预编译的二进制文件用于`spark-submit` 命令，并使用Alluxio中包含的Dockerfile来构建Docker镜像。

> 注：下载用于Hadoop的预编译文件包

```console
$ tar -xf spark-2.4.4-bin-hadoop2.7.tgz
$ cd spark-2.4.4-bin-hadoop2.7
```

### 构建Spark Docker镜像

解压Alluxio Docker镜像中的Alluxio client：

```console
$ id=$(docker create alluxio/alluxio:{{site.ALLUXIO_VERSION_STRING}})
$ docker cp $id:/opt/alluxio/client/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar \
  - > alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar
$ docker rm -v $id 1>/dev/null
```

添加所需的Alluxio client jar并构建用于Spark driver和executor pod的Docker镜像。从Spark发行版目录运行以下命令，从而添加Alluxio client jar。

```console
$ cp <path_to_alluxio_client>/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar jars/
```
> 注：任何拷贝到jars目录的jar 文件在构建时都包含在Spark Docker镜像中。

构建Spark Docker镜像

```console
$ docker build -t spark-alluxio -f kubernetes/dockerfiles/spark/Dockerfile .
```
> 注：**确保所有（运行spark-driver和spark-executor pod的）节点都包含此镜像。**

## 示例

本节介绍如何使用构建的Docker镜像来启动一个以Alluxio为数据源的Spark作业。

### 短路操作

短路访问使得Spark executor中的Alluxio client能够直接访问主机上的Alluxio worker存储，而无需通过网络传输与Alluxio worker通信，因而实现了性能提升。

如果未按照[此页]({{ '/cn/kubernetes/Running-Alluxio-On-Kubernetes.html' | relativize_url }}#enable-short-circuit-access)的说明在部署Alluxio时设置domain socket（域套接字），则可以跳过将`hostPath`卷挂载到Spark executor的操作。

如果在运行Alluxio worker进程的主机上将domain socket位置设置成 `/tmp/alluxio-domain` ，而Alluxio配置为 `alluxio.worker.data.server.domain.socket.address=/opt/domain`，则应使用以下Spark配置将 `/tmp/alluxio-domain` 挂载到Spark executor pod上的 `/opt/domain`。下节中提到的`spark-submit`命令将包括这些属性。

根据设置不同，Alluxio worker上的domain socket可以是`hostPath`卷，也可以是`PersistententVolumeClaim`。有关如何配置Alluxio worker来使用短路读的详细信息，请点击[此处]({{ '/cn/kubernetes/Running-Alluxio-On-Kubernetes.html#short-circuit-access' | relativize_url }})。上述两个选项的spark-submit参数会有所不同。有关如何将卷挂载到 Spark executor的详细信息，请参见Spark[文档](https://spark.apache.org/docs/2.4.4/running-on-kubernetes.html#using-kubernetes-volumes)。

{% navtabs domainSocket %}
  {% navtab hostPath %}

  如果您使用的是`hostPath` domain socket，则应将下述属性传递给Spark：

  ```properties
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.path=/opt/domain
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.readOnly=true
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.path=/tmp/alluxio-domain
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.type=Directory
  ```
  {% endnavtab %}
  {% navtab PersistententVolumeClaim %}

  如果您使用的是`PersistententVolumeClaim`domain socket，则应将下述属性传递给Spark：
  
  ```properties
  spark.kubernetes.executor.volumes.persistentVolumeClaim.alluxio-domain.mount.path=/opt/domain \
  spark.kubernetes.executor.volumes.persistentVolumeClaim.alluxio-domain.mount.readOnly=true \
  spark.kubernetes.executor.volumes.persistentVolumeClaim.alluxio-domain.options.claimName=<domainSocketPVC name>
  ```
  
  {% endnavtab %}
{% endnavtabs %}

> 注: 
> - Spark 2.4.0版本中新增了卷支持。
> - 当不通过domain socket使用短路访问时可能会出现性能下降。

### 运行Spark作业

#### 创建服务账户（可选）

如果您没有服务帐户可用，可创建一个具有所需访问权限的服务帐户来运行spark作业，如下所示：

```console
$ kubectl create serviceaccount spark
$ kubectl create clusterrolebinding spark-role --clusterrole=edit \
  --serviceaccount=default:spark --namespace=default
```

#### 提交Spark作业

下述命令在Alluxio `/LICENSE`位置运行字数统计作业。请确保此文件存在于您的Alluxio集群中，或者将路径更改为已存在的文件。

您可以在Spark driver pod的日志中看到输出和所用时间。有关在Kubernetes上运行Spark的更多详细信息，请参阅Spark[文档](https://spark.apache.org/docs/latest/running-on-kubernetes.html)。比如，点击[此处](https://spark.apache.org/docs/latest/running-on-kubernetes.html?q=cluster-info#cluster-mode)可查看该命令中使用的部分flag的详细信息。

从Spark发行版目录运行Spark作业
```console
$ ./bin/spark-submit --master k8s://https://<kubernetes-api-server>:6443 \
--deploy-mode cluster --name spark-alluxio --conf spark.executor.instances=1 \
--class org.apache.spark.examples.JavaWordCount \
--driver-memory 500m --executor-memory 1g \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image=spark-alluxio \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.path=/opt/domain \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.readOnly=true \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.path=/tmp/alluxio-domain \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.type=Directory \
local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar \
alluxio://<alluxio-master>:19998/LICENSE
```

> 注:
> - 您可通过运行`kubectl cluster-info`找到Kubernetes API服务器的地址和端口。
>   - 默认的 Kubernetes API 服务器端口为 6443，但可能会因集群配置而异
> - 建议将此命令中的 `<alluxio-master>` 主机名设置为Alluxio master的Kubernetes服务名（例如，`alluxio-master-0`）。
> - 如果您使用的是不同版本的 Spark，请确保根据Spark 版本正确设置`spark-examples_2.11-2.4.4.jar`的路径
> - 此外，应注意确保卷属性与[domain socket卷类型]({{ '/cn/kubernetes/Spark-On-Kubernetes.html#short-circuit-operations' | relativize_url }})一致。

## 故障排查

### 访问Alluxio Client日志

Alluxio client日志可以在Spark driver和executor日志中查看。详细说明请参见[Spark文档](https://spark.apache.org/docs/latest/running-on-kubernetes.html#debugging)。


### Kubernetes client上出现HTTP 403错误

如果您的Spark作业由于Kubernetes client故障而运行失败，如下所示：
```
WARN ExecutorPodsWatchSnapshotSource: Kubernetes client has been closed
...
ERROR SparkContext: Error initializing SparkContext.
io.fabric8.kubernetes.client.KubernetesClientException
```
这可能是由一个[已知问题](https://issues.apache.org/jira/browse/SPARK-28921)导致，该问题可以通过将 `kubernetes-client.jar`升级到4.4.x来解决。您可以在构建`spark-alluxio`镜像之前通过更新`kubernetes-client-x.x.jar`来修补docker镜像。

```console
rm spark-2.4.4-bin-hadoop2.7/jars/kubernetes-client-*.jar
wget https://repo1.maven.org/maven2/io/fabric8/kubernetes-client/4.4.2/kubernetes-client-4.4.2.jar 
cp kubernetes-client-4.4.2.jar spark-2.4.4-bin-hadoop2.7/jars
```
然后构建`spark-alluxio`镜像并分发到所有节点。

### 服务帐户没有访问权限

如果您看到某些操作被禁止的错误（如下所示），那是因为用于spark作业的服务帐户没有足够的访问权限来执行该操作。

```
ERROR Utils: Uncaught exception in thread main
io.fabric8.kubernetes.client.KubernetesClientException: Failure executing: DELETE at: \
https://kubernetes.default.svc/api/v1/namespaces/default/pods/spark-alluxiolatest-exec-1. \
Message: Forbidden!Configured service account doesn't have access. Service account may have been revoked. \
pods "spark-alluxiolatest-exec-1" is forbidden: User "system:serviceaccount:default:default" \
cannot delete resource "pods" in API group "" in the namespace "default".
```

您应该通过[创建服务帐户]({{ '/cn/kubernetes/Spark-On-Kubernetes.html#create-the-service-account-optional' | relativize_url }})来确保账户具有合理的访问权限。
