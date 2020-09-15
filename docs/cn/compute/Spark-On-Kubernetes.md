---
layout: global
title: Kubernetes环境下在Alluxio上运行Spark
nickname: Spark on Kubernetes
group: Compute Integrations
priority: 7
---

Alluxio可以在Kubernetes上运行。本指南演示了如何在Kubernetes环境下运行的Alluxio上跑一个Spark作业。

* Table of Contents
{:toc}

## 概述

在Kubernetes上运行的Spark可以将Alluxio用作数据访问层。
本指南介绍了Kubernetes环境下在Alluxio上运行Spark作业示例。
本教程中使用的示例是一个计算一个文件中有多少行的作业。
在下文中，我们将此作业称为 count。

## 先决条件

- Kubernetes集群(版本>=1.8)。
- Alluxio已部署在Kubernetes集群上。有关如何部署Alluxio的说明，请参考
[本页]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url}})

## 基本设置

首先，我们准备一个Spark Docker镜像，其中包括Alluxio客户端和任何其他必需的jar文件。
在所有Kubernetes节点上都需提供此镜像。

### 下载二进制文件

[下载](https://spark.apache.org/downloads.html)所需的Spark版本。
对于`spark-submit`命令和使用Alluxio所含的Dockerfile编译Docker镜像
我们都使用预生成的二进制文件，。
>注:下载为Hadoop预制的软件包

```console
$ tar -xf spark-2.4.4-bin-hadoop2.7.tgz
$ cd spark-2.4.4-bin-hadoop2.7
```

### 编译Spark Docker镜像

从Alluxio Docker镜像中提取Alluxio客户端jar:

```console
$ id=$(docker create alluxio/alluxio:{{site.ALLUXIO_VERSION_STRING}})
$ docker cp $id:/opt/alluxio/client/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar \
  - > alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar
$ docker rm -v $id 1>/dev/null
```

添加所需的Alluxio客户端jar并构建用于Spark驱动程序和执行程序pods的Docker镜像。
从Spark发行版目录运行以下命令以添加Alluxio客户端jar。

```console
$ cp <path_to_alluxio_client>/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar jars/
```
>注意:任何复制到`jars`目录的jar文件在编译时都会被包含到Spark Docker镜像中。

编译Spark Docker镜像

```console
$ docker build -t spark-alluxio -f kubernetes/dockerfiles/spark/Dockerfile .
```
>注意:确保所有节点(spark-driver和spark-executor pods将运行的所在节点) 
都有该镜像。

## 示例

本节说明如何使用编译的Docker镜像来发起一个以Alluxio作为数据源的Spark作业。

### 短路操作

短路访问使Spark执行器中的Alluxio客户端可以直接访问主机上的Alluxio worker存储。
因为不通过网络堆栈来与Alluxio worker通信，这样可以提高性能。

如果在部署Alluxio时未按照指令设置domain socket
[本页]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url}}＃short-circuit-access)，则
可以跳过将`hostPath`卷挂载到Spark执行器步骤。

如果在运行Alluxio worker进程的主机上将domain socket位置设置为
`/tmp/alluxio-domain`，并且Alluxio配置为`alluxio.worker.data.server.domain.socket.address=/opt/domain`，使用以下Spark
配置将`/tmp/alluxio-domain`挂载到Spark执行器pod中的`/opt/domain`。
下一节中的`spark-submit`命令包含这些属性。

取决于你的设置，Alluxio worker上的domain socket可以是`hostPath`卷或`PersistententVolumeClaim`两种之一。可以再[此处]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html#short-circuit-access' | relativize_url}})找到有关如何配置Alluxio worker以使用短路操作的更多详细信息。
这两个选项的spark-submit参数将有所不同。
可以在以下Spark文档中找到有关如何将卷挂载到Spark执行器的更多[信息](https://spark.apache.org/docs/2.4.4/running-on-kubernetes.html#using-kubernetes-volumes)。

{% navtabs domainSocket %}
  {% navtab hostPath %}
  如果使用的是`hostPath` domain socket，则应将以下属性传递给Spark:
  
  ```properties
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.path=/opt/domain
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.readOnly=true
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.path=/tmp/alluxio-domain
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.type=Directory
  ```
 
  {% endnavtab %}
  {% navtab PersistententVolumeClaim %}
  如果使用的是`PersistententVolumeClaim` domain socket，则应将以下属性传递给Spark:
  
  ```properties
  spark.kubernetes.executor.volumes.persistentVolumeClaim.alluxio-domain.mount.path=/opt/domain \
  spark.kubernetes.executor.volumes.persistentVolumeClaim.alluxio-domain.mount.readOnly=true \
  spark.kubernetes.executor.volumes.persistentVolumeClaim.alluxio-domain.options.claimName=<domainSocketPVC name>
  ```
  
  {% endnavtab %}
{% endnavtabs %}

注意: 
- Spark中的卷支持是在2.4.0版中添加的。
- 当不通过domain socket使用短路访问时，可能会观察到性能下降。

### 运行Spark作业

以下命令在Alluxio位置`/LICENSE`运行一个计字数作业样例。
可以在Spark驱动程序pod的日志中看到运行的输出和所花费的时间。更进一步[说明参考Spark](https://spark.apache.org/docs/latest/running-on-kubernetes.html)。

#### 创建服务帐户(可选)

如果没有可使用的服务帐户，可以按如下指令创建一个具有所需访问权限的服务账户来运行spark作业。

```console
$ kubectl create serviceaccount spark
$ kubectl create clusterrolebinding spark-role --clusterrole=edit \
  --serviceaccount=default:spark --namespace=default
```

#### 提交Spark作业

从Spark发行版目录运行Spark作业

```console
$ ./bin/spark-submit --master k8s://https://<kubernetes-api-server>:8443 \
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
> 注意:可以通过运行`kubectl cluster-info`找到Kubernetes API服务器地址。
您可以在Spark[文档](https://spark.apache.org/docs/latest/running-on-kubernetes.html?q=cluster-info#cluster-mode)中找到更多详细信息。
你应该使用与你的domain socket卷类型相应的属性 
[domain socket卷类型]({{ '/en/compute/Spark-On-Kubernetes.html#short-circuit-operations' | relativize_url}}。

## 故障排除

### 访问Alluxio客户端日志

可在Spark驱动和执行器日志中找到Alluxio客户端日志。
有关更多说明参考[Spark文档](https://spark.apache.org/docs/latest/running-on-kubernetes.html#debugging)

### Kubernetes客户端上的HTTP 403

如果你的Spark作业因Kubernetes客户端中如下错误而失败:
```
WARN ExecutorPodsWatchSnapshotSource: Kubernetes client has been closed
...
ERROR SparkContext: Error initializing SparkContext.
io.fabric8.kubernetes.client.KubernetesClientException
```

这可能是由于一个[已知问题](https://issues.apache.org/jira/browse/SPARK-28921)导致的，可以通过将`kubernetes- client.jar`升级至4.4.x来解决。
您可以在编译`spark-alluxio`镜像之前通过更新`kubernetes-client-xxjar`来修补docker镜像。

```console
rm spark-2.4.4-bin-hadoop2.7/jars/kubernetes-client-*.jar
wget https://repo1.maven.org/maven2/io/fabric8/kubernetes-client/4.4.2/kubernetes-client-4.4.2.jar 
cp kubernetes-client-4.4.2.jar spark-2.4.4-bin-hadoop2.7/jars
```
然后编译`spark-alluxio`镜像，并分发到所有节点。

### 服务帐户没有访问权限

如果你看到类似以下某些操作被禁止的错误，这是因为用于Spark作业服务帐户没有足够的访问权限来执行操作引起的。

```
ERROR Utils: Uncaught exception in thread main
io.fabric8.kubernetes.client.KubernetesClientException: Failure executing: DELETE at: \
https://kubernetes.default.svc/api/v1/namespaces/default/pods/spark-alluxiolatest-exec-1. \
Message: Forbidden!Configured service account doesn't have access. Service account may have been revoked. \
pods "spark-alluxiolatest-exec-1" is forbidden: User "system:serviceaccount:default:default" \
cannot delete resource "pods" in API group "" in the namespace "default".
```

你应该参考[创建服务帐户]({{ '/en/compute/Spark-On-Kubernetes.html#create-the-service-account-optional' | relativize_url}}确保有正确访问权限。
