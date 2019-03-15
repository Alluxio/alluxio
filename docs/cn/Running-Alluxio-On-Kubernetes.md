---
layout: global
title: 在Kubernetes上运行Alluxio
nickname: Kubernetes上的Alluxio
group: 部署Alluxio
priority: 3
---

Alluxio可以在Kubernetes上运行。这个指南演示了如何使用Alluxio Github库中的说明来在Kubernetes上运行Alluxio。

# 基础教程

本教程将介绍Kubernetes上的基本Alluxio安装。

## 前提条件

- 一个Kubernetes集群 (版本 >= 1.8). Alluxio workers将使用`sizeLimit`参数来限制使用`emptyDir`卷的大小。这是Kubernetes 1.8的一个alpha特性。
请确保该功能已启用。
- 一个Alluxio Docker镜像。请参阅[本页](Running-Alluxio-On-Docker.html)了解如何构建镜像。该镜像必须可以从运行Alluxio进程的所有Kubernetes主机提取。这可以通过将镜像推送到可访问的Docker注册表来实现，或者将镜像单独推送到所有主机。如果使用私人Docker注册表，请参阅Kubernetes [文档](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/)。

## 克隆Alluxio库

```bash
git clone https://github.com/Alluxio/alluxio.git
cd integration/kubernetes
```

可以在`integration/kubernetes`下找到部署Alluxio所需的Kubernetes说明。

## 启用短路操作

短路访问使客户端可以直接对worker的内存执行读取和写入操作，而不必经过worker进程。在所有有资格运行Alluxio worker进程的主机上设置一个域套接字来启用这种操作模式。

在主机上，为共享域套接字创建一个目录。
```bash
mkdir /tmp/domain
chmod a+w /tmp/domain
touch /tmp/domain/d
chmod a+w /tmp/domain/d
```

如果不需要或不能设置短路访问，则可以跳过此步骤。要禁用此功能，请根据下面的配置部分中的说明设置属性 `alluxio.user.short.circuit.enabled=false`。

## 提供持久性卷

Alluxio master可以配置为使用[持久性卷](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)来存储日志。一旦使用，在master进程的重启之间该卷将被持久化。

从模板创建持久性卷。访问模式`ReadWriteMany`用于允许多个Alluxio master节点访问共享卷。
```bash
cp alluxio-journal-volume.yaml.template alluxio-journal-volume.yaml
```

注意：提供的说明是使用`hostPath`卷在单节点部署上进行演示。对于多节点集群，可以选择使用NFS，AWSElasticBlockStore，GCEPersistentDisk或其他可用的持久性卷插件。

创建持久性卷。
```bash
kubectl create -f alluxio-journal-volume.yaml
```

## 配置Alluxio属性
Kubernetes中的Alluxio容器使用环境变量来设置Alluxio属性。有关`conf/alluxio-site.properties`中的Alluxio属性的相应环境变量名称，请参阅[Docker配置](Running-Alluxio-On-Docker.html)。

在一个文件中定义所有的环境变量。复制`integration/kubernetes/conf`中的属性模板，并根据需要修改或添加配置属性。
请注意，在与主机联网运行Alluxio时，分配给Alluxio服务的端口不能事先被占用。
```bash
cp conf/alluxio.properties.template conf/alluxio.properties
```

创建一个ConfigMap。
```bash
kubectl create configmap alluxio-config --from-file=ALLUXIO_CONFIG=conf/alluxio.properties
```

## 部署

从模板准备Alluxio部署。 修改所需的参数，例如Docker映像的位置，以及Pod的CPU和内存要求。
```bash
cp alluxio-master.yaml.template alluxio-master.yaml
cp alluxio-worker.yaml.template alluxio-worker.yaml
```

一旦所有的前提条件和配置已经建立，部署Alluxio。
```bash
kubectl create -f alluxio-master.yaml
kubectl create -f alluxio-worker.yaml
```

验证Alluxio部署的状态。
```bash
kubectl get pods
```

如果为Alluxio master使用持久卷，卷的状态应该变为 `CLAIMED`。
```bash
kubectl get pv alluxio-journal-volume
```

## 验证

准备就绪后，从master pod 访问Alluxio CLI并运行基本的I/O测试。
```bash
kubectl exec -ti alluxio-master-0 /bin/bash
```

从master pod执行以下操作：
```bash
cd /opt/alluxio
./bin/alluxio runTests
```
