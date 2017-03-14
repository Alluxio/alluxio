---
layout: global
title: 在Docker上运行Alluxio
nickname: 在Docker上运行Alluxio
group: Deploying Alluxio
priority: 3
---

Alluxio可以运行在一个Docker容器中，本指南介绍如何使用Alluxio github仓库提供的Dockerfile来将Alluxio运行在Docker之中。

# 前期准备

一台linux主机。在该指南中，我们将使用一台全新的运行着Amazon Linux的EC2主机。主机容量不需要太大，这里我们使用t2.small。

# 启动一个独立模式集群

你需要在你的linux主机上执行以下的所有步骤。

## 安装Docker

```bash
sudo yum install -y docker git
sudo service docker start
sudo usermod -a -G docker $(id -u -n)
```

最后，先登出系统，再登录以应用组设置更改。

## 复制Alluxio仓库

```bash
git clone https://github.com/Alluxio/alluxio.git
```

## 构建Alluxio Docker镜像

```bash
cd alluxio/integration/docker
docker build -t alluxio .
```

默认情况下，这会为最新版本的Alluxio构建镜像。若要为本地的Alluxio压缩包构建，可以使用`--build-arg`参数。

```bash
$ docker build -t alluxio --build-arg ALLUXIO_TARBALL=alluxio-snapshot.tar.gz .
```

### 设置虚拟内存允许快速短路读取

当Alluxio客户端作为Alluxio worker运行在同一台机器上时，设置一块共享的虚拟内存使得快速短路读取以内存速度而不是网络速度读取数据。

从主机：

```bash
$ sudo mkdir /mnt/ramdisk
$ sudo mount -t ramfs -o size=10G ramfs /mnt/ramdisk
$ sudo chmod a+w /mnt/ramdisk
```

挂载虚拟内存后，重启Docker，使得Docker可以检测到新的挂载点。

```bash
$ sudo service docker restart
```

## 运行Alluxio master

```bash
docker run -d --net=host alluxio master
```

## 运行Alluxio worker

我们需要让worker知道master的位置，在启动worker Docker容器时设置`ALLUXIO_MASTER_HOSTNAME`环境变量为你的主机的主机名。为了允许快速短路读取，
通过`-v /mnt/ramdisk:/mnt/ramdisk`来给worker指定给定位置和大小的共享虚拟内存。`-v /mnt/ramdisk:/mnt/ramdisk`命令将主机路径`/mnt/ramdisk`挂载到worker容器路径`/mnt/ramdisk`。这样，Alluxio worker 可以直接从容器外部写入数据。

```bash
$ docker run -d \
           -v /mnt/ramdisk:/mnt/ramdisk \
           -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_HOSTNAME} \
           -e ALLUXIO_RAM_FOLDER=/mnt/ramdisk \
           -e ALLUXIO_WORKER_MEMORY_SIZE=10GB \
           --net=host alluxio worker
```

### 测试集群

要测试该集群是否安装成功，首先进入worker Docker容器。

```bash
docker exec -it ${ALLUXIO_WORKER_CONTAINER_ID} /bin/sh
```

接着运行Alluxio测试

```bash
cd opt/alluxio*
bin/alluxio runTests
```

### 客户端之间共享虚拟内存

带`-v /mnt/ramdisk:/mnt/ramdisk`命令启动的客户端与主机共享虚拟内存。为了使同一台主机上其他容器内的客户端使用当前机器的虚拟内存，这些容器需要带`-v /mnt/ramdisk:/mnt/ramdisk`命令启动。

# 配置

## Alluxio配置属性

要配置一个Alluxio配置项，将所有字母变成大写，并将句点替换为下划线从而将它转换为环境变量。例如，将`alluxio.master.hostname`转换为`ALLUXIO_MASTER_HOSTNAME`。然后，你可以使用`-e PROPERTY=value`在镜像上设置该环境变量。当该镜像启动时，相应的Alluxio配置项会被拷贝到`conf/alluxio-site.properties`。

```bash
docker run -d --net=host -e ALLUXIO_MASTER_HOSTNAME=ec2-203-0-113-25.compute-1.amazonaws.com alluxio worker
```

# 配置worker内存大小

当未指定虚拟内存文件夹时，worker Docker容器会使用挂载在`/dev/shm`上的tmpfs。若要配置worker的内存大小为`50GB`，可以指定`--shm-size 50G`，完整的docker命令如下：

```bash
docker run -d --net=host --shm-size=50GB \
           -e ALLUXIO_MASTER_HOSTNAME=master \
           -e ALLUXIO_WORKER_MEMORY_SIZE=50GB \
           alluxio worker
```

建议：当Alluxio worker 是远程客户端时，快速短路读取不能使用这种设置。
