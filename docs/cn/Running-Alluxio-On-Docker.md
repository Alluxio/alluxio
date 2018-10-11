---
layout: global
title: 在Docker上运行Alluxio
nickname: 在Docker上运行Alluxio
group: Deploying Alluxio
priority: 3
---

* 内容列表
{:toc}

Alluxio可以运行在一个Docker容器中，本指南介绍如何使用Alluxio Github仓库提供的Dockerfile来将Alluxio运行在Docker之中。

# 基础教程

这篇教程主要介绍如何在单节点上使用 Docker 运行 Alluxio。

# 前期准备

一台Linux主机。在该指南中，我们将使用一台全新的运行着Amazon Linux的EC2主机。主机容量不需要太大，这里我们使用t2.small。当为实例设置网络安全时，允许端口19998-19999和29998-30000通信。

# 启动一个独立模式集群

你需要在你的Linux主机上执行以下的所有步骤。

## 安装Docker

```bash
$ sudo yum install -y docker git
$ sudo service docker start
$ # Add the current user to the docker group
$ sudo usermod -a -G docker $(id -u -n)
$ # Log out and log back in again to pick up the group changes
$ exit
```

## 复制Alluxio仓库

```bash
git clone https://github.com/Alluxio/alluxio.git
```

## 构建Alluxio Docker镜像

```bash
cd alluxio/integration/docker
docker build -t alluxio .
```

默认情况下，这会为最新版本的Alluxio构建镜像。若要根据本地的Alluxio压缩包或者另外一个可下载的压缩包构建，可以使用`--build-arg`参数。

### 设置底层存储系统

在主机上创建一个底层存储文件目录
```bash
$ mkdir underStorage
```

### 设置虚拟内存允许快速短路读取

从主机：

```bash
$ sudo mkdir /mnt/ramdisk
$ sudo mount -t ramfs -o size=1G ramfs /mnt/ramdisk
$ sudo chmod a+w /mnt/ramdisk
```

重启Docker，使得Docker可以检测到新的挂载点。

```bash
$ sudo service docker restart
```

## 运行Alluxio master

```bash
docker run -d --net=host alluxio master
```

### 运行Alluxio master

```bash
$ # This gets the public ip of the current EC2 instance
$ export INSTANCE_PUBLIC_IP=$(curl http://169.254.169.254/latest/meta-data/public-ipv4)
$ docker run -d --net=host \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP} \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio master
```
详细说明:
- `-e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP}`: 指定 Alluxio master 的监听 IP 地址.
- `-v $PWD/underStorage:/underStorage`:和Docker容器共享底层存储层文件夹。
- `-e ALLUXIO_UNDERFS_ADDRESS=/underStorage`:通知worker应用 /underStorage为底层文件存储层。


## 运行Alluxio worker

```bash
$ # Launch an Alluxio worker container and save the container ID for later
$ ALLUXIO_WORKER_CONTAINER_ID=$(docker run -d --net=host \
             -v /mnt/ramdisk:/opt/ramdisk \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP} \
             -e ALLUXIO_RAM_FOLDER=/opt/ramdisk \
             -e ALLUXIO_WORKER_MEMORY_SIZE=1GB \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio worker)
```
Details:
- `-v /mnt/ramdisk:/opt/ramdisk`: 和Docker容器共享主机的ramdisk。
- `-v $PWD/underStorage:/underStorage`: 和Docker容器共享底层文件系统的文件夹。
- `-e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP}`: 通知worker如何连接Master。
- `-e ALLUXIO_RAM_FOLDER=/opt/ramdisk`: 通知worker如何定位ramdisk。
- `-e ALLUXIO_WORKER_MEMORY_SIZE=1GB`: 通知worker节点ramdisk可用空间。
- `-e ALLUXIO_UNDERFS_ADDRESS=/underStorage`: 通知worker将/underStorage作为底层文件系统。

### 测试集群

要测试该集群是否安装成功，首先进入worker Docker容器。

```bash
docker exec -it ${ALLUXIO_WORKER_CONTAINER_ID} /bin/sh
```

接着运行Alluxio测试

```bash
cd opt/alluxio
bin/alluxio runTests
```
# 通过具体的Alluxio分布式环境搭建Docker镜像

通过 `--build-arg`，将本地或远程的Alluxio压缩包搭建Alluxio Docker镜像

本地压缩包:
```bash
$ docker build -t alluxio --build-arg ALLUXIO_TARBALL=alluxio-snapshot.tar.gz .
```

远程压缩包:
```bash
$ docker build -t alluxio --build-arg ALLUXIO_TARBALL=http://downloads.alluxio.org/downloads/files/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz .
```

# Alluxio配置属性

要配置一个Alluxio配置项，将所有字母变成大写，并将句点替换为下划线从而将它转换为环境变量。例如，将`alluxio.master.hostname`转换为`ALLUXIO_MASTER_HOSTNAME`。然后，你可以使用`-e PROPERTY=value`在镜像上设置该环境变量。当该镜像启动时，相应的Alluxio配置项会被拷贝到`conf/alluxio-site.properties`。

本教程使用ramdisk 以允许快速短路读取。另一个选择是使用Docker容器附带的tmpfs。 后者更易于设置，同时提高隔离性，但代价是无法从本地客户端获得内存级别的快速短路读取。 本地客户端需要通过网络从Alluxio workers获取数据。

## 使用Docker tmpfs

当未指定`ALLUXIO_RAM_FOLDER`时，worker Docker容器会使用挂载在`/dev/shm`上的tmpfs。若要配置worker的内存大小为`1GB`，可以在启动时指定`--shm-size 1G`，并且配置Alluxio worker内存大小为`1GB`。

```bash
$ docker run -d --net=host --shm-size=1G \
           -v $PWD/underStorage:/underStorage \
           -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP} \
           -e ALLUXIO_WORKER_MEMORY_SIZE=1GB \
           -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
           alluxio worker
```


为了防止客户端尝试短路读取失败，客户端主机名必须与worker主机名不同。 在客户端上，配置“alluxio.user.hostname = dummy”。

# 快速短路操作

在这篇教程中，我们在主机系统和worker容器之间启动了一个共享的ramdisk，客户端可以在ramdisk上直接执行读写操作，不用通过worker进程执行
此方法的局限是难于设置worker容器的内存限额，尽管ramdisk挂载在主机上，从worker容器向ramdisk写将会计入容器的内存限额，为了避免这一缺陷，
你可以共享一份域套接字来代替ramdisk本身。使用域套接字的缺陷是计算开销大，并受限于更低的写操作吞吐量。

## 域套接字

在主机上为域套接字创建一个文件夹。
```bash
$ mkdir /tmp/domain
$ chmod a+w /tmp/domain
```

当通过worker和客户端的docker容器启动它们时，通过`-v /tmp/domain:/opt/domain`共享域套接字文件夹。同时在启动容器时，通过添加`-e ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS=/opt/domain/d`
在worker设置属性值`alluxio.worker.data.server.domain.socket.address`。

```bash
$ # This gets the public ip of the current EC2 instance
$ export INSTANCE_PUBLIC_IP=$(curl http://169.254.169.254/latest/meta-data/public-ipv4)
$ ALLUXIO_WORKER_CONTAINER_ID=$(docker run -d --net=host --shm-size=1G \
             -v /tmp/domain:/opt/domain \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP} \
             -e ALLUXIO_WORKER_MEMORY_SIZE=1GB \
             -e ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS=/opt/domain/d \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio worker)
```

默认情况下，如果Alluxio客户端和工作节点的主机名相匹配，那么它们之间的短路操作是允许的。但如果客户端依靠虚拟网络作为容器的一部分来运行那就不一定允许。
在这种情况下，当你启动工作节点的时候，设置以下属性来使用文件系统校验：`ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS=/opt/domain`和`ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID=true`。
短路写操作就启用了，只要工作节点的UUID位于客户端文件系统上。

# FUSE
为了使用FUSE,你需要在FUSE激活状态下创建一个docker镜像。

```bash
docker build -f Dockerfile.fuse -t alluxio-fuse .
```

运行支持FUSE的docker镜像还需要一对额外的参数。
例如：

```bash
docker run -e ALLUXIO_MASTER_HOSTNAME=alluxio-master --cap-add SYS_ADMIN --device /dev/fuse alluxio-fuse [master|worker|proxy]
```

注意：在docker上运行FUSE需要增加[SYS_ADMIN capability](http://man7.org/linux/man-pages/man7/capabilities.7.html)
到容器中。这消除了容器的独立性，应该谨慎使用。

重要的是，为了使应用访问装有FUSE的Alluxio存储数据，必须在同一个容器中运行Alluxio。你可以很容易地扩展docker镜像来包含运行在Alluxio之上的应用。例如，为了在一个docker容器中用Alluxio运行TensorFlow,只需要修改Dockerfile.fuse并且用

```bash
FROM tensorflow/tensorflow:1.3.0
```

来代替

```bash
FROM ubuntu:16.04
```

然后你可以用和创建支持FUSE的镜像一样的命令来创建镜像并且运行它。https://hub.docker.com/r/alluxio/alluxio-tensorflow/有一个预先创建好的带有TersorFlow的docker镜像。


