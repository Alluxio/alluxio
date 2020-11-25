---
layout: global
title: 在Docker上运行Alluxio
nickname: 在Docker上运行Alluxio
group: Install Alluxio
priority: 3
---

Docker可用于简化Alluxio服务器的部署和管理。使用 Dockerhub上的[alluxio/alluxio](https://hub.docker.com/r/alluxio/alluxio/)Docker镜像，只要运行几个`docker run`命令就可以从零到运行Alluxio集群。本文档提供了以本地磁盘作为底层存储的单个节点上运行Dockerized Alluxio的教程。我们也还将讨论更高级的主题以及如何解决遇到的问题。

* Table of Contents
{:toc}

## 前提条件

- 一台安装了Docker主机。
- 开放端口19998、19999、29998、29999和30000

如果自己没有安装了Docker的主机，则可以在AWS上创建一个小AWS EC2计算实例(例如t2.small)，以便随本教程一起学习。对EC2的security group做出以下设置，以便以下端口对你的IP地址和Alluxio客户端(例如，远程Spark集群)CIDR范围内地址时开放的:

+ 对于Alluxio服务器和客户端CIDR地址范围内端口19998:允许客户端和workers与Alluxio Master RPC进程进行通信。
+ 你的浏览器的IP地址端口19999:允许您访问Alluxio master Web UI。
+ Alluxio和客户端的CIDR范围内端口29999:允许客户端与Alluxio Worker RPC进程进行通信。
+ 你的浏览器的IP地址端口30000:允许您访问Alluxio worker Web UI。

在发放实例后(后续称为Docker Host)运行以下命令来设置Docker

```console
$ sudo yum install -y docker
$ sudo service docker start
# Add the current user to the docker group
$ sudo usermod -a -G docker $(id -u -n)
# Log out and log back in again to pick up the group changes
$ exit
```

## 准备Docker Volume存储数据

默认情况下容器内创建的所有文件都存在可写的容器layer。当该容器不再存在时，数据不会保留。[Docker volumes](https://docs.docker.com/storage/volumes/)是将数据保存到容器外部的方法。最常使用的以下两种的Docker volumes:

+ **Host Volume**:你来决定和管理在Docker 主机的文件系统中在哪里存储和共享容器数据。创建host volume，运行:

  ```console
  $ docker run -v /path/on/host:/path/in/container ...
  ```
  文件或目录通过Docker主机上的完整路径引用。可以是已经存在于Docker主机上的，如果尚不存在则会自动创建。

+ **Named Volume**:Docker来管理其位置。用特定名称来指代即可。创建命名卷，运行:

  ```console
  $ docker volume create volumeName
  $ docker run -v  volumeName:/path/in/container ...
  ```

Alluxio容器用host volume或named volume都可以。出于测试目的话，建议使用host volume，因为它是最容易使用的volume，并且性能非常好。更重要的是，你知道在主机文件系统中所引用数据的位置，并且可以在容器外部直接和很容易的做针对文件的操作。

因此，我们将使用host volume并将host目录 `/alluxio_ufs`挂载到容器路径 `/opt/alluxio/underFSStorage`，这是Alluxio docker镜像中Alluxio UFS root挂载点的默认设置:
  ```console
  $ docker run -v /alluxio_ufs:/opt/alluxio/underFSStorage   ...
  ```
当然，您可以选择安装到其他路径来代替`/alluxio_ufs`。从2.1版本开始，Alluxio Docker镜像默认以`alluxio`用户运行。UID为1000和GID1000。确保运行Docker镜像的用户可写该文件。

## 启动Alluxio Master和Worker容器

Alluxio客户端(本地或远程)需要与Alluxio master和worker通信。因此，确保客户端可以访问以下两项服务非常重要:

+ Master RPC 端口19998
+ Worker RPC 端口29999

Alluxio集群中，同时要确保master和worker容器可以在[下面定义的端口互访]({{ '/en/deploy/Requirements.html#general-requirements' | relativize_url}})。

我们将在同一台Docker主机上启动Alluxio master容器和worker容器。为了确保此方法适用于本地或远程客户端，必须设置Docker网络准确开放通信所需端口。

有两种方法可以在Docker主机上启动Alluxio Docker容器:
+ A: 使用[主机网络](https://docs.docker.com/network/host/) 或
+ B: 使用[用户自定义桥接网络](https://docs.docker.com/network/bridge/)。

主机网络在容器和Docker主机之间共享ip地址和网络命名空间。用户自定义桥接网络允许在桥接网络内的容器互相通信，同时与未连接到该桥接网络的容器隔离。建议使用主机网络(选项A)进行测试。

### 选项A:启动使用主机网络Docker Alluxio容器

```console
# Launch the Alluxio Master
$ docker run -d --rm \
    --net=host \
    --name=alluxio-master \
    -v /alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=$(hostname -i) \
       -Dalluxio.master.mount.table.root.ufs=/opt/alluxio/underFSStorage" \
    alluxio/alluxio master

#Launch the Alluxio Worker
$ docker run -d --rm \
    --net=host \
    --name=alluxio-worker \
    --shm-size=1G \
    -v /alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.worker.memory.size=1G \
       -Dalluxio.master.hostname=$(hostname -i)" \
    alluxio/alluxio worker
```
注意:

  1. 参数`net=host`告诉Docker使用主机网络。在这种设置下，容器直接使用主机的网络适配器。
所有容器都将具有与Docker主机相同的主机名和IP地址，并且所有主机的端口也都直接映射到容器。因此，所有必需的容器端口 `19999、19998、29999、30000` 都通过Docker主机允许客户端访问。[在此处](https://docs.docker.com/network/host/)可以找到有关此设置的更多详细信息。
  1. 参数`-e ALLUXIO_JAVA_OPTS =“-Dalluxio.worker.memory.size = 1G -Dalluxio.master.hostname = $(hostname -I)”` 分配worker的内存容量并绑定master地址。使用`host`网络驱动程序时，不能通过master容器名来引用`alluxio-master`，否则将报 “No Alluxio worker available” 错误。而是应通过主机IP地址引用它。`$(hostname -i` 要改用Docker主机的名称。
  1. 参数`--shm-size = 1G`将为workers分配 `1G` tmpfs存储Alluxio数据。
  1. 参数`-v /alluxio_ufs:/opt/alluxio/underFSStorage` 指引Docker使用host volume并将Alluxio UFS根数据保留在主机目录 `/alluxio_ufs`中，如上文在Docker volume部分所述。

### 选项B:启动使用用户自定义网络Docker Alluxio容器

使用主机网络很简单，但是有缺点。例如

+ 在容器内运行的服务可能与使用同一端口上运行的其他容器中的其他服务冲突。
+ 容器可以访问主机的整个网络堆栈带来潜在的安全风险。

更好的方法是使用用户自定义网络，但我们需要明确开放所需的端口，以便外部客户端可以访问容器的服务:

```console
# Prepare the network
$ docker network create alluxio_network

# Launch the Alluxio master
$ docker run -d  --rm \
    -p 19999:19999 \
    -p 19998:19998 \
    --net=alluxio_network \
    --name=alluxio-master \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=alluxio-master \
       -Dalluxio.master.mount.table.root.ufs=/opt/alluxio/underFSStorage" \
    -v /alluxio_ufs:/opt/alluxio/underFSStorage \
    alluxio/alluxio master

# Launch the Alluxio worker
$ docker run -d --rm \
    -p 29999:29999 \
    -p 30000:30000 \
    --net=alluxio_network \
    --name=alluxio-worker \
    --shm-size=1G \
    -v /alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.worker.memory.size=1G \
       -Dalluxio.master.hostname=alluxio-master \
       -Dalluxio.worker.hostname=alluxio-worker" \
    alluxio/alluxio worker
```

Notes:
  1. 参数 `net=alluxio_network`告诉Docker使用用户自定义桥接网络`alluxio_network`。所有容器都将使用其自己的容器ID作为其主机名，每个容器在网络subnet中具有不同的IP地址。除非定义了防火墙策略，否则连接到相同用户定义桥接网络的容器可以有效地将所有端口对彼此开放。可以[在此处](https://docs.docker.com/network/bridge/)找到有关桥接网络驱动程序的更多详细信息 。 
  1. 仅指定的端口(`-p`选项)公开给客户端所在外部网络。命令 `-p <host-port>:<container-port>` 会将容器端口映射到主机端口。因此，必须明确开放master容器两个端口19999和19998和worker容器端口29999和30000。否则，客户端将无法与master和worker进行通信。
  1. 如果所有通信都在docker网络内部(例如，没有在docker网络外部客户端)，可以通过容器名称(`alluxio-master` for master容器， `alluxio-worker` for worker容器)或通过Docker主机的IP地址$ {`hostname -i`)主机来引用容器 。否则，就必须指定客户端可以访问的master和worker的docker主机IP(例如, 通过`-Dalluxio.worker.hostname = $(hostname -I`))。这是docker网络外部客户端与master/worker通信所必需的。否则，客户端将无法连接到worker，因为它们无法识别worker的容器ID。将触发以下错误:
     ```
     Target: 5a1a840d2a98:29999, Error: alluxio.exception.status.UnavailableException: Unable to resolve host 5a1a840d2a98
     ```

## 验证集群

要验证服务是否已启动，运行 `docker ps`。应该看到类似如下信息
```console
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                      NAMES
1fef7c714d25        alluxio/alluxio     "/entrypoint.sh work…"   39 seconds ago      Up 38 seconds                                  alluxio-worker
27f92f702ac2        alluxio/alluxio     "/entrypoint.sh mast…"   44 seconds ago      Up 43 seconds       0.0.0.0:19999->19999/tcp   alluxio-master
```

如果你看不到容器，针对容器ids运行`docker logs`查看发生了什么。容器ID可以通过运行`docker run`命令来找到，也可以通过`docker ps -a`命令。

访问`instance-hostname`:19999 查看Alluxio Web UI。应该看到一个worker已连接并提供`1024MB`的空间。

如要运行测试，先进入worker容器执行

```console
$ docker exec -it alluxio-worker /bin/bash
```

然后运行测试

```console
$ cd /opt/alluxio
$ ./bin/alluxio runTests
```

要测试远程客户端访问，例如，从Spark集群 (Python 3)

```scala
textFile_alluxio_path = "alluxio://{docker_host-ip}:19998/path_to_the_file"
textFile_RDD = sc.textFile (textFile_alluxio_path)

for line in textFile_RDD.collect():
  print (line)
```

恭喜，您已经部署了一个基本的Dockerized容器化 Alluxio集群！请继续阅读以了解更多有关如何管理集群并使之投入生产的信息。

## 操作介绍

### 服务器配置

配置更改需要停止Alluxio Docker镜像，然后使用新配置重新启动它们。

要正确设置Alluxio配置属性，请将其添加到Alluxio java选项环境变量中，

```
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property.name=value"
```

多个属性应以空格分隔。

使用单引号来将带空格的字符串考虑为一个值。

```
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property1=value1 -Dalluxio.property2=`value2 with spaces`"
```

镜像启动时Alluxio环境变量将被复制到`conf/alluxio-env.sh`。如果你没有看到某个属性生效，请确保容器`conf/alluxio-env.sh`文件中此特性拼写正确。您可以使用以下命令来检查此文件内容。

```console
$ docker exec ${container_id} cat /opt/alluxio/conf/alluxio-env.sh
```

### 在高可用性模式下运行

单个Alluxio master会导致单点故障。为防止这种情况，生产集群应在[高可用性模式]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url}})下运行多个Alluxio master。

#### 选项A:内部Leader选举

Alluxio默认使用内部Leader选举。

提供master embedded 日志地址和把hostname设置为当前master:

```console
$ docker run -d \
  ...
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.embedded.journal.addresses=master-hostname-1:19200,master-hostname-2:19200,master-hostname-3:19200 -Dalluxio.master.hostname=master-hostname-1" \
  alluxio master
```

为所有worker设置master rpc地址，以便他们可以查询master节点找到leader master。

```console
$ docker run -d \
  ...
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998" \
  alluxio worker
```

你可以在[此处]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html#option1-raft-based-embedded-journal' | relativize_url}})找到更多Embedded Journal配置信息。

#### 选项B:Zookeeper和共享日志存储

要在Zookeeper HA模式下运行，Alluxio需要一个所有master都能访问共享日志目录，通常是NFS或HDFS。

将他们指向共享日志并设置其Zookeeper配置。

```console
$ docker run -d \
  ...:
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.journal.type=UFS -Dalluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/alluxio_journal -Dalluxio.zookeeper.enabled=true -Dalluxio.zookeeper.address=zkhost1:2181,zkhost2:2181,zkhost3:2181" \
  alluxio master
```

为workers设置相同的Zookeeper配置，以便他们可以查询Zookeeper发现当前leader。

```console
$ docker run -d \
  ...
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.zookeeper.enabled=true -Dalluxio.zookeeper.address=zkhost1:2181,zkhost2:2181,zkhost3:2181" \
  alluxio worker
```

您可以在[此处]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html#option2-zookeeper-and-shared-journal-storage' | relativize_url}})找到更多关于ZooKeeper和共享日志配置信息。

### 启用短路读写

如果你的计算应用程序将与Alluxio workers在同一节点上运行，则可以通过启用短路读写来提高性能。这允许应用程序在不通过loopback网络的情况下读写其本地Alluxio worker。而是通过[domain sockets](https://en.wikipedia.org/wiki/Unix_domain_socket)来读写。 

在worker主机上，为共享domain socket创建目录。

```console
$ mkdir /tmp/domain
$ chmod a+w /tmp/domain
```

当启动workers和客户端时，增加以下参数运行docker容器`-v /tmp/domain:/opt/domain`以共享domain socket目录。同时在启动worker容器时通过传递以下设置来配置`domain socket`属性 `alluxio.worker.data.server.domain.socket.address=/opt/domain`和`alluxio.worker.data.server.domain.socket.as.uuid=true`。

```console
$ docker run -d \
  ...
  -v /tmp/domain:/opt/domain \
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.worker.data.server.domain.socket.address=/opt/domain -Dalluxio.worker.data.server.domain.socket.as.uuid=true" \
  alluxio worker
```

### 重新启动Alluxio服务器

重新启动Alluxio master时，请使用 `--no-format` 标志以避免重新格式化日志。日志仅应在第一次运行镜像时进行格式化。格式化日志会删除所有Alluxio元数据，并以全空白状态启动集群。

### 激活POSIX API访问

使用 [alluxio/alluxio-fuse](https://hub.docker.com/r/alluxio/alluxio-fuse/)，您可以激活使用POSIX API对Alluxio的访问。

启动具有[SYS_ADMIN](http://man7.org/linux/man-pages/man7/capabilities.7.html) 权限功能的容器。以下命令在一个需要通过POSIX API访问Alluxio的客户端上启动FUSE daemon程序， 挂载访问路径为`/alluxio-fuse`。

```console
$ docker run -e \
  ... \
  --cap-add SYS_ADMIN \
  --device /dev/fuse \
  alluxio-fuse fuse \
```

## 故障排除

可以通过运行`docker logs $container_id`来访问Alluxio 服务器日志。通常，日志可以很好地指出什么地方出了问题。如果它们不足以诊断您的问题，则可以在[用户邮件列表](https://groups.google.com/forum/#!forum/alluxio-users).上获得帮助 。

## FAQ

### AvailableProcessors: returns 0 in docker container

当在 alluxio master container 里执行 `alluxio fs ls /`，遇到如下报错时，

```bash
bash-4.4$ alluxio fs ls /
Exception in thread "main" java.lang.ExceptionInInitializerError
...
Caused by: java.lang.IllegalArgumentException: availableProcessors: 0 (expected: > 0)
        at io.netty.util.internal.ObjectUtil.checkPositive(ObjectUtil.java:44)
        at io.netty.util.NettyRuntime$AvailableProcessorsHolder.setAvailableProcessors(NettyRuntime.java:44)
        at io.netty.util.NettyRuntime$AvailableProcessorsHolder.availableProcessors(NettyRuntime.java:70)
        at io.netty.util.NettyRuntime.availableProcessors(NettyRuntime.java:98)
        at io.grpc.netty.Utils$DefaultEventLoopGroupResource.<init>(Utils.java:394)
        at io.grpc.netty.Utils.<clinit>(Utils.java:84)
        ... 20 more
```

可以通过增加 JVM 参数 `-XX:ActiveProcessorCount=4` 解决这个问题。
