---
布局： 全局
title： 在 Docker 上安装 Alluxio
---


Docker 可用于简化 Alluxio 服务器的部署和管理。
使用 [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}/){:target="_blank"} Docker镜像，
只需几个`docker run` 命令步骤，您就可以从零开始创建运行中的 Alluxio 集群。您就可以从
本文档提供了在单节点上运行 Docker 化 Alluxio 的教程。并将本地磁盘作为底层存储的教程。
我们还将讨论更多高级主题以及如何排除故障。


* 目录
{:toc}


## 前提条件


- 安装了 Docker 的机器。
- 可用端口：19998、29999 和 30000


如果无法访问已安装 Docker 的机器，可以
配置一个 AWS EC2 实例（如 t2.large）
来学习本教程。在配置实例时，设置安全组
使以下端口对您的 IP 地址和
Alluxio 客户端（如远程 Spark 集群）的 CIDR 范围开放：


Alluxio 服务器和客户端的 CIDR 范围为 + 19998： 允许客户端和 Worker
与 Alluxio 主 RPC 进程通信。
Alluxio 和客户端的 CIDR 范围：+ 29999： 允许客户端与
与 Alluxio Worker RPC 进程通信。
浏览器 IP 地址 + 30000： 允许您访问通过此端口公开的 Alluxio Worker 指标。


要在配置实例（将称为 Docker Host）后设置 Docker，请运行


```shell
$ sudo yum install -y docker
# 创建 docker 组
$ sudo groupadd docker
# 将当前用户加入 docker 组
$ sudo usermod -a -G docker $(id -u -n)
# 启动 docker 服务
$ sudo service docker start
# 退出并重新登录，以接收组的更改
$ exit
```


## 准备用于持久化数据的 Docker 卷


默认情况下，容器内创建的所有文件都存储在可写的容器层上。
当容器不存在时，数据不会持久化。[Docker Volume](https://docs.docker.com/storage/volumes/){:target="_blank"}
是在容器外保存数据的首选方式。以下两种类型的 Docker 卷使用最多：


+ **主机卷**： 您可以管理 Docker 主机文件系统中存储和共享容器数据的位置。
容器的数据。要创建主机卷，请在启动容器时包含以下内容：


  ```shell
  $ docker run -v /path/on/host:/path/in/container ...
  ```
  文件或目录以其在 Docker 主机上的完整路径作为引用。它可以已经存在于 Docker 主机上，如果还不存在，则会自动创建。


+ **命名卷**： Docker 管理它们所在的位置。应使用特定名称来称呼它。
要创建命名卷，首先运行


  ```shell
  $ docker volume create volumeName
  ```
  然后在启动容器时加入以下内容：
  ```shell
  $ docker run -v volumeName:/path/in/container ...
  ```


主机卷或命名卷都可以用于 Alluxio 容器。为了测试的目的，建议使用主机卷、
推荐使用主机卷，因为它是最容易使用的卷类型，而且性能非常高。
而且性能很好。更重要的是，您可以知道在主机文件系统中的哪个位置引用数据，还可以操作主机卷和命名卷。
文件系统中的数据，而且可以在容器外直接轻松地操作文件。


例如，我们将使用主机卷，并将主机目录 `/tmp/alluxio_ufs` 挂载到
容器位置 `/opt/alluxio/underFSStorage`，这是
这是 Alluxio docker 映像中 Alluxio UFS 根挂载点的默认设置：


```shell
$ mkdir -p /tmp/alluxio_ufs
```


当然，你也可以选择挂载不同的路径，而不是 `/tmp/alluxio_ufs`。
从 2.1 版开始，Alluxio Docker 映像默认以用户 `alluxio` 的身份运行。
它的 UID 是 1000，GID 是 1000。
请确保 Docker 镜像以用户身份运行时，主机卷是可写的。


## 启动主程序和工作程序的 Alluxio 容器


Alluxio 客户端（本地或远程）需要与
通信。因此，必须确保客户端可以同时访问
以下两个服务：


+ 主节点的RPC端口 19998
+ 工作节点的RPC端口 29999


在 Alluxio 集群内，还请确保主容器和工作容器可以通过在
一般要求]（{{ '/en/deploy/Software-Requirements.html#general-requirements' | relativize_url }}）中定义的端口相互连接。

我们将在同一台 Docker 主机上启动 Alluxio 主容器和工作容器。为了确保本地或远程客户端都能正常运行，我们必须设置Docker 网络，并正确暴露所需的端口。

在 Docker 主机上启动 Alluxio Docker 容器有两种方法：
+ 选项 1：使用 [host network](https://docs.docker.com/network/host/){:target="_blank"} 或
+ 选项 2： 使用 [用户自定义桥接网络](https://docs.docker.com/network/bridge/){:target="_blank"}。

主机网络共享容器和 Docker 主机之间的 IP 地址和网络命名空间。
用户定义的桥接网络允许连接的容器进行通信、
同时与未连接到该桥接网络的容器隔离。
建议使用主机网络（选项 1）进行测试。

{% navtabs network %}
{% navtab Using Host Network %}

启动 Alluxio Master

```shell
$ docker run -d --rm \
    --net=host \
    --name=alluxio-master \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost \
       -Dalluxio.dora.client.read.location.policy.enabled=true \
       -alluxio.user.short.circuit.enabled=false \
       -alluxio.master.worker.register.lease.enabled=false \
       -alluxio.worker.block.store.type=PAGE \
       -alluxio.worker.page.store.type=LOCAL \
       -alluxio.worker.page.store.sizes=1GB \
       -alluxio.worker.page.store.page.size=1MB \
       -alluxio.worker.page.store.dirs=/mnt/ramdisk \
       -alluxio.dora.client.ufs.root=/opt/alluxio/underFSStorage" \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} master
```
启动 Alluxio Worker

```shell
$ docker run -d --rm \
    --net=host \
    --name=alluxio-worker \
    --shm-size=1G \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost \
       -Dalluxio.dora.client.read.location.policy.enabled=true \
       -alluxio.user.short.circuit.enabled=false \
       -alluxio.master.worker.register.lease.enabled=false \
       -alluxio.worker.block.store.type=PAGE \
       -alluxio.worker.page.store.type=LOCAL \
       -alluxio.worker.page.store.sizes=1GB \
       -alluxio.worker.page.store.page.size=1MB \
       -alluxio.worker.page.store.dirs=/mnt/ramdisk \
       -alluxio.dora.client.ufs.root=/opt/alluxio/underFSStorage" \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} worker
```
注释

  1. 参数 `--net=host ` 告诉 Docker 使用主机网络。
     在这种设置下，容器会直接使用主机的网络适配器。
     所有容器都将拥有与 Docker 主机相同的主机名和 IP 地址、
     主机的所有端口都直接映射到容器。因此，所有需要的容器
     端口 `19998, 29999, 30000` 都可以通过 Docker 主机提供给客户端。
     有关此设置的更多详情，请参阅 [此处](https://docs.docker.com/network/host/){:target="_blank"}。
  1. 参数 `-e ALLUXIO_JAVA_OPTS="-D..."`为 Alluxio 集群设置所需的配置。
  1. 参数 `-v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage` 告诉 Docker 使用主机卷并持久化 Alluxio UFS。
     并将 Alluxio UFS 根数据持久化在主机目录 `/tmp/alluxio_ufs`中。

{% endnavtab %}
{% navtab Using User-Defined Network %}

使用主机网络很简单，但也有缺点。例如

+ 容器内运行的服务有可能与在同一端口上运行的其他容器内的其他服务发生冲突。
+ 容器可以访问主机的整个网络堆栈，带来潜在的安全风险。

更好的方法是使用用户定义的网络，但我们需要显式地暴露所需的端口
以便外部客户端可以访问容器的服务

准备网络
```shell
$ docker network create alluxio_network
```
启动 Alluxio master

```shell
$ docker run -d  --rm \
    -p 19999:19999 \
    -p 19998:19998 \
    --net=alluxio_network \
    --name=alluxio-master \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost \
       -Dalluxio.dora.client.read.location.policy.enabled=true \
       -alluxio.user.short.circuit.enabled=false \
       -alluxio.master.worker.register.lease.enabled=false \
       -alluxio.worker.block.store.type=PAGE \
       -alluxio.worker.page.store.type=LOCAL \
       -alluxio.worker.page.store.sizes=1GB \
       -alluxio.worker.page.store.page.size=1MB \
       -alluxio.worker.page.store.dirs=/mnt/ramdisk \
       -alluxio.dora.client.ufs.root=/opt/alluxio/underFSStorage" \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} master
```

启动 Alluxio  worker

```shell
$ docker run -d --rm \
    -p 29999:29999 \
    -p 30000:30000 \
    --net=alluxio_network \
    --name=alluxio-worker \
    --shm-size=1G \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost \
       -Dalluxio.dora.client.read.location.policy.enabled=true \
       -alluxio.user.short.circuit.enabled=false \
       -alluxio.master.worker.register.lease.enabled=false \
       -alluxio.worker.block.store.type=PAGE \
       -alluxio.worker.page.store.type=LOCAL \
       -alluxio.worker.page.store.sizes=1GB \
       -alluxio.worker.page.store.page.size=1MB \
       -alluxio.worker.page.store.dirs=/mnt/ramdisk \
       -alluxio.dora.client.ufs.root=/opt/alluxio/underFSStorage" \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} worker
```

注释

  1. 参数 `--net=alluxio_network`告诉 Docker 使用用户定义的桥接网络 `alluxio_network` 。
     所有容器都将使用自己的容器 ID 作为主机名，每个容器在网络子网中都有不同的 IP
     地址。
     连接到同一个用户定义的桥接网络的容器会有效地相互暴露所有端口、
     除非定义了防火墙策略。
     有关桥接网络驱动程序的更多详情，请参阅 [此处](https://docs.docker.com/network/bridge/){:target="_blank"}。
  1. 只有指定的端口（"-p "选项）才会被暴露给外部网络，客户端可以在外部网络中运行。
     命令 `-p <host-port>:<container-port>` 将容器端口映射到主机端口。
     因此，必须显式地暴露主容器的两个端口 19999 和 19998 以及worker容器的端口 29999 和 30000。否则，客户端就无法与主站和工作站通信。
  1. 您可以通过容器名称
     (用于主容器，"alluxio-worker "用于 Worker 容器）或
     或 Docker 主机的 IP 地址"$(hostname -i)"。
     如果所有通信都在 docker 网络内进行（例如，没有外部客户端在 docker 网络之外），则使用 Docker 主机的 IP 地址"$(hostname -i)"。否则，必须
     指定主控程序和 Worker 的 docker 主机 IP，以便客户端可以访问（例如，使用 `-Dalluxio.worker.hostname=$(hostname -i)`）。
     这对于主控程序/工作程序与
     docker 网络之外的客户端之间的外部通信所必需的。否则，客户端将无法连接到 Worker，因为它们无法识别 Worker 的主机名。
     它们无法识别 Worker 的容器 Id。这将导致如下错误：
     ```
     Target: 5a1a840d2a98:29999, Error: alluxio.exception.status.UnavailableException: Unable to resolve host 5a1a840d2a98
     ```


{% endnavtab %}
{% endnavtabs %}

## 验证群集
要验证服务是否启动，请检查 `docker ps`。你应该看到类似

```shell
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                      NAMES
1fef7c714d25        alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}     "/entrypoint.sh work…"   39 seconds ago      Up 38 seconds                                  alluxio-worker
27f92f702ac2        alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}     "/entrypoint.sh mast…"   44 seconds ago      Up 43 seconds       0.0.0.0:19999->19999/tcp   alluxio-master
```

如果看不到容器，请在容器 id 上运行 `docker logs` 查看发生了什么。
容器 id 由 `docker run` 命令打印，也可以在 `docker ps -a` 中找到。

访问 `instance-hostname:19999` 查看 Alluxio Web UI。您应该会看到一个 Worker 已连接并提供
1024MB` 的空间。

要运行测试，请进入 Worker 容器

```shell
$ docker exec -it alluxio-worker /bin/bash
```

Run the tests

```shell
$ cd /opt/alluxio
$ ./bin/alluxio runTests
```
测试远程客户端访问，例如从 Spark 集群（python 3）访问

```scala
textFile_alluxio_path = "alluxio://{docker_host-ip}:19998/path_to_the_file"
textFile_RDD = sc.textFile (textFile_alluxio_path)

for line in textFile_RDD.collect():
  print (line)
```

恭喜你，你已经部署了一个基本的 Docker 化 Alluxio 集群！继续阅读，了解如何管理集群并使其为生产做好准备。

## 高级设置

### 使用开发映像启动 Alluxio

Dockerhub 上有单独的开发镜像 `alluxio/alluxio-dev` 供开发使用。与默认的 `alluxio/alluxio` 映像不同，它只包含 Alluxio 服务运行所需的软件包。
不同，"alluxio-dev "镜像安装了更多开发工具，包括 gcc、make、async-profiler 等、 
使用户更容易在容器中部署更多服务和 Alluxio。

要使用开发镜像，只需在容器启动过程中将 `alluxio/alluxio` 替换为 `alluxio/alluxio-dev`。

### 设置服务器配置

配置更改需要停止 Alluxio Docker 映像，然后用新配置重新启动它们。
然后使用新配置重新启动它们。

要设置 Alluxio 配置属性，可将其添加到 Alluxio java 选项环境变量中，方法如下：
```properties
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property.name=value"
```
多个属性应以空格分隔。

如果属性值包含空格，必须使用单引号将其转义。

```properties
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property1=value1 -Dalluxio.property2='value2 with spaces'"
```

启动图像时，Alluxio 环境变量将被复制到 `conf/alluxio-env.sh` 中。如果没有看到某个属性生效，请确保容器内的
中的属性拼写正确。您可以使用下面的命令：

```shell
$ docker exec ${container_id} cat /opt/alluxio/conf/alluxio-env.sh
```
### 启用 POSIX API 访问权限

Alluxio 的 POSIX 访问是通过 FUSE 实现的。
为了在 docker 环境中实现对 Alluxio 的 POSIX 访问，我们将运行一个独立的 Alluxio Fuse 容器

首先确保 Alluxio FUSE 容器中[bind-mount](https://docs.docker.com/storage/bind-mounts/){:target="_blank"}的主机上存在具有正确权限的目录：

```shell
$ mkdir -p /tmp/mnt/alluxio-fuse && sudo chmod -R a+rwx /tmp/mnt/alluxio-fuse
```
原来的 [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}-fuse](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}-fuse/){:target="_blank"} 已被弃用。现在，您可以通过 [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}/){:target="_blank"} 在 Docker 主机上使用 POSIX API 启用对 Alluxio 的访问。Docker 镜像，与用于启动 Alluxio 主站和 Worker 的镜像相同。

例如，以下命令将 alluxio-fuse 容器作为长期运行的客户端运行，通过 Docker 主机上的 POSIX 接口显示 Alluxio 文件系统：
运行 Alluxio FUSE 服务，在主机绑定挂载目录中创建 FUSE 挂载：
```shell
$ docker run -d --rm \
    --net=host \
    --name=alluxio-fuse \
    -v /tmp/mnt:/mnt:rshared \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost" \
    --cap-add SYS_ADMIN \
    --device /dev/fuse \
    --security-opt apparmor:unconfined \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} fuse \
    /opt/alluxio/underFSStorage /mnt/alluxio-fuse \
    -o allow_other -o entry_timeout=3600 -o attr_timeout=3600
```

注释
- `-v /tmp/mnt:/mnt:rshared` 通过容器内的 fuse 将 Alluxio 的默认目录 `/mnt/alluxio-fuse` 绑定到主机上可访问的挂载路径 `/tmp/mnt/alluxio-fuse`。
要将此路径更改为主机文件系统上的 `/foo/bar/alluxio-fuse`，请将 `/tmp/mnt` 替换为 `/foo/bar`。
- `--cap-add SYS_ADMIN` 以 [SYS_ADMIN](http://man7.org/linux/man-pages/man7/capabilities.7.html) 能力启动容器。
能力。
- `--device /dev/fuse` 与容器共享主机设备 `/dev/fuse`。
- 启动 Fuse 容器的命令需要 4 个参数。以上面的命令为例
  - fuse" - 表示启动 AlluxioFuse 进程
  - /opt/alluxio/underFSStorage"--Alluxio 系统的存储空间
  - /mnt/alluxio-fuse" - Alluxio 在容器内的挂载点
  - "-o allow_other -o entry_timeout=3600 -o attr_timeout=3600allow_other -o entry_timeout=3600 -o attr_timeout=3600" - AlluxioFuse 选项

请参见 [Fuse 高级调整]({{ '/en/fuse-sdk/Advanced-Tuning.html' | relativize_url }})
以及[本地缓存调整]({{ '/en/fuse-sdk/Local-Cache-Tuning.html' | relativize_url }})
了解有关 Alluxio Fuse 安装选项和调整的更多详情。

#### 设置 Alluxio 代理服务器
要在 Docker 容器中启动 Alluxio 代理服务器，只需运行以下命令即可：

```shell
$ docker run -d \
    --net=host \
    --name=alluxio-proxy \
    --security-opt apparmor:unconfined \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost" \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} proxy
```
有关 Alluxio 代理服务器的更多配置选项，请参阅 [Properties List]({{ '/en/reference/Properties-List.html' | relativize_url }})。
配置选项。

## 疑难解答

如果 Alluxio 服务器无法启动，请在运行 "docker run "命令时删除"-d"。
这样进程就能在前台启动，控制台输出也能提供一些有用的信息。

如果启动了 Alluxio 服务器，可以通过运行 `docker logs $container_id` 访问它们的日志。
日志通常能很好地说明问题所在。如果日志不足以诊断
问题，您可以在
[用户邮件列表](https://groups.google.com/forum/#!forum/alluxio-users){:target="_blank"}。
或 [github issues](https://github.com/Alluxio/alluxio/issues){:target="_blank"}.

## FAQ

### 可用处理器: 在 docker 容器中返回 0
在 alluxio 主容器中执行 `alluxio ls` 时，出现以下错误。

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
通过添加 `-XX:ActiveProcessorCount=4` 作为 jvm 参数，可修复此错误。


