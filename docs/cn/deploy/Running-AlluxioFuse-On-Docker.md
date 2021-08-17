---
layout: global
title: 使用Docker部署AlluxioFuse加速深度学习训练（试验）
nickname: 在Docker上运行AlluxioFuse（试验）
group: Install Alluxio
priority: 5
---

* 内容列表
{:toc}

## 概述

本指南介绍读者如何使用基于JNI(Java Native Interface)的实验版的AlluxioFuse来优化机器学习任务的云端IO性能。
该实验版AlluxioFuse同默认Alluxio发行版中的基于JNR实现的AlluxioFuse相比还有一些局限性。
我们正在持续解决兼容性和优化稳定性和速度等问题。欢迎试用并提出建议。
本实验版AlluxioFuse当前仅通过容器镜像交付。

### 什么是FUSE和AlluxioFuse

用户空间文件系统（**F**ilesystem in **Use**rspace， 简称**FUSE**）是一个面向类Unix系统的软件接口，它使得普通用户无需更改内核代码就能创建自己的文件系统[2]。
FUSE包括kernel提供的内核模块和用户态的[libfuse库](https://github.com/libfuse/libfuse)。libfuse库提供了一套标准的文件系统接口。

Alluxio基于这套文件系统接口实现了简单易用的**AlluxioFuse**。AlluxioFuse给予了Alluxio更多易用性和灵活性，使得Alluxio能够扩展到更多场景。
由于libfuse基于C，而Alluxio基于Java，我们可以采用JNI或者JNR等不同方式使Alluxio对接libfuse。
本实验版本是使用JNI来直接对接Alluxio和libfuse，而不是Alluxio发行版默认的JNR。

## 在Docker上单机部署AlluxioFuse

### 前提条件

- 安装[Docker](https://www.docker.com/)
- 下载预生成的Aluxio Docker镜像：镜像信息如下表所示。Alluxio master和Alluxio worker使用下表中第一个镜像，而Alluxio fuse则使用第二个镜像。Alluxio fuse运行环境与Alluxio master等有差异，它还额外依赖于`libfuse`库，所以它们使用不同的镜像。

<table class="table table-striped">
    <tr>
        <td>REPOSITORY</td>
        <td>TAG</td>
    </tr>
    <tr>
        <td>registry.cn-huhehaote.aliyuncs.com/alluxio/alluxio</td>
        <td>2.3.0-SNAPSHOT-b7629dc</td>
    </tr>
    <tr>
        <td>registry.cn-huhehaote.aliyuncs.com/alluxio/alluxio-fuse</td>
        <td>2.3.0-SNAPSHOT-b7629dc</td>
    </tr>
</table>

### 单机部署

#### 设置底层存储系统

在主机上创建文件夹`underStorage`作为Alluxio的底层存储系统

```console
$ mkdir underStorage
```

#### 设置RAMFS

在主机上创建文件夹`/mnt/ramdisk`，并挂载为`ramfs`，注意添加ramfs的读写权限

```console
$ sudo mkdir /mnt/ramdisk
$ sudo mount -t ramfs -o size=1G ramfs /mnt/ramdisk
$ sudo chmod a+w /mnt/ramdisk
```

在这个示例中，`ramfs`的大小设置为了`1G`，也可以根据实验环境设置为其他数值。
此后在Worker的设置中，交给Worker管理的堆外内存大小需要与其一致。

#### 启动Alluxio master

在主机上启动`alluxio-master`

```console
$ docker run -d \
    --name=alluxio-master \
    -u=0 \
    --net=host \
    -v $PWD/underStorage:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.hostname=localhost \
        -Dalluxio.master.mount.table.root.ufs=/opt/alluxio/underFSStorage " \
    registry.cn-huhehaote.aliyuncs.com/alluxio/alluxio:2.3.0-SNAPSHOT-b7629dc master
```

命令参数说明：

- `-u=0`表示以`root`身份运行
- `--net=host`指定容器共享主机的network namespace
- `-v $PWD/underStorage:/opt/alluxio/underFSStoragee`表示宿主机和Docker容器共享底层存储层文件夹
- `-e ALLUXIO_JAVA_OPTS`中设置Alluxio的配置项，其中`alluxio.master.mount.table.root.ufs`设置了底层存储系统文件夹

#### 启动Alluxio worker

在主机上启动`alluxio-worker`

```console
$ docker run -d \
    --name=alluxio-worker \
    -u=0 \
    --net=host \
    --shm-size=1G \
    -v /mnt/ramdisk:/opt/ramdisk \
    -v $PWD/underStorage:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS="-Dalluxio.worker.hostname=localhost \
        -Dalluxio.master.hostname=localhost \
        -Dalluxio.worker.ramdisk.size=1G \
        -Dalluxio.master.mount.table.root.ufs=/opt/alluxio/underFSStorage " \
    -e ALLUXIO_RAM_FOLDER=/opt/ramdisk \
    registry.cn-huhehaote.aliyuncs.com/alluxio/alluxio:2.3.0-SNAPSHOT-b7629dc worker
```

命令参数说明：

- `--shm-size=1G`：设置容器的共享内存大小，与`alluxio.worker.ramdisk.size`（`ALLUXIO_JAVA_OPTS`中设置）和RAMFS的大小（`1G`）保持一致
- `-v /mnt/ramdisk:/opt/ramdisk`：和Docker容器共享主机的ramdisk
- `-e ALLUXIO_RAM_FOLDER=/opt/ramdisk`：通知worker如何定位ramdisk

#### 设置AlluxioFuse挂载路径

在主机上创建文件夹`/mnt/alluxio-fuse`，AlluxioFuse会数据集挂载在这个路径下：

```console
$ mkdir /mnt/alluxio-fuse
$ sudo chmod a+r /mnt/alluxio-fuse
```

#### 启动AlluxioFuse

在宿主机上启动`alluxio-fuse`，注意容器挂载路径是`/mnt`（而不是`/mnt/alluxio-fuse`）。

```console
$ docker run -d \
    --name=alluxio-fuse \
    -u=0 \
    --net=host \
    -v /mnt/ramdisk:/opt/ramdisk \
    -v /mnt:/mnt:rshared \
    --cap-add SYS_ADMIN \
    --device /dev/fuse \
    registry.cn-huhehaote.aliyuncs.com/alluxio/alluxio-fuse:2.3.0-SNAPSHOT-b7629dc fuse \
    --fuse-opts=kernel_cache
```

命令参数说明：

- `-v /mnt:/mnt:rshared`：容器共享宿主机的`/mnt`文件夹，Alluxio中的数据将会挂载到`/mnt/alluxio-fuse`
- `--cap-add SYS_ADMIN`：赋予容器`SYS_ADMIN`权限
- `--device /dev/fuse`：容器共享宿主机的设备`/dev/fuse`
- `--fuse-opts=kernel_cache`：设置FUSE参数，`kernel_cache`表示使用`page cahce`

### 测试Alluxio集群

在`alluxio-master`容器里往Alluxio添加文件

```console
$ docker exec -it alluxio-master bash
$ alluxio fs copyFromLocal LICENSE  /
```

查看`alluxio-fuse`容器的挂载点`/mnt/alluxio-fuse`

```console
$ ls /mnt/alluxio-fuse/
LICENSE
```

可在主机上通过挂载点直接访问Alluxio中的文件，说明Alluxio集群已成功部署。

## AlluxioFuse调优

在FUSE层和Alluxio层都能进一步调优，下面分别介绍。

### FUSE层优化配置项

<table class="table table-striped">
    <tr>
        <td>配置项</td>
        <td>默认值</td>
        <td>调优建议</td>
        <td>描述</td>
    </tr>
    <tr>
        <td>`direct_io`</td>
        <td></td>
        <td>不要设置</td>
        <td>开启`direct_io`模式，内核不会主动缓存和预读；当IO负载压力特别高时，`direct_io`模式下可能存在稳定性问题。</td>
    </tr>
    <tr>
        <td>`kernel_cache`</td>
        <td></td>
        <td>建议启用</td>
        <td>开启`kernel_cache`，使用更多的系统缓存，同时提升文件读取速度。</td>
    </tr>
    <tr>
        <td>`max_read=N`</td>
        <td>131072</td>
        <td>使用默认值</td>
        <td>FUSE单次request能读取文件的大小上限。这个值在kernel被设置为32个pages，在i386上，为131072，或者说128kbytes。</td>
    </tr>
    <tr>
        <td>`attr_timeout=N`</td>
        <td>1.0</td>
        <td>7200</td>
        <td>设置文件属性（struct inode）被缓存的时间（单位：second）。增加inode缓存时间可减少FUSE文件元数据操作，提升性能。</td>
    </tr>
    <tr>
        <td>`entry_timeout=N`</td>
        <td>1.0</td>
        <td>7200</td>
        <td>设置文件项（struct dentry）被缓存的时间（单位：second）。增加entry缓存时间可减少FUSE文件元数据操作，提升性能。</td>
    </tr>
    <tr>
        <td>`max_idle_threads`</td>
        <td>10</td>
        <td>视任务而定</td>
        <td>libfuse用户态daemon线程的最大闲置数量。在高并发下，这个属性过小会导致FUSE频繁创建销毁线程。建议视用户进程的IO活跃度配置。注意：libfuse 2并未提供这个参数，在alluxio-fuse容器镜像中，我们修改了libfuse代码，并通过环境变量的方式修改这个参数。</td>
    </tr>
</table>

### AlluxioFuse相关优化配置项

除了一些通用的Alluxio调优手段外，针对AlluxioFuse，我们增加了额外的配置项进行优化。AlluxioFuse优化主要分为两部分。

#### jnifuse

初始的Alluxio基于[jnr-fuse](https://github.com/SerCeMan/jnr-fuse)实现AlluxioFuse，为解决性能和稳定性问题，我们基于`jni-fuse`实现了`AlluxioJniFuse`，可支持`kernel_cache`模式。通过设置`alluxio.fuse.jnifuse.enabled`在两者间切换，建议设置为`true`。

注意：目前AlluxioJniFuse正处于实验阶段，不支持写入，只支持读取。

<table  class="table table-striped">
    <tr>
        <td>属性名</td>
        <td>默认值</td>
        <td>调优建议</td>
        <td>描述</td>
    </tr>
    <tr>
        <td>`alluxio.fuse.jnifuse.enabled`</td>
        <td>true</td>
        <td>true</td>
        <td>使用jnifuse(true)，否则使用jnrfuse(false)。</td>
    </tr>
</table>

#### client端缓存

在高并发和`kernel_cache`模式下，由于kernel的prefetch机制和FUSE的daemon多线程并发访问机制，可能破坏文件的顺序读特性，导致Alluxio产生频繁的seek gRPC请求，进而大幅降低性能。启用client端缓存可一定程度解决这个问题。

配置项`alluxio.user.client.cache.enabled`决定了是否启用client端缓存，建议仅在IO压力特别大的情况下启用。若启用client端cache，还有其他配套参数可以调整，包括cache的类型、cache的存储目录、页大小和容量上限。在深度学习任务中，比较通用的配置是将cache类型设置为`MEMORY`，页大小设置为`2MB`，容量上限设置为`2000MB`，然后通过观测cache的命中率（通过alluxio-fuse日志查看）和JVM的GC情况来适当调整页大小和cache容量。

<table class="table table-striped">
    <tr>
        <td>属性名</td>
        <td>默认值</td>
        <td>描述</td>
    </tr>
    <tr>
        <td>`alluxio.user.client.cache.enabled`</td>
        <td>false</td>
        <td>是否启用client端cache。因为会产生额外的资源消耗，建议仅在IO压力特别高的场景下启用。</td>
    </tr>
    <tr>
        <td>`alluxio.user.client.cache.store.type`</td>
        <td>LOCAL</td>
        <td>LOCAL设置client端cache的页存储类型，可选：{LOCAL, MEMORY, ROCKS}。LOCAL表示所有页存储在一个目录（通过`alluxio.user.client.cache.dir`指定）；ROCKS使用rocksDB来持久化数据；MEMORY将所有页存储在Java堆内存中。推荐使用MEMORY。</td>
    </tr>
    <tr>
        <td>`alluxio.user.client.cache.dir`</td>
        <td>/tmp/alluxio_cache</td>
        <td>client端cache存储目录（在LOCAL和ROCKS下有效）。一般将这个目录挂载为ramfs，提升cache访问速度。</td>
    </tr>
    <tr>
        <td>`alluxio.user.client.cache.page.size`</td>
        <td>1MB</td>
        <td>client端cache的页大小。在调优时，可优先尝试2MB，4MB和8MB。注意：页大小设置过大可能导致JVM频繁GC；而太小则可能导致cache命中率过低。建议调优时，通过观察GC情况和cache命中率适当调整。</td>
    </tr>
    <tr>
        <td>`alluxio.user.client.cache.size`</td>
        <td>512MB</td>
        <td>client端cache的容量上限。MEMORY模式下，建议设置在1800MB以内。</td>
    </tr>
</table>

## 应用实践：在阿里云上基于kubernetes集群搭建四机八卡深度学习环境

在阿里云平台基于k8s集群搭建了4机八卡深度学习训练环境，优化深度学习训练的IO性能。

### 实验环境说明

#### 硬件环境

- 4台V100（高配GPU机型），一共32块GPU卡

#### 数据集

- 使用ResNet-50模型在ImageNet数据集上测试，ImageNet数据集总大小144GB，数据以TFRecord格式存储，每个TFRecord大小约130MB。每个GPU的batch_size设置为256
- 数据集可从[这里下载](https://imagenet-tgz.oss-cn-hongkong.aliyuncs.com/imagenet_data.tar)

#### 数据存储

- 数据集存储在[Aliyun OSS](https://cn.aliyun.com/product/oss)，Alluxio将OSS作为底层存储系统，模型训练程序通过AlluxioFuse读取数据

### 部署流程

#### 前提条件

- Kubernetes集群（version >= 1.8）
- [helm](https://helm.sh/docs/)（version >= 3.0）
- Alluxio Docker镜像（由阿里云提供）

#### 设置虚拟内存文件系统

若设置RAMFS的挂载路径为`/ram/alluxio`，则需在kubernetes集群的每台机器上，挂载RAMFS：

```console
$ mkdir -p /alluxio/ram
$ mount -t ramfs -o size=260G ramfs /alluxio/ram
```

RAMFS的size具体大小，应视机器配置和训练任务需求而定。

#### 设置底层文件系统

集群使用[Aliyun OSS](https://cn.aliyun.com/product/oss)作为Alluxio的底层文件系统，详细配置过程参见[在OSS上配置Alluxio](https://docs.alluxio.io/os/user/stable/cn/ufs/OSS.html)。

#### 下载helm-chart

使用[helm](https://helm.sh/docs/)部署Alluxio集群，需要先下载helm chart，我们使用0.14.0版，若使用其他更老版本可能存在适配问题：

```console
$ wget http://kubeflow.oss-cn-beijing.aliyuncs.com/alluxio-0.14.0.tgz
```

#### 配置Alluxio集群（基础版）

在配置中，至少应指定Alluxio和Alluxio-fuse的镜像（直接使用阿里云镜像），Alluxio的底层文件系统（阿里云OSS）和Alluxio的RAMFS。如下的`config.yaml`文件是未使用任何参数优化情况下，部署Alluxio集群所需最简配置。

```yaml
# config.yaml
image: registry.cn-huhehaote.aliyuncs.com/alluxio/alluxio
imageTag: "2.3.0-SNAPSHOT-b7629dc"
imagePullPolicy: Always

properties:
    # set OSS endpoint
    fs.oss.endpoint: <OSS_ENDPOINT>
    fs.oss.accessKeyId: <OSS_ACCESS_KEY_ID>
    fs.oss.accessKeySecret: <OSS_ACCESS_KEY_SECRET>
    alluxio.master.mount.table.root.ufs: oss://<OSS_BUCKET>/<OSS_DIRECTORY>/

tieredstore:
  levels:
  - alias: MEM
    level: 0
    type: hostPath
    path: /alluxio/ram

fuse:
  enabled: true
  clientEnabled: true
  mountPath: /mnt/alluxio-fuse
  image: registry.cn-huhehaote.aliyuncs.com/alluxio/alluxio-fuse
  imageTag: "2.3.0-SNAPSHOT-b7629dc"
  imagePullPolicy: Always
  args:
    - fuse
    - --fuse-opts=kernel_cache
```

#### 启动Alluxio集群

helm安装Alluxio集群时，指定前述的集群配置文件`config.yaml`和helm chart压缩包`alluxio-0.14.0.tgz`。

```console
$ helm install alluxio -f config.yaml alluxio-0.14.0.tgz
```

等待Alluxio集群部署一定时间后，查看kubernetes pod：

```console
$ kubectl get po
NAME                                                         READY   STATUS      RESTARTS   AGE
alluxio-fuse-5ttjt                                           1/1     Running     0          7s
alluxio-fuse-6l5pf                                           1/1     Running     0          7s
alluxio-fuse-l84lp                                           1/1     Running     0          7s
alluxio-fuse-m7z28                                           1/1     Running     0          7s
alluxio-master-0                                             2/2     Running     0          7s
alluxio-worker-22m44                                         2/2     Running     0          7s
alluxio-worker-jsqxl                                         2/2     Running     0          7s
alluxio-worker-qp69q                                         2/2     Running     0          7s
alluxio-worker-t6wzj                                         2/2     Running     0          7s
```

在配置为四机八卡的kubernetes集群上，应该有1个alluxio-master、4个alluxio-worker和4个alluxio-fuse在运行。

在集群任意一台机器查看alluxio-fuse挂载路径：

```console
$ ls /mnt/alluxio-fuse/
train  validation
```

可见OSS云端数据集已成功挂载到这台机器。之后，用户便可像操作本地文件一样，在挂载数据集上运行深度学习等任务。

#### 配置Alluxio集群（进阶版）

如果直接使用上述最简配置，alluxio-fuse读取性能难以满足深度学习训练需求。在此实例中，我们基于这个深度学习训练环境，全面优化了Alluxio集群的配置，如下所示：

```yaml
# config.yaml
image: registry.cn-huhehaote.aliyuncs.com/alluxio/alluxio
imageTag: "2.3.0-SNAPSHOT-b7629dc"
imagePullPolicy: Always

properties:
    alluxio.user.ufs.block.read.location.policy: alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy
    alluxio.fuse.jnifuse.enabled: true
    alluxio.user.client.cache.enabled: false
    alluxio.user.client.cache.store.type: MEMORY
    alluxio.user.client.cache.dir: /alluxio/ram
    alluxio.user.client.cache.page.size: 8MB
    alluxio.user.client.cache.size: 1800MB
    alluxio.master.journal.log.size.bytes.max: 500MB
    alluxio.user.update.file.accesstime.disabled: true
    alluxio.user.block.worker.client.pool.min: 512
    # It should be great than (120)*0.01 = 2GB
    alluxio.fuse.debug.enabled: "false"
    alluxio.web.ui.enabled: false
    alluxio.user.file.writetype.default: MUST_CACHE
    alluxio.user.block.write.location.policy.class: alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy
    alluxio.worker.allocator.class: alluxio.worker.block.allocator.GreedyAllocator
    alluxio.user.block.size.bytes.default: 32MB
    # set OSS endpoint
    fs.oss.endpoint: <OSS_ENDPOINT>
    fs.oss.accessKeyId: <OSS_ACCESS_KEY_ID>
    fs.oss.accessKeySecret: <OSS_ACCESS_KEY_SECRET>
    alluxio.master.mount.table.root.ufs: oss://<OSS_BUCKET>/<OSS_DIRECTORY>/
    alluxio.user.streaming.reader.chunk.size.bytes: 32MB
    alluxio.user.local.reader.chunk.size.bytes: 32MB
    alluxio.worker.network.reader.buffer.size: 32MB
    alluxio.worker.file.buffer.size: 320MB
    alluxio.job.worker.threadpool.size: 64
    alluxio.user.metrics.collection.enabled: false
    alluxio.master.rpc.executor.max.pool.size: 10240
    alluxio.master.rpc.executor.core.pool.size: 128
    alluxio.master.mount.table.root.readonly: true
    alluxio.user.update.file.accesstime.disabled: true
    alluxio.user.file.passive.cache.enabled: false
    alluxio.user.block.avoid.eviction.policy.reserved.size.bytes: 2GB
    alluxio.master.journal.folder: /journal
    alluxio.master.journal.type: UFS
    alluxio.user.block.master.client.pool.gc.threshold: 2day
    alluxio.user.file.master.client.threads: 1024
    alluxio.user.block.master.client.threads: 1024
    alluxio.user.file.readtype.default: CACHE
    alluxio.security.stale.channel.purge.interval: 365d
    alluxio.user.metadata.cache.enabled: true
    alluxio.user.metadata.cache.expiration.time: 2day
    alluxio.user.metadata.cache.max.size: "1000000"
    alluxio.user.direct.memory.io.enabled: true
    alluxio.fuse.cached.paths.max: "1000000"
    alluxio.job.worker.threadpool.size: 164
    alluxio.user.worker.list.refresh.interval: 2min
    alluxio.user.logging.threshold: 1000ms
    alluxio.fuse.logging.threshold: 1000ms
    alluxio.worker.block.master.client.pool.size: 1024

worker:
    jvmOptions: " -Xmx12G -XX:+UnlockExperimentalVMOptions -XX:MaxDirectMemorySize=32g  -XX:ActiveProcessorCount=8 "

master:
    jvmOptions: " -Xmx6G -XX:+UnlockExperimentalVMOptions -XX:ActiveProcessorCount=8 "

tieredstore:
  levels:
  - alias: MEM
    level: 0
    type: hostPath
    path: /alluxio/ram
    high: 0.99
    low: 0.8

fuse:
  image: registry.cn-huhehaote.aliyuncs.com/alluxio/alluxio-fuse
  imageTag: "2.3.0-SNAPSHOT-b7629dc"
  imagePullPolicy: Always
  env:
    MAX_IDLE_THREADS: "64"
    SPENT_TIME: "1000"
  # Customize the MaxDirectMemorySize
  jvmOptions: " -Xmx16G -Xms16G -XX:+UseG1GC -XX:MaxDirectMemorySize=32g -XX:+UnlockExperimentalVMOptions -XX:ActiveProcessorCount=24 -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCTimeStamps "
  shortCircuitPolicy: local
  mountPath: /mnt/alluxio-fuse
  args:
    - fuse
    - --fuse-opts=kernel_cache,ro,max_read=131072,attr_timeout=7200,entry_timeout=7200
```

上述优化涵盖了FUSE、JVM和Alluxio，最终为深度学习训练速度带来了巨大的提升。在深度学习训练时间上，使用Alluxio需要65分钟，作为对比的云上SSD则需要110分钟，提升达`40%`。

## 总结与展望

在本文中，我们介绍了Alluxio和FUSE的技术背景，以及AlluxioFuse的应用价值，接着我们介绍了如何通过Docker镜像的方式在单机上快速部署AlluxioFuse，然后从FUSE和Alluxio层分别分析了AlluxioFuse调优方法，最后在阿里云上基于kubernetes集群部署四机八卡深度学习训练环境，提升深度学习训练速度。

为进一步提升AlluxioFuse性能，我们还在持续优化AlluxioFuse，欢迎各位尝试目前提供的阿里云实验版本镜像！

## 参考文献

[1] alluxio: Alluxio概览, https://docs.alluxio.io/os/user/stable/cn/Overview.html.

[2] wikipedia: FUSE, https://zh.wikipedia.org/wiki/FUSE.

[3] 阿里云容器平台：阿里云容器服务团队实践——Alluxio 优化数倍提升云上 Kubernetes 深度学习训练性能, https://www.infoq.cn/article/FLMjB7A7jD2aAWvg6Xjb.
