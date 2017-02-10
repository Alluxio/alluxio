---
layout: global
title: 在Mesos上运行Alluxio
nickname: 在Mesos上运行Alluxio
group: Deploying Alluxio
priority: 4
---

* 内容列表
{:toc}

Alluxio能够通过Mesos进行部署，这样可以让Mesos对Alluxio使用的资源进行管理。具体来说，对于Alluxio master便是JVM进程需要的cpu和内存资源，而对于Alluxio worker来说除了这两者，还有ramdisk所需要的内存。

## Mesos版本

Alluxio兼容Mesos 0.23.0及之后的版本。

## 在Mesos上部署Alluxio

要在Mesos上部署Alluxio，需要让Mesos能获取到Alluxio发布包。有两个方法：

1. 拷贝Alluxio到所有Mesos节点
2. 配置Mesos指向一个Alluxio压缩包

#### 配置属性

参考[配置项设置](Configuration-Settings.html)文档获取如何配置相应属性。

#### 部署已经拷贝在所有Mesos节点上的Alluxio

1. 在所有Mesos节点上安装Alluxio。接下来的步骤应在安装完Alluxio后进行
2. 设置属性`alluxio.integration.mesos.alluxio.jar.url`的值为`LOCAL`
3. 设置属性`alluxio.home`的值为Alluxio在所有Mesos节点上的安装路径
4. 启动Alluxio Mesos框架

{% include Running-Alluxio-on-Mesos/alluxio-mesos.md %}

#### 通过Alluxio压缩包url进行部署

在任何一个已经安装Alluxio的地方进行以下步骤：

1. 设置属性`alluxio.integration.mesos.alluxio.jar.url`指向一个Alluxio压缩包
2. 启动Alluxio Mesos框架

{% include Running-Alluxio-on-Mesos/alluxio-mesos.md %}

注意这个压缩包应该使用`-Pmesos`选项进行编译。1.3.0及以上的已发布的Alluxio压缩包是采用这种方式编译的。

#### Java

默认情况下，Alluxio Mesos框架会下载Java 7 jdk以运行Alluxio，如果你希望使用Mesos executor上已有的Java，设置以下属性：

```
alluxio.integration.mesos.jdk.url=LOCAL
```

#### 配置Alluxio Masters和Workers

当Alluxio在Mesos上部署后，它会将所有Alluxio配置传播到已经启动的masters和workers上，这也就是说你可以通过更改`conf/alluxio-site.properties`里的配置项来配置已经启动的Alluxio集群。

#### 日志文件

`./integration/mesos/bin/alluxio-mesos-start.sh`脚本会启动一个名为`AlluxioFramework`的Java进程，其日志记录在`alluxio/logs/framework.out`。
在Mesos上启动的Alluxio masters和workers会将其日志记录在`mesos_container/logs/`，另外在`mesos_container/stderr`文件里或许也会有些有用的信息。
