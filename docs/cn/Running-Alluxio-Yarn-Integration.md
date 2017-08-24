---
layout: global
title: Alluxio与YARN整合运行
nickname: Alluxio与YARN整合
group: Deploying Alluxio
priority: 4
---

* 内容列表
{:toc}

本指南介绍在一个YARN集群中运行Alluxio作为一个应用的过程。自带的在EC2上运行Alluxio + YARN的教程，查阅[this guide](Running-Alluxio-on-EC2.html)

注意：YARN不是非常适合像Alluxio这样的长时间运行的应用，我们推荐[these instructions](Running-Alluxio-Yarn-Standalone.html)来替代作为一个YARN应用来运行Alluxio。

## 前期准备

**一个运行的YARN的集群**

**Alluxio在本地下载完成**

```bash
$ curl http://downloads.alluxio.org/downloads/files/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz | tar xz
```

## 构建与YARN的整合

```bash
$ mvn clean install -Dhadoop.version=<your hadoop version> -Pyarn -Dlicense.skip -DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip
```

确保用你使用的Hadoop版本来替代<your hadoop version>

## 配置
通过一些特定的属性来定制Alluxio master和worker(比如，在各个worker节点上设置分层存储)，查阅[Configuration settings](Configuration-Settings.html)
为了确保你的配置能够被ApplicationMaster和Alluxio master/workers都读到，将`alluxio-site.properties` 放到`/etc/alluxio/alluxio-site.properties`中。

如果YARN不再`HADOOP_HOME`中，设置环境变量`YARN_HOME`为YARN的基础路径。

## 运行Alluxio应用

使用脚本`integration/yarn/bin/alluxio-yarn.sh`来启动Alluxio，这个脚本需要三个参数：

1. 需要启动的Alluxio workers的总数目（必须的）
2. 一个HDFS路径来分发包给Alluxio ApplicationMaster（必须的）
3. 运行Alluxio Master的节点的YARN的名字（可选的，默认为`${ALLUXIO_MASTER_HOSTNAME}`）

例如，启动一个有3个worker节点的Alluxio集群，其中一个HDFS的临时目录为`hdfs://masterhost:9000/tmp/`，并且master节点的主机名为`masterhost`，你可以这样运行：

```bash
$ export HADOOP_HOME=/hadoop
$ /hadoop/bin/hadoop fs -mkdir hdfs://masterhost:9000/tmp
$ /alluxio/integration/yarn/bin/alluxio-yarn.sh 3 hdfs://masterhost:9000/tmp/ masterhost
```

你也可以单独启动Alluxio Master节点，在这种情况下，上述的启动能够在提供的地址上自动检测到Master而且跳过一个新实例的初始化。
如果你希望在一个特定的主机运行Master，这个主机不在你的YARN集群中，比如一个AWS EMR Master实例，这将会非常有用。

这个脚本会在YARN上启动一个Alluxio的Application Master，这个Application Master会为Alluxio master和workers申请容器。你可以
在浏览器中查看 YARN的UI来观察Alluxio作业的状态。

运行这个脚本会产生包含如下内容的输出

```
INFO impl.YarnClientImpl: Submitted application application_1445469376652_0002
```

这个application ID可以通过运行如下指令来销毁application：

```bash
$ /hadoop/bin/yarn application -kill application_1445469376652_0002
```

这个ID也可以在YARN web UI上看到。

## 测试Alluxio

如果你有Alluxio application在运行，你可以通过在`conf/alluxio-site.properties`上配置`alluxio.master.hostname=masterhost`并运行如下指令来查看其健康性

```bash
$ /alluxio/bin/alluxio runTests
```
