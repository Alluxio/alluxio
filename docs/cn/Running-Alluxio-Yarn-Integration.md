---
layout: global
title: Alluxio与YARN整合运行
nickname: Alluxio与YARN整合
group: Deploying Alluxio
priority: 4
---

* 内容列表
{:toc}

注意：YARN不是非常适合像Alluxio这样的长时间运行的应用，我们推荐[these instructions](Running-Alluxio-Yarn-Standalone.html)来将Alluxio与YARN一起运行，而不是作为YARN内的一个应用来运行。

## 前期准备

**一个运行的YARN的集群**

**下载到本地的 [Alluxio](https://alluxio.org/download)**

## 配置
通过一些特定的属性来定制Alluxio master和worker(比如，在各个worker节点上设置分层存储)，查阅[Configuration settings](Configuration-Settings.html)
为了确保你的配置能够被ApplicationMaster和Alluxio master/workers都读到，将`alluxio-site.properties` 放到`/etc/alluxio/alluxio-site.properties`中。

## 运行Alluxio应用

使用脚本`integration/yarn/bin/alluxio-yarn.sh`来启动Alluxio，这个脚本需要三个参数：

1. 需要启动的Alluxio workers的总数目（必须的）
2. 一个HDFS路径来分发包给Alluxio ApplicationMaster（必须的）
3. 运行Alluxio Master的节点的YARN的名字（可选的，默认为`${ALLUXIO_MASTER_HOSTNAME}`）

例如，启动一个有3个worker节点的Alluxio集群，其中一个HDFS的临时目录为`hdfs://${HDFS_MASTER}:9000/tmp/`，并且master节点的主机名为`${ALLUXIO_MASTER}`，你可以这样运行：

```bash
$ # If Yarn does not reside in `HADOOP_HOME`, set the environment variable `YARN_HOME` to the base path of Yarn.
$ export HADOOP_HOME=<path to hadoop home>
$ ${HADOOP_HOME}/bin/hadoop fs -mkdir hdfs://${HDFS_MASTER}:9000/tmp
$ ${ALLUXIO_HOME}/integration/yarn/bin/alluxio-yarn.sh 3 hdfs://${HDFS_MASTER}:9000/tmp/ ${ALLUXIO_MASTER}
```

你也可以单独启动Alluxio Master节点，在这种情况下，上述的启动能够在提供的地址上自动检测到Master而且跳过一个新实例的初始化。
如果你希望在一个特定的主机运行Master，这个主机不在你的YARN集群中，比如一个AWS EMR Master实例，这将会非常有用。

这个脚本会在YARN上启动一个Alluxio的Application Master，这个Application Master会为Alluxio master和workers申请容器。你可以在浏览器中查看YARN的UI来观察Alluxio作业的状态。

运行这个脚本会产生包含如下内容的输出

```
INFO impl.YarnClientImpl: Submitted application application_1445469376652_0002
```

这个application ID可以通过运行如下指令来销毁application：

```bash
$ ${HADOOP_HOME}/bin/yarn application -kill application_1445469376652_0002
```

这个ID也可以在YARN web UI上看到。

## 测试Alluxio

如果你有Alluxio application在运行，你可以通过在`conf/alluxio-site.properties`上配置`alluxio.master.hostname=masterhost`并运行如下指令来查看其健康性

```bash
$ ${ALLUXIO_HOME}/bin/alluxio runTests
```
