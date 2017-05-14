---
layout: global
title: 在Alluxio上运行Apache HBase
nickname: Apache HBase
group: Frameworks
priority: 2
---

* 内容列表
{:toc}

该文档介绍如何运行[Apache HBase](http://hbase.apache.org/)，以能够在不同存储层将HBase的表格存储到Alluxio当中。

## 前期准备

开始之前你需要安装好[Java](Java-Setup.html)。同时使用[本地模式](Running-Alluxio-Locally.html)或[集群模式](Running-Alluxio-on-a-Cluster.html)构建好Alluxio。

请在[Apache HBase Configuration](https://hbase.apache.org/book.html#configuration)网站上阅读HBase安装说明。

## 配置

Apache HBase可以通过通用文件系统包装类（可用于Hadoop文件系统）来使用Alluxio。因此，Alluxio的配置主要在HBase配置文件中完成。

### 在`hbase-site.xml`中设置属性

需要添加以下3个属性到HBase安装的`conf`目录下的`hbase-site.xml`文件中(确保这些属性在所有HBase集群节点中都被配置好)：

提示：无需在Alluxio中创建/hbase目录，HBase将会创建。

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
<property>
  <name>fs.AbstractFileSystem.alluxio.impl</name>
  <value>alluxio.hadoop.AlluxioFileSystem</value>
</property>
<property>
  <name>hbase.rootdir</name>
  <value>alluxio://<hostname>:<port>/hbase</value>
</property>
```

## 分发Alluxio客户端Jar包

接下来需要让Alluxio client `jar`文件对HBase可用，因为其中包含了配置好的`alluxio.hadoop.FileSystem`类。

有2种方式实现：

- 将`{{site.ALLUXIO_CLIENT_JAR_PATH}}`文件放在HBase的`lib`目录下。
- 在`$HBASE_CLASSPATH`环境变量中指定该jar文件的路径（要保证该路径对集群中的所有节点都有效）。例如：

```bash
export HBASE_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HBASE_CLASSPATH}
```

### 添加Alluxio site中额外属性到HBase

如果Alluxio site中有任何想要指定给HBase的属性，将其添加到`hbase-site.xml`。例如，
将`alluxio.user.file.writetype.default`从默认的`MUST_CACHE`改为`CACHE_THROUGH`：

```xml
<property>
<name>alluxio.user.file.writetype.default</name>
<value>CACHE_THROUGH</value>
</property>
```

## 在HBase中使用Alluxio

启动HBase

```bash
$ ${HBASE_HOME}/bin/start-hbase.sh
```

访问HBase网址`http://<hostname>:16010`的Web用户界面以确认HBase在Alluxio上运行
（检查`HBase Root Directory`属性）：

![HBaseRootDirectory]({{site.data.img.screenshot_start_hbase_webui}})

并且访问Alluxio网址为`http://<hostname>:19999`的Web用户界面，点击 "Browse" 就会看到HBase存储在Alluxio上的文件，包括数据和WALs:

![HBaseRootDirectoryOnAlluxio]({{site.data.img.screenshot_start_hbase_alluxio_webui}})

## HBase shell示例

创建一个文本文件`simple_test.txt`并且将这些命令写进去：

```
create 'test', 'cf'
for i in Array(0..9999)
 put 'test', 'row'+i.to_s , 'cf:a', 'value'+i.to_s
end
list 'test'
scan 'test', {LIMIT => 10, STARTROW => 'row1'}
get 'test', 'row1'
```

从HBase最顶层项目目录运行以下命令：

```bash
bin/hbase shell simple_test.txt
```

将会看到一些类似这样的输出：

![HBaseShellOutput]({{site.data.img.screenshot_hbase_shell_output}})

如果已经安装了Hadoop,可以在HBase shell中运行一个Hadoop功能程序以统计新创建的表的行数：

```bash
bin/hbase org.apache.hadoop.hbase.mapreduce.RowCounter test
```

在这个mapreduce作业结束后，会看到如下结果：

![HBaseHadoopOutput]({{site.data.img.screenshot_hbase_hadoop_output}})
