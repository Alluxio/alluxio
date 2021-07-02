---
layout: global
title: Alluxio集成Ozone作为底层存储
nickname: Alluxio集成Ozone作为底层存储
group: Storage Integrations
priority: 10
---

本指南介绍了如何将[Ozone](https://hadoop.apache.org/ozone)配置为Alluxio的底层存储系统。 
Ozone是用于Hadoop的可扩展，冗余和分布式对象存储。除了可以扩展到数十亿个大小不同的对象外， 
Ozone可以在容器化环境(例如Kubernetes和YARN)中有效运行。

## 先决条件

要在一组计算机上运行Alluxio集群，必须将Alluxio二进制文件部署到集群的每台
计算机上。你可以使用[直接下载预编译的二进制文件]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url}}
具有正确的Hadoop版本(推荐))，或 
[从Alluxio源代码编译二进制文件]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url}})(适用于高级用户)。

在准备Ozone与Alluxio一起使用时，请遵循[Ozone本地安装](https://hadoop.apache.org/ozone/docs/1.0.0/start/onprem.html)
安装Ozone集群，并遵循[卷命令](https://hadoop.apache.org/ozone/docs/1.0.0/shell/volumecommands.html)和 
[桶命令](https://hadoop.apache.org/ozone/docs/1.0.0/shell/bucketcommands.html)创建Ozone集群的卷和存储桶。

## 基本设置

要配置Alluxio使用Ozone做为底层存储系统，需要修改配置文件 
`conf/alluxio-site.properties`。如果此文件不存在，请从模板创建此配置文件。

```
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

编辑`conf/alluxio-site.properties`文件把底层存储地址设置为Ozone桶和 
想要挂载到Alluxio的Ozone目录。例如，如果要将整个存储桶挂载到Alluxio
底层存储的地址可以是`o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/`
，或者是`o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/alluxio/data`如果仅将`<OZONE_VOLUME>`的 `<OZONE_BUCKET>` ozone桶内的`/alluxio/data`目录映射到Alluxio。

```
alluxio.master.mount.table.root.ufs=o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/
``` 

## 示例:使用Ozone本地运行Alluxio

启动Alluxio服务器:

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

这将启动Alluxio master和Alluxio worker。可以在[http:// localhost:19999](http:// localhost:19999)上看到 master UI。

运行一个简单的示例程序

```console
$ ./bin/alluxio runTests
```

使用HDFS shell或Ozone shell来访问Ozone目录`o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/<OZONE_DIRECTORY>`
来确认由Alluxio创建的文件和目录是存在的。对于此测试，应该看到名为
`<OZONE_BUCKET>.<OZONE_VOLUME>/<OZONE_DIRECTORY>/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.的文件。

停止Alluxio运行

```console
$ ./bin/alluxio-stop.sh local
```
## 高级设置

### 挂载Ozone 

一个Ozone位置可以挂载到Alluxio命名空间中一个嵌套目录来保证对多个底层存储的统一访问。可以使用Alluxio的
[Mount Command]({{ '/en/operation/User-CLI.html' | relativize_url}}＃mount)来挂载。
例如，以下命令将Ozone存储桶中的一个目录挂载到Alluxio目录
`/ozone`上:

```console
$ ./bin/alluxio fs mount \
  --option alluxio.underfs.hdfs.configuration=<DIR>/ozone-site.xml:<DIR>/core-site.xml \
  /ozone o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/
```

可能的`core-site.xml`和`ozone-site.xml`文件设置
- `core-site.xml`

```xml
<configuration>
  <property>
    <name>fs.o3fs.impl</name>
    <value>org.apache.hadoop.fs.ozone.BasicOzoneFileSystem</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.o3fs.impl</name>
    <value>org.apache.hadoop.fs.ozone.BasicOzFs</value>
  </property>
</configuration>
```

- `ozone-site.xml`

```xml
<configuration>
  <property>
    <name>ozone.om.address</name>
    <value>localhost</value>
  </property>
  <property>
    <name>scm.container.client.max.size</name>
    <value>256</value>
  </property>
  <property>
    <name>scm.container.client.idle.threshold</name>
    <value>10s</value>
  </property>
  <property>
    <name>hdds.ratis.raft.client.rpc.request.timeout</name>
    <value>60s</value>
  </property>
  <property>
    <name>hdds.ratis.raft.client.async.outstanding-requests.max</name>
    <value>32</value>
  </property>
  <property>
    <name>hdds.ratis.raft.client.rpc.watch.request.timeout</name>
    <value>180s</value>
  </property>
</configuration>
```

确保相关的配置文件在所有运行Alluxio的服务器节点上。

### 支持的Ozone版本

当前，唯一经过与Alluxio测试Ozone版本是`1.0.0`。
