---
layout: global
title: Alluxio集成CephFS作为底层存储
nickname: Alluxio集成CephFS作为底层存储
group: Storage Integrations
priority: 11
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用CephFS作为底层文件系统。Alluxio使用[CephFS](https://docs.ceph.com/en/latest/cephfs/)
目前，支持两种不同类型的UFS CephFS：
- [cephfs](https://docs.ceph.com/en/latest/cephfs/api/libcephfs-java/)
- [cephfs-hadoop](https://docs.ceph.com/en/nautilus/cephfs/hadoop/)

## 初始步骤

首先，在你的机器上必须安装Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-From-Source.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

其次，在你的机器上需要安装以下包:

```
cephfs-java
libcephfs_jni
libcephfs2
```

以上，包的安装请参考[ceph包安装](https://docs.ceph.com/en/latest/install/get-packages/)，

再次，建立软连：

```
$ ln -s /usr/lib64/libcephfs_jni.so.1.0.0 /usr/lib64/libcephfs_jni.so
$ ln -s /usr/lib64/libcephfs.so.2.0.0 /usr/lib64/libcephfs.so
$ java_path=`which java | xargs readlink | sed 's#/bin/java/##g'`
$ ln -s /usr/share/java/libcephfs.jar $java_path/jre/lib/ext/libcephfs.jar
```

最后，下载CephFS Hadoop jar包:

```
$ curl -o $java_path/jre/lib/ext/hadoop-cephfs.jar -d https://download.ceph.com/tarballs/hadoop-cephfs.jar
```

## 配置Alluxio

为了配置Alluxio以使用底层文件系统，需要修改`alluxio-site.properties`和`core-site.xml`文件。如果文件不存在，根据模板创建配置文件。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
$ cp conf/core-site.xml.template conf/core-site.xml
```

{% navtabs 配置 %}
{% navtab 方法1: cephfs %}

向`conf/alluxio-site.properties`文件添加以下代码：

```
alluxio.underfs.cephfs.conf.file=<ceph-conf-file>
alluxio.underfs.cephfs.mds.namespace=<ceph-fs-name>
alluxio.underfs.cephfs.mount.point=<ceph-fs-dir>
alluxio.underfs.cephfs.auth.id=<client-id>
alluxio.underfs.cephfs.auth.keyring=<client-keyring-file>
```

{% endnavtab %}
{% navtab 方法2: cephfs-hadoop %}

向`conf/alluxio-site.properties`文件添加以下代码：

```
alluxio.underfs.hdfs.configuration=${ALLUXIO_HOME}/conf/core-site.xml
```

向`conf/core-site.xml`文件添加以下代码：

```
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>ceph://mon1,mon2,mon3/</value>
  </property>
  <property>
    <name>fs.defaultFS</name>
    <value>ceph://mon1,mon2,mon3/</value>
  </property>
  <property>
    <name>ceph.data.pools</name>
    <value>${data-pools}</value>
  </property>
  <property>
    <name>ceph.auth.id</name>
    <value>${client-id}</value>
  </property>
  <property>
    <name>ceph.conf.options</name>
    <value>client_mount_gid=${gid},client_mount_uid=${uid},client_mds_namespace=${ceph-fs-name}</value>
  </property>
  <property>
    <name>ceph.root.dir</name>
    <value>${ceph-fs-dir}</value>
  </property>
  <property>
    <name>ceph.mon.address</name>
    <value>mon1,mon2,mon3</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.ceph.impl</name>
    <value>org.apache.hadoop.fs.ceph.CephFs</value>
  </property>
  <property>
    <name>fs.ceph.impl</name>
    <value>org.apache.hadoop.fs.ceph.CephFileSystem</value>
  </property>
  <property>
    <name>ceph.auth.keyring</name>
    <value>${client-keyring-file}</value>
  </property>
</configuration>
```

{% endnavtab %}
{% endnavtabs %}

## 使用CephFS本地运行Alluxio

完成所有的配置之后，你可以本地运行Alluxio,观察是否一切运行正常。

```
./bin/alluxio format
./bin/alluxio-start.sh local
```

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

{% navtabs 测试 %}
{% navtab 方法1: cephfs %}

```
./bin/alluxio fs mkdir /mnt/cephfs
./bin/alluxio fs mount /mnt/cephfs cephfs://mon1\;mon2\;mon3/
```
接着，你可以运行一个简单的示例程序：

```
./bin/alluxio runTests --path cephfs://mon1\;mon2\;mon3/
```

运行成功后，访问你的alluxio /mnt/cephfs 和 cephfs <cephfs-fs-dir> 目录，确认其中包含了由Alluxio创建的文件和目录default_tests_files/Basic_CACHE_THROUGH _。
前者，通过Alluxio的[Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize__url }})可被访问；
后者，通过ceph-fuse或mount的方式使用POSIX APIs[Mounting CephFS](https://docs.ceph.com/en/latest/cephfs/#mounting-cephfs)可被访问。

{% endnavtab %}
{% navtab 方法2: cephfs-hadoop %}

```
./bin/alluxio fs mkdir /cephfs-hadoop
./bin/alluxio fs mount /cephfs-hadoop ceph://mon1\;mon2\;mon3/
```
接着，你可以运行一个简单的示例程序：

```
./bin/alluxio runTests --path ceph://mon1\;mon2\;mon3/
```

运行成功后，访问你的alluxio /mnt/cephfs-hadoop 和 cephfs <cephfs-fs-dir> 目录，确认其中包含了由Alluxio创建的文件和目录default_tests_files/Basic_CACHE_THROUGH _。
前者，通过Alluxio的[Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize__url }})可被访问；
后者，通过ceph-fuse或mount的方式使用POSIX APIs[Mounting CephFS](https://docs.ceph.com/en/latest/cephfs/#mounting-cephfs)可被访问。

{% endnavtab %}
{% endnavtabs %}

