---
layout: global
title: 在MapR-FS上配置Alluixo
nickname: Alluxio使用MapR-FS
group: Under Store
priority: 3
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[MapR-FS](https://www.mapr.com/products/mapr-fs)作为底层存储系统。

## 以特定MapR版本编译Alluxio

Alluxio要与MapR-FS结合，必须以相对应的MapR版本进行[编译](Building-Alluxio-Master-Branch.html)，以下是一些对应于不同MapR版本的`hadoop.version`：

<table class="table table-striped">
<tr><th>MapR Version</th><th>hadoop.version</th></tr>
<tr>
  <td>5.2</td>
  <td>2.7.0-mapr-1607</td>
</tr>
<tr>
  <td>5.1</td>
  <td>2.7.0-mapr-1602</td>
</tr>
<tr>
  <td>5.0</td>
  <td>2.7.0-mapr-1506</td>
</tr>
<tr>
  <td>4.1</td>
  <td>2.5.1-mapr-1503</td>
</tr>
<tr>
  <td>4.0.2</td>
  <td>2.5.1-mapr-1501</td>
</tr>
<tr>
  <td>4.0.1</td>
  <td>2.4.1-mapr-1408</td>
</tr>
</table>

## 针对MapR-FS配置Alluxio

一旦你以对应的`hadoop.version`和MapR版本编译了Alluxio，就需要配置Alluxio从而使其能够识别MapR-FS的模式和URI。Alluxio能够使用HDFS客户端访问MapR-FS，要让HDFS客户端能够访问MapR-FS URI，需要给`alluxio.underfs.hdfs.prefixes`配置项添加一个`maprfs:///`前缀:

```
alluxio.underfs.hdfs.prefixes=hdfs://,maprfs:///
```

该配置项需要在所有的Alluxio服务器上（masters和workers）进行设置，具体可以参考[Alluxio配置](Configuration-Settings.html)。对于Alluxio进程，该配置项能够在`alluxio-site.properties`配置文件中设置，请参考[通过属性文件配置Alluxio](Configuration-Settings.html#property-files)获取更多信息。

另外，该配置项还需要在所有访问Alluxio的客户端中进行配置，这也就意味着在所有访问Alluxio的应用中（MapReduce、Spark、Flink等等）也要进行配置。通常可以在命令行中添加`-Dalluxio.underfs.hdfs.prefixes=hdfs://,maprfs:///`完成设置，可以参考[配置Alluxio应用](Configuration-Settings.html#application-settings)获取更多信息。

## 配置Alluxio以使用MapR-FS作为底层文件系统

有多种方式配置Alluxio以使用MapR-FS作为底层文件系统。如果你需要将MapR-FS挂载到Alluxio的根目录，添加以下配置项到`conf/alluxio-site.properties`属性文件中：

```
alluxio.underfs.address=maprfs:///<path in MapR-FS>/
```

也可以将MapR-FS中的某个目录挂载到Alluxio命名空间中：

```bash
$ ${ALLUXIO_HOME}/bin/alluxio fs mount /<path in Alluxio>/ maprfs:///<path in MapR-FS>/
```

## 使用MapR-FS在本地运行Alluxio

完成所有相应配置后，你可以在本地启动Alluxio，观察一切是否正常运行：

{% include Common-Commands/start-alluxio.md %}

这应当会在本地启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

成功启动后，你可以访问MapR-FS的web UI以确认通过Alluxio创建的文件和目录确实存在。对于该测试，你应该可以看到类似`/default_tests_files/Basic_CACHE_THROUGH`的文件。

接着，可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行以下命令停止Alluxio：

{% include Common-Commands/stop-alluxio.md %}
