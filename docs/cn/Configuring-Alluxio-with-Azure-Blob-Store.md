
---
layout： global
title： 在Azure Blob Store上配置Alluxio
nickname： Alluxio使用Azure Blob Store
group： Under Store
priority： 0
---

* 内容列表
{:toc}

该指南介绍了如何配置Alluxio以使用[Azure Blob Store](https://azure.microsoft.com/en-in/services/storage/blobs/)作为底层存储系统。

## 初始步骤

为了在多台机器上运行一个Alluxio集群，你必须在每台机器上部署Alluxio的二进制文件。
你可以[从Alluxio源码编译二进制文件](Building-Alluxio-Master-Branch.html)，或者[直接下载已经编译好的Alluxio二进制文件](Running-Alluxio-Locally.html)。

接着，如果还没有配置，用`bootstrapConf`命令创建你的配置文件。
例如，如果你是在你的本地机器运行Alluxio,`ALLUXIO_MASTER_HOSTNAME`应该被设置为`localhost`。

```
$ ./bin/alluxio bootstrapConf <ALLUXIO_MASTER_HOSTNAME>
```

或者你可以从模板创建文件并手动设置内容。

```
$ cp conf/alluxio-env.sh.template conf/alluxio-env.sh
```
而且，为了在Alluxio上使用Azure Blob Store，在你的Azure storage帐户上创建一个新的container或者使用一个已有的container。你应该注意你在这个container里准备使用的目录,你可以在这个容器里面创建一个目录，或者使用一个已有的目录。鉴于这篇文章的目的，我们将Azure storage帐户名取名为`AZURE_ACCOUNT`，将帐户里的容器取名为`AZURE_CONTAINER`并将该container里面的目录称为`AZURE_DIRECTORY`。更多关于Azure storage帐户的信息，请看[这里](https://docs.microsoft.com/en-us/azure/storage/storage-create-storage-account)。

## 配置Alluxio
Alluxio可以通过HDFS接口支持Azure blob store。你可以在[这里](http://hadoop.apache.org/docs/r2.7.1/hadoop-azure/index.html)找到更多关于在Azure blob store上运行hadoop的信息。
从[这里](https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage)下载azure storage的java库(版本2.2.0)并根据你的Hadoop版本从[这里](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure)下载hadoop azure库。请确定使用了 `azure-storage-2.2.0.jar`而不是任何更高版本的，因为这些版本会与hadoop-azure库冲突。

你需要将上面提到的库加入到`ALLUXIO_CLASSPATH`。你可以通过在`conf/alluxio-env.sh`中添加以下代码来完成：
```
export ALLUXIO_CLASSPATH=PATH_TO_HADOOP_AZURE_JAR/hadoop-azure-2.7.3.jar:PATH_TO_AZURE_STORAGE_JAR/azure-storage-2.2.0.jar
```

你需要配置Alluxio以使用Azure Blob Store作为底层存储系统。第一个需要修改的地方就是指定底层文件系统的地址并且设置hdfs前缀以便Alluxio可以识别 `wasb://`机制，你需要修改`conf/alluxio-site.properties`以包含以下内容：

```
alluxio.underfs.address=wasb://AZURE_CONTAINER@AZURE_ACCOUNT.blob.core.windows.net/AZURE_DIRECTORY/
alluxio.underfs.hdfs.prefixes=hdfs://,glusterfs:///,maprfs:///,wasb://
```

接着你需要通过将下列属性添加到`conf/core-site.xml`中来指定证书和wasb的实现类：
```
<configuration>
<property>
  <name>fs.AbstractFileSystem.wasb.impl</name>
  <value>org.apache.hadoop.fs.azure.Wasb</value>
</property>
<property>
  <name>fs.azure.account.key.AZURE_ACCOUNT.blob.core.windows.net</name>
  <value>YOUR ACCESS KEY</value>
</property>
</configuration>
```

完成这些修改后，Alluxio应该已经配置好以使用Azure Blob Store作为底层存储系统，你可以试着使用它本地运行Alluxio。

## 使用Azure Blob Store本地运行Alluxio

完成所有配置之后，你可以本地运行Alluxio,观察是否一切运行正常。

```
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

```
$ ./bin/alluxio runTests
```

运行成功后，你可以访问你的容器AZURE_CONTAINER，确认其中包含了由Alluxio创建的文件和目录。该测试中，创建的文件名应该像下面这样：

```
AZURE_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE
```

若要停止Alluxio，你可以运行以下命令:

```
$ ./bin/alluxio-stop.sh local
```
