---
layout: global
title: 编译Alluxio源代码
nickname: 编译Alluxio源代码
group: Contributor Resources
priority: 0
---

* 内容列表
{:toc}

该指南介绍如何克隆Alluxio仓库，编译Alluxio源码，并且在你的环境中运行测试。

## 软件版本

- [Java JDK 8或以上](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [Maven 3.3.9](http://maven.apache.org/download.cgi)
- [Git](https://git-scm.org/downloads)

### Checkout源码

从Github上获取主分支并编译：

```console
$ git clone git://github.com/alluxio/alluxio.git
$ cd alluxio
$ export ALLUXIO_HOME=$(pwd)
```
您可以编译特定版本的Alluxio，否则这将编译源码的master分支。如果你要构建特定版本的代码，你可以使用 git tag 获取。
```console
$ git tag
$ git checkout <TAG_NAME>
```

## (可选) 使用Docker创建构建环境

本节将指导您使用我们发布的Docker镜像来构建一个编译环境。
如果本地已经安装了JDK和Maven，您可以跳过本章节在本地编译Alluxio。

首先启动一个名为 `alluxio-build` 的容器，然后进入这个容器继续操作：

```console
$ docker run -itd \
  --network=host \
  -v ${ALLUXIO_HOME}:/alluxio  \
  -v ${HOME}/.m2:/root/.m2 \
  --name alluxio-build \
  alluxio/alluxio-maven bash

$ docker exec -it -w /alluxio alluxio-build bash
```

注意，
- 容器路径 `/alluxio` 映射到主机路径 `${ALLUXIO_HOME}`，因此构建后的二进制文件仍然可以在容器外访问。
- 容器路径 `/root/.m2` 映射到主机路径 `${HOME}/.m2` 这可以利用 maven 缓存的本地副本，这是可选的。

使用完容器后，通过以下命令将其销毁

```console
$ docker rm -f alluxio-build
```

### 编译

使用Maven编译源码：

```console
$ mvn clean install -DskipTests
```

为了加速编译过程，你可以运行如下指令跳过不同的检查：

```console
$ mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip
```

Maven编译环境将自动获取依赖，编译源码，运行单元测试，并进行打包。如果你是第一次编译该项目，下载依赖包可能需要一段时间，但以后的编译过程将会快很多。

### 验证Alluxio编译完成

一旦Alluxio编译完成，你可以运行如下命令：

```console
$ echo "alluxio.master.hostname=localhost" > conf/alluxio-site.properties
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

若要确认Alluxio是否在运行，可以访问[http://localhost:19999](http://localhost:19999)，或者查看`alluxio/logs`目录下的日志文件，也可以执行下面的简单程序:

```console
$ ./bin/alluxio runTests
```

你将看到运行结果`Passed the test!`

你可以通过使用如下命令停止Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```

## 编译选项

### 计算框架支持
自Alluxio 1.7开始，编译后位于`{{site.ALLUXIO_CLIENT_JAR_PATH}}`的Alluxio客户端jar包将可适用于不同的计算框架（如：Spark、Flink，Presto等）。

### Apple M1 芯片支持
由于目前protoc暂未发行支持M1芯片的版本，在M1的Mac上编译需要使用兼容模式，使用x86_64的版本进行编译。
```console
$ mvn clean compile -DskipTests -Dos.detected.classifier=osx-x86_64
```

### Hadoop发行版的支持

默认情况下，Alluxio构建的HDFS发行版本为 Hadoop 3.3
要针对hadoop发行版本中某一个版本构建Alluxio，可以通过指定`<HADOOP_PROFILE>`和对应的`ufs.hadoop.version`来运行如下命令：

```console
$ mvn install -pl underfs/hdfs/ \
   -P<UFS_HADOOP_PROFILE> -Dufs.hadoop.version=<HADOOP_VERSION> -DskipTests
```

`<HADOOP_VERSION>`可以被设置不同值。可用的Hadoop配置文件包括`hadoop-1`, `hadoop-2`, `hadoop-3`，涵盖主要的Hadoop版本1.x, 2.x和3.x。

Hadoop versions >= 3.0.0 与新版本的Alluxio有最好的兼容性。

例如
```console
$ mvn clean install -pl underfs/hdfs/ \
  -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true \
  -Dcheckstyle.skip=true -Dfindbugs.skip=true \
  -Pufs-hadoop-3 -Dufs.hadoop.version=3.3.1
```
要启用`active sync`，请确保使用 `hdfsActiveSync` 属性来构建，
请参考 [Active Sync for HDFS]({{ '/cn/core-services/Unified-Namespace.html' | relativize_url }}#hdfs元数据主动同步) 获得更多关于使用Active Sync的信息。

如果你在`${ALLUXIO_HOME}/lib`目录中发现名为`alluxio-underfs-hdfs-<UFS_HADOOP_VERSION>-{{site.ALLUXIO_VERSION_STRING}}.jar`的jar，表明编译成功。

查看不同HDFS发行版的标志。
{% accordion Hdfs发行版 %}
{% collapsible Apache %}
#### Apache
所有主要版本都来自Apache，所以所有Apache发行版都可以直接使用。

```properties
-Phadoop-1 -Dhadoop.version=1.0.4
-Phadoop-1 -Dhadoop.version=1.2.0
-Phadoop-2 -Dhadoop.version=2.2.0
-Phadoop-2 -Dhadoop.version=2.3.0
-Phadoop-2 -Dhadoop.version=2.4.1
-Phadoop-2 -Dhadoop.version=2.5.2
-Phadoop-2 -Dhadoop.version=2.6.5
-Phadoop-2 -Dhadoop.version=2.7.3
-Phadoop-2 -Dhadoop.version=2.8.0
-Phadoop-2 -Dhadoop.version=2.9.0
-Phadoop-3 -Dhadoop.version=3.0.0
```

{% endcollapsible %}
{% collapsible Cloudera %}

对于Cloudera发行版，使用该形式`$apacheRelease-cdh$cdhRelease`的版本号

```properties
-Phadoop-2 -Dhadoop.version=2.3.0-cdh5.1.0
-Phadoop-2 -Dhadoop.version=2.0.0-cdh4.7.0
```

{% endcollapsible %}
{% collapsible Hortonworks %}

对于Hortonworks发行版，使用`$apacheRelease.$hortonRelease`形式的版本号

```properties
-Phadoop-2 -Dhadoop.version=2.1.0.2.0.5.0-67
-Phadoop-2 -Dhadoop.version=2.2.0.2.1.0.0-92
-Phadoop-2 -Dhadoop.version=2.4.0.2.1.3.0-563
```

{% endcollapsible %}
{% endaccordion %}

## 故障排除

### 编译时出现 OutOfMemoryError 的异常

如果你看到`java.lang.OutOfMemoryError: Java heap space`，请增大如下maven变量，以增加可使用的内存空间。

```console
$ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
````

### protolock 错误

如果你看到maven的以下错误信息。
"`An error occurred while running protolock: Cannot run program "/alluxio/core/transport/target/protolock-bin/protolock" (in directory "/alluxio/core/transport/target/classes"): error=2, No such file or directory`"

请确保在构建源代码时不包含maven标志"`-Dskip.protoc`"。

### NullPointerException occurred while execute org.codehaus.mojo:buildnumber-maven-plugin:1.4:create

如果你看到maven有以下错误信息。
"`Failed to execute goal org.codehaus.mojo:buildnumber-maven-plugin:1.4:create-metadata (default) on project alluxio-core-common: Execution default of goal org.codehaus.mojo:buildnumber-maven-plugin:1.4:create-metadata failed: NullPointerException`"

是由于`build number`是基于从SCM检索的`revision number`，它将从git哈希代码中检查`build number`。
如果检查失败，SCM就会抛出NPE。为避免该异常，请用maven参数"`-Dmaven.buildNumber.revisionOnScmFailure`"来设置Alluxio版本。
例如，如果alluxio的版本是2.7.3，那么请设置参数"`-Dmaven.buildNumber.revisionOnScmFailure=2.7.3`"。

更多信息可见[此处](https://www.mojohaus.org/buildnumber-maven-plugin/create-mojo.html#revisionOnScmFailure)。
