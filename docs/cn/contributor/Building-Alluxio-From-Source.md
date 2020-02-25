---
layout: global
title: 编译Alluxio源代码
nickname: 编译Alluxio源代码
group: Contributor Resources
priority: 0
---

* 内容列表
{:toc}

## 编译Alluxio

该指南介绍如何从头编译Alluxio。

这部分内容的前提条件是你已安装Java JDK 8或以上、Maven 3.3.9或以上。

### Checkout源码

从Github上获取主分支并编译：

```console
$ git clone git://github.com/alluxio/alluxio.git
$ cd alluxio
```
您可以编译特定版本的Alluxio，否则这将编译源码的master分支。

```console
$ git tag
$ git checkout <TAG_NAME>
```

### 编译

使用Maven编译源码：

```console
$ mvn clean install -DskipTests
```

为了加速编译过程，你可以运行如下指令跳过不同的检查：

```console
$ mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip  -Dskip.protoc
```
注意：有`-Dskip.protoc`这个标志，说明不执行protoc，也就不会生成proto相关的源文件。因此，不应该在首次build的时候使用`-Dskip.protoc`。

如果你看到了 `java.lang.OutOfMemoryError: Java heap space`，请设置如下变量增大maven可使用的内存空间：

```console
$ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
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
自Alluxio 1.7开始，**你不要在Maven编译的时候使用不同计算框架的配置文件**。编译后位于`{{site.ALLUXIO_CLIENT_JAR_PATH}}`的Alluxio客户端jar包将适用于不同的计算框架（如：Spark、Flink，Presto等）。

### Hadoop发行版的支持
要针对hadoop发行版本中某一个版本构建Alluxio，可以通过指定`<HADOOP_PROFILE>`和对应的`hadoop.version`来运行如下命令：

```console
$ mvn install -P<HADOOP_PROFILE> -Dhadoop.version=<HADOOP_VERSION> -DskipTests
```

`<HADOOP_VERSION>`可以被设置不同值。可用的Hadoop配置文件包括`hadoop-1`, `hadoop-2`, `hadoop-3`，涵盖主要的Hadoop版本1.x, 2.x和3.x。

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

#### Cloudera
对于Cloudera发行版，使用该形式`$apacheRelease-cdh$cdhRelease`的版本号

```properties
-Phadoop-2 -Dhadoop.version=2.3.0-cdh5.1.0
-Phadoop-2 -Dhadoop.version=2.0.0-cdh4.7.0
```

#### Hortonworks

对于Hortonworks发行版，使用`$apacheRelease.$hortonRelease`形式的版本号

```properties
-Phadoop-2 -Dhadoop.version=2.1.0.2.0.5.0-67
-Phadoop-2 -Dhadoop.version=2.2.0.2.1.0.0-92
-Phadoop-2 -Dhadoop.version=2.4.0.2.1.3.0-563
```
