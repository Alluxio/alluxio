---
layout: global
title: 开发者指导
nickname: 开发者指导
group: Resources
---

* 内容列表
{:toc}

> 如果你是一个新开源贡献者，请先浏览[开始贡献源码向导](Contributing-Getting-Started.html)以熟悉如何向Alluxio贡献源码。

感谢您对Alluxio的兴趣！我们非常感谢任何的新特性或者修复的贡献。

## 提交代码

-   我们鼓励你每次尽可能提交小的、单一目的的补丁包，因为要合并一个含有许多不相关的特性的大的改动十分困难。

-   我们会追踪[JIRA](https://alluxio.atlassian.net/)里的issues和features，如果你还没有帐号，请先进行注册。

-   设立[JIRA](https://alluxio.atlassian.net/)里的一个ticket，在里面详细介绍提议的修改和该修改的目的。

-   将你的修改作为一个GitHub pull request进行提交，可以查看Github向导中的[forking a repo](https://help.github.com/articles/fork-a-repo)和[sending a pull request](https://help.github.com/articles/using-pull-requests)学习如何进行这些操作。

-   在你提交的pull request的标题中，确保其引用了该JIRA ticket，这将会连接到该指定的ticket。例如：

~~~~~
[ALLUXIO-100] Implement an awesome new feature
~~~~~

-   在pull request的描述框中，请添加该JIRA ticket的超链接。

注意对于一些很小的修改，在提交pull request之前不必创建对应的JIRA tickets，例如

-   对于仅仅修改一些错别字或者进行一些格式化的pull request，你可以在该pull request的标题中添加"[SMALLFIX]"前缀，例如：

~~~~~
[SMALLFIX] Fix formatting in Foo.java
~~~~~

-   对于改善Alluxio项目网站的文档（例如修改`docs`目录下的markdown文件）的pull request，你可以在该pull request的标题中添加"[DOCFIX]"前缀，例如，修改由`docs/Contributing-to-Alluxio.md`生成的网页文件，其标题可以是：

~~~~~
[DOCFIX] Improve documentation of how to contribute to Alluxio
~~~~~

## 单元测试

- 运行所有单元测试

```bash
$ cd ${ALLUXIO_HOME}
$ mvn test
```

这会使用本地文件系统作为底层文件系统。

- 运行单个单元测试

```bash
$ mvn -Dtest=AlluxioFSTest#createFileTest -DfailIfNoTests=false test
```

- 要运行特定模块的单元测试, 在想要的子模块下执行`maven test`命令。例如，要运行HDFS UFS模块测试，执行以下命令

```bash
$ mvn test -pl underfs/hdfs
```

用Hadoop版本运行HDFS UFS模块单元测试：

```bash
# build and run test on HDFS under filesystem module for Hadoop 2.7.0
$ mvn test -pl underfs/hdfs -Phadoop-2 -Dhadoop.version=2.7.0
# build and run test on HDFS under filesystem module for Hadoop 3.0.0
$ mvn test -pl underfs/hdfs -Phadoop-3 -Dhadoop.version=3.0.0
```

以上命令会创建一个特定版本的模拟HDFS服务。要用实时运行的配置在HDFS上运行更多可解释的测试：

```bash
$ mvn test -pl underfs/hdfs -PufsContractTest -DtestHdfsBaseDir=hdfs://ip:port/alluxio_test
```

- 要想日志输出到STDOUT, 在mvn命令后添加以下参数：

```
-Dtest.output.redirect=false -Dalluxio.root.logger=DEBUG,CONSOLE
```

- 要以交互的方式快速运行某些API测试，你可能需要使用Scala shell，这在
[blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding)有讨论。

- 如果libfuse库丢失，其测试将被忽略。要运行这些测试，请安装[this page](Mounting-Alluxio-FS-with-FUSE.html#requirements)中所提到的正确的库。

### 系统设置

有时你要配置一些系统设置使得测试能在本地通过。其中之一就是`ulimit`。

要使得MacOS上允许的文件数和进程数增加，运行以下命令

```bash
$ sudo launchctl limit maxfiles 32768 32768
$ sudo launchctl limit maxproc 32768 32768
```

同时推荐从Spotlight indexing导出本地Alluxio。否则，你的Mac在单元测试时将会一直挂起来尝试重新定位文件系统。去`System Preferences > Spotlight > Privacy`，点击`+`键，浏览你本地Alluxio的目录，点击`Choose`将其添加到导出列表。

## 编码风格

-   请遵循已有代码的风格。具体地，我们使用[Google Java style](https://google.github.io/styleguide/javaguide.html)风格，但有以下不同：
    -  每行最多**100**个字符
    -  第三方导入被整理到一起以使得IDE格式化起来更简单
    -  类成员变量要使用`m`前缀，例如`private WorkerClient mWorkerClient;`
    -  静态成员变量要使用`s`前缀，例如`public static String sUnderFSAddress;`
-   Bash脚本遵循[Google Shell style](https://google.github.io/styleguide/shell.xml), 且必须兼容Bash 3.x版本
-   你可以下载我们提供的[Eclipse formatter](../resources/alluxio-code-formatter-eclipse.xml)
    -  为了让Eclipse能够正确地组织你的导入, 配置"组织导入"以看上去像
       [这样](../resources/eclipse_imports.png)
    -  如果你使用IntelliJ IDEA:
       - 你可以使用我们提供的formatter，参考[Eclipse Code Formatter](https://github.com/krasa/EclipseCodeFormatter#instructions)，或者在IntelliJ
       IDEA中使用[Eclipse Code Formatter Plugin](http://plugins.jetbrains.com/plugin/6546)
       - 要自动格式化**import**，在Preferences->Code Style->Java->Imports->Import中设置Layout为[该顺序](../resources/importorder.png)
       - 要自动将方法按字母顺序重新排序，可以使用[Rearranger插件](http://plugins.jetbrains.com/plugin/173)，打开Preferences，查找rearrager，去除不必要的内容，然后右击，选择"Rearrange"，代码将格式化成你需要的风格。

-  为验证编码风格符合标准，你在提交pull request之前应该先运行[checkstyle](http://checkstyle.sourceforge.net)，并且保证没有警告：

```bash
$ mvn checkstyle:checkstyle
```

## 日志约定

Alluxio使用[SLF4J](https://www.slf4j.org/)记录日志，典型用法为：

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public MyClass {

  private static final Logger LOG = LoggerFactory.getLogger(MyClass.class);

    public void someMethod() {
      LOG.info("Hello world");
    }
}
```

下面是Alluxio源码对不同类别日志的约定：

* 错误级别日志（`LOG.error`）表示无法恢复的系统级问题。错误级别日志总是伴随着堆栈跟踪信息。
```java
LOG.error("Failed to do something due to an exception", e);
```
* 警告级别日志（`LOG.warn`）表示用户预期行为和Alluxio实际行为之间的逻辑不匹配。警告级别日志伴有异常消息。相关的堆栈跟踪信息可以在调试级日志中找到。
```java
LOG.warm("Failed to do something due to {}", e.getMessage());
```
* 信息级别日志（`LOG.info`）记录了重要系统状态的更改信息。异常消息和堆栈跟踪与信息级别日志从无关联。
```java
LOG.info("Master started.");
```
* 调试级别日志（`LOG.debug`）包括Alluxio系统各方面的详细信息。控制流日志记录（Alluxio系统的进入和退出调用）在调试级别日志中完成。调试级别日志异常通常有包含堆栈跟踪的具体信息。
```java
LOG.debug("Failed to connec to {} due to exception", host + ":" + port, e); // wrong
LOG.debug("Failed to connec to {} due to exception", mAddress, e); // OK
if (LOG.isDebugEnabled()) {
    LOG.debug("Failed to connec to address {} due to exception", host + ":" + port, e); // OK
}
```
* 跟踪级别日志在Alluxio中不使用。

## FindBugs

在提交pull request之前，对最新的代码运行
[FindBugs](http://findbugs.sourceforge.net/)确保不出现警告：

{% include Contributing-to-Alluxio/findbugs.md %}

## IDE

你可以通过运行以下命令生成Eclipse配置文件：

{% include Contributing-to-Alluxio/eclipse-configuration.md %}

然后将该文件夹导入到Eclipse中。

也可以运行以下命令将M2_REPO添加到classpath变量中：

{% include Contributing-to-Alluxio/M2_REPO.md %}

如果你使用的是IntelliJ IDEA，你可能需要修改Maven profile配置中的'developer'以防止导入错误，可以通过以下方式进行：

    View > Tool Windows > Maven Projects

## 更改Thrift RPC的定义

Alluxio使用Thrift来完成客户端与服务端的RPC通信。`.thrift`文件定义在`common/src/thrift/`目录下，其一方面用于自动生成客户端调用RPC的Java代码，另一方面用于实现服务端的RPC。要想更改一个Thrift定义，你首先必须要[安装Thrift的编译器](https://thrift.apache.org/docs/install/)。如果你的机器上有brew，你可以通过运行下面的命令来完成。

{% include Developer-Tips/install-thrift.md %}

然后重新生成Java代码，运行

{% include Developer-Tips/thriftGen.md %}

## 更改Protocol Buffer消息

Alluxio使用Protocol Buffer来读写日志消息。`.proto`文件被定义在`servers/src/proto/journal/`目录下，其用于为Protocol Buffer消息自动生成Java定义。要需要修改这些消息，首先要[读取更新的消息类型](https://developers.google.com/protocol-buffers/docs/proto#updating)从而保证你的修改不会破坏向后兼容性。然后就是[安装protoc](https://github.com/google/protobuf#protocol-buffers---googles-data-interchange-format)。如果你的机器上有brew，你可以通过运行下面的命令来完成。

{% include Developer-Tips/install-protobuf.md %}

然后重新生成Java代码，运行

{% include Developer-Tips/protoGen.md %}

## bin/alluxio目录下的命令列表

开发者所用到的大多数命令都在`bin/alluxio`目录下。下面的表格有对每条命令及其参数的说明。

<table class="table table-striped">
<tr><th>命令</th><th>参数</th><th>介绍</th></tr>
{% for dscp in site.data.table.Developer-Tips %}
<tr>
  <td>{{dscp.command}}</td>
  <td>{{dscp.args}}</td>
  <td>{{site.data.table.cn.Developer-Tips[dscp.command]}}</td>
</tr>
{% endfor %}
</table>

此外，这些命令的执行有不同的先决条件。`format`，`formatWorker`，`journalCrashTest`，`readJournal`，`version`，`validateConf`和`validateEnv`命令的先决条件是你已经构建了Alluxio（见[构建Alluxio主分支](Building-Alluxio-From-Source.html)其介绍了如何手动构建Alluxio)。而`fs`，`loadufs`，`logLevel`, `runTest`和`runTests`命令的先决条件是你已经运行了Alluxio系统。
