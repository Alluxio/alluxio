---
layout: global
title: 开发者向导
nickname: 开发者向导
group: Resources
---

* 内容列表
{:toc}

感谢您对Alluxio的兴趣！我们非常感谢任何的新特性或者修复的贡献。

## 新贡献者

如果你是一个新开源贡献者，请先浏览[开始贡献源码向导](Contributing-Getting-Started.html)以熟悉如何向Alluxio贡献源码。

### Alluxio初始任务

新开发者可以先进行以下列出的任务，从而能够更加熟悉Alluxio：

1.  [在本地运行Alluxio](Running-Alluxio-Locally.html)

2.  [在集群运行Alluxio](Running-Alluxio-on-a-Cluster.html)

3.  阅读[配置项设置](Configuration-Settings.html)以及[命令行接口](Command-Line-Interface.html)

4.  阅读一段[代码示例](https://github.com/alluxio/alluxio/blob/master/examples/src/main/java/alluxio/examples/BasicOperations.java)

5.  [构建Alluxio主分支](Building-Alluxio-Master-Branch.html)

6.  Fork Alluxio Github仓库，并添加一两个单元测试或者javadoc文件，再提交一个pull request。也欢迎你处理我们的[JIRA](https://alluxio.atlassian.net/browse/ALLUXIO)中的issues。这里是一些未分配的[新开发者任务](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20status%20%3D%20Open%20AND%20labels%20%3D%20NewContributor%20AND%20assignee%20in%20(EMPTY))。
请每个新开发者最多只完成的两个New-Contributor任务。
在这之后，尝试去做一些Beginner/Intermediate任务，或者在[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)里咨询。可以查看Github向导中的[forking a repo](https://help.github.com/articles/fork-a-repo)和[sending a pull request](https://help.github.com/articles/using-pull-requests)学习如何进行基本Github操作。

### 提交代码

-   我们鼓励你每次尽可能提交小的、单一目的的补丁包，因为要合并一个含有许多不相干的特性的大的改动十分困难。

-   我们会追踪[JIRA](https://alluxio.atlassian.net/)里的issues和features，如果你还没有帐号，请先进行注册。

-   设立[JIRA](https://alluxio.atlassian.net/)里的一个ticket，在里面详细介绍提议的修改和该修改的目的。

-   将你的修改作为一个GitHub pull request进行提交，可以查看Github向导中的[forking a repo](https://help.github.com/articles/fork-a-repo)和[sending a pull request](https://help.github.com/articles/using-pull-requests)学习如何进行这些操作。

-   在你提交的pull request的标题中，确保其引用了该JIRA ticket，这将会连接到该指定的ticket。例如：

~~~~~
[ALLUXIO-100] Implement an awesome new feature
~~~~~

-   添加一个GitHub标签表明你的修改属于Alluxio的哪个组件，如果你对此不确定，那么就选择包含最多的修改代码的那个组件（组件与根目录下的目录一一对应）。

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

#### 测试

-   使用``mvn test``命令运行所有单元测试（会将本地文件系统作为底层文件系统，在HDFS模块中将HDFS 1.0.4作为底层文件系统），``mvn -Dhadoop.version=2.4.0 test``命令将HDFS 2.4.0作为HDFS模块中的底层文件系统。

-   要在特定的底层文件系统上运行测试，在对应的子模块目录下运行Maven命令，例如，要运行针对HDFS的测试，在``alluxio/underfs/hdfs``目录下运行``mvn test``。

-   对于GlusterFS环境的单元测试，可以在``alluxio/underfs/glusterfs``目录下运行`mvn -PglusterfsTest -Dhadoop.version=2.3.0 -Dalluxio.underfs.glusterfs.mounts=/vol -Dalluxio.underfs.glusterfs.volumes=testvol test`（将GlusterFS作为底层文件系统，`/vol`为一个有效GlusterFS挂载点）。

-   要进行单个单元测试，运行`mvn -Dtest=AlluxioFSTest#createFileTest -DfailIfNoTests=false test`。

-   要以交互的方式快速运行某些API测试，你可能需要使用Scala shell，这在[blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding)有讨论。

-   使用不同的Hadoop版本进行测试，运行``mvn -Dhadoop.version=2.2.0 clean test``。

-   要进行Hadoop文件系统的契约式设计测试（用hadoop 2.6.0），运行：`mvn -PcontractTest clean test`。

#### 编码风格

-   请遵循已有代码的风格。具体地，我们使用[Google Java style](https://google.github.io/styleguide/javaguide.html)风格，但有以下不同：
    -  每行最多**100**个字符
    -  第三方导入被整理到一起以使得IDE格式化起来更简单
    -  类成员变量要使用`m`前缀，例如`private WorkerClient mWorkerClient;`
    -  静态成员变量要使用`s`前缀，例如`public static String sUnderFSAddress;`
-   你可以下载我们提供的[Eclipse formatter](../resources/alluxio-code-formatter-eclipse.xml)
    -  为了让Eclipse能够正确地组织你的导入, 配置"组织导入"以看上去像
       [这样](../resources/eclipse_imports.png)
    -  如果你使用IntelliJ IDEA:
       - 你可以使用我们提供的formatter，参考[Eclipse Code Formatter](https://github.com/krasa/EclipseCodeFormatter#instructions)，或者在IntelliJ
       IDEA中使用[Eclipse Code Formatter Plugin](http://plugins.jetbrains.com/plugin/6546)
       - 要自动格式化**import**，在Preferences->Code Style->Java->Imports->Import中设置Layout为[该顺序](../resources/importorder.png)
       - 要自动将方法按字母顺序重新排序，可以使用[Rearranger插件](http://plugins.jetbrains.com/plugin/173)，打开Preferences，查找rearrager，去除不必要的内容，然后右击，选择"Rearrange"，代码将格式化成你需要的风格。
-   Alluxio使用SLF4J记录日志，典型用法为：

{% include Contributing-to-Alluxio/slf4j.md %}

-  为验证编码风格符合标准，你在提交pull request之前应该先运行[checkstyle](http://checkstyle.sourceforge.net)，并且保证没有警告：

{% include Contributing-to-Alluxio/checkstyle.md %}

#### FindBugs

在提交pull request之前，对最新的代码运行
[FindBugs](http://findbugs.sourceforge.net/)确保不出现警告：

{% include Contributing-to-Alluxio/findbugs.md %}

### IDE

你可以通过运行以下命令生成Eclipse配置文件：

{% include Contributing-to-Alluxio/eclipse-configuration.md %}

然后将该文件夹导入到Eclipse中。

也可以运行以下命令将M2_REPO添加到classpath变量中：

{% include Contributing-to-Alluxio/M2_REPO.md %}

如果你使用的是IntelliJ IDEA，你可能需要修改Maven profile配置中的'developer'以防止导入错误，可以通过以下方式进行：

    View > Tool Windows > Maven Projects
