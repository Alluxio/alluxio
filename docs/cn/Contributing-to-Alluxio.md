---
layout: global
title: 参与Alluxio开发
nickname: 开发者向导
group: Resources
---

* Table of Contents
{:toc}

感谢您对alluxio的兴趣！我们对您贡献的任何的新特性或者修复都表示深深感激。

### Alluxio初始任务

新的开发者可以先进行以下列出的任务，从而能够更加熟悉Alluxio：

1.  [在本地运行Alluxio](Running-Alluxio-Locally.html)

2.  [在集群运行Alluxio](Running-Alluxio-on-a-Cluster.html)

3.  阅读[配置项设置](Configuration-Settings.html)以及[命令行接口](Command-Line-Interface.html)

4.  阅读一段[代码示例](https://github.com/amplab/alluxio/blob/master/examples/src/main/java/alluxio/examples/BasicOperations.java)

5.  [构建Alluxio主分支](Building-Alluxio-Master-Branch.html)

6.  Fork Alluxio Github仓库，并添加一两个单元测试或者javadoc文件，再提交一个pull request。也欢迎你处理我们的[JIRA](https://alluxio.atlassian.net/browse/TACHYON)中的issues。这里是专门为新的开发者准备的一些[任务](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20NewContributor%20AND%20status%20%3D%20OPEN)，每个新开发者最多只能做其中的两个任务，在这之后，尝试去做一些Beginner/Intermediate任务，或者在[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)里询问。可以查看Github向导中的[forking a repo](https://help.github.com/articles/fork-a-repo)和[sending a pull request](https://help.github.com/articles/using-pull-requests)学习如何进行基本Github操作。

### 提交代码

-   我们鼓励你每次尽可能提交小的、单一目的的pull request，因为要合并一个含有许多不相干的特性的大的更改十分困难。

-   我们会追踪[JIRA](https://alluxio.atlassian.net/)里的issues和features，如果你还没有帐号，请先进行注册。

-   打开[JIRA](https://alluxio.atlassian.net/)里的一个ticket，里面详细介绍了需要的修改和该修改的目的。

-   将你的修改作为一个GitHub pull request进行提交，可以查看Github向导中的[forking a repo](https://help.github.com/articles/fork-a-repo)和[sending a pull request](https://help.github.com/articles/using-pull-requests)学习如何进行这些操作。

-   在你提交的pull request的标题中，确保其引用了该JIRA ticket，这将会连接到该指定的ticket。例如：

~~~~~
[TACHYON-100] Implement an awesome new feature
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

#### 测试

-   使用``mvn test``命令运行所有单元测试（会将本地文件系统作为底层文件系统，在HDFS模块中将HDFS 1.0.4作为底层文件系统），``mvn -Dhadoop.version=2.4.0 test``命令将HDFS 2.4.0作为HDFS模块中的底层文件系统

-   要在特定的底层文件系统上运行测试，在对应的子模块目录下运行Maven命令，例如，要运行针对HDFS的测试，在``alluxio/underfs/hdfs``目录下运行``mvn test``。

-   对于GlusterFS环境的单元测试，可以在``alluxio/underfs/glusterfs``目录下运行`mvn -PglusterfsTest -Dhadoop.version=2.3.0 -Dalluxio.underfs.glusterfs.mounts=/vol -Dalluxio.underfs.glusterfs.volumes=testvol test`（将GlusterFS作为底层文件系统，`/vol`为一个有效GlusterFS挂载点）。

-   要进行单个单元测试，运行`mvn -Dtest=AlluxioFSTest#createFileTest -DfailIfNoTests=false test`。

-   要以交互的方式快速运行某些API测试，你可能需要使用Scala shell，这在[blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding)有讨论。

-   使用不同的Hadoop版本进行测试，运行``mvn -Dhadoop.version=2.2.0 clean test``。

-   要进行Hadoop文件系统的合同测试（是用hadoop 2.6.0），运行：`mvn -PcontractTest clean test`。

#### 编码风格

-   请遵循已有代码的风格，我们使用[Google Java style](http://google-styleguide.googlecode.com/svn/trunk/javaguide.html)风格，但有以下不同：
    -  每行最多**100**个字符
    -  导入包应遵循[该顺序](../resources/order.importorder)，每组里面遵循**字母顺序**
    -  使用`i ++`代替`i++`
    -  使用`i + j`代替`i+j`
    -  类和成员的修饰符，应按照Java语言规范推荐的顺序：**public protected private abstract static final transient volatile
    synchronized native strictfp**，然后按照**字母顺序**
    -  类成员变量要使用`m`前缀，例如`public static String sUnderFSAddress;`
    -  静态成员变量要使用`s`前缀，例如`public static String sUnderFSAddress;`
    -  在Java接口中不要对方法使用`public`或`abstract`修饰符，这是因为在接口中声明的方法隐式被认为是public和abstract (http://docs.oracle.com/javase/specs/jls/se7/html/jls-9.html#jls-9.4)
-   你可以下载我们提供的[Eclipse formatter](../resources/alluxio-code-formatter-eclipse.xml)
    -  如果你使用IntelliJ IDEA:
       - 你可以使用我们提供的formatter，参考[Eclipse Code Formatter](https://github.com/krasa/EclipseCodeFormatter#instructions)，或者在IntelliJ
       IDEA中使用[Eclipse Code Formatter Plugin](http://plugins.jetbrains.com/plugin/6546)
       - 要自动格式化**import**，在Preferences->Code Style->Java->Imports->Import中设置Layout为[该顺序](../resources/order.importorder)
       - 要自动将方法按字母顺序重新排序，可以使用[Rearranger插件](http://plugins.jetbrains.com/plugin/173)，打开Preferences，查找rearrager，去除不必要的内容，然后右击，选择"Rearrange"，代码将格式化成你需要的风格。
-   Alluxio使用SLF4J记录日志，典型用法为：

{% include Contributing-to-Alluxio/slf4j.md %}

-  为确保编码风格符合标准，你在提交pull request之前应该先运行[checkstyle](http://checkstyle.sourceforge.net)，保证没有警告：

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

如果你使用的是IntelliJ IDEA，你可能需要修改Maven配置中的'developer'以防止导入错误，可以通过以下方式进行：

    View > Tool Windows > Maven Projects

### 幻灯片介绍

-   AMPCamp 6 (November, 2015)
[SlideShare](http://www.slideshare.net/AlluxioNexus/alluxio-presentation-at-ampcamp-6-november-2015)
-   Strata and Hadoop World 2015 (September, 2015)
[SlideShare](http://www.slideshare.net/AlluxioNexus/alluxio-an-open-source-memorycentric-distributed-storage-system)
-   Strata and Hadoop World 2014 (October, 2014)
[pdf](http://www.cs.berkeley.edu/~haoyuan/talks/Alluxio_2014-10-16-Strata.pdf)
[pptx](http://www.cs.berkeley.edu/~haoyuan/talks/Alluxio_2014-10-16-Strata.pptx)
-   Spark Summit 2014 (July, 2014) [pdf](http://goo.gl/DKrE4M)
-   Strata and Hadoop World 2013 (October, 2013) [pdf](http://goo.gl/AHgz0E)

### 延伸阅读

-   [Alluxio: Reliable, Memory Speed Storage for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2014_socc_alluxio.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Scott Shenker, Ion Stoica, *SOCC 2014*.
-   [Reliable, Memory Speed Storage for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2014_EECS_alluxio.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Scott Shenker, Ion Stoica, *UC Berkeley EECS 2014*.
-   [Alluxio: Memory Throughput I/O for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2013_ladis_alluxio.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Eric Baldeschwieler, Scott Shenker, Ion Stoica, *LADIS 2013*.
