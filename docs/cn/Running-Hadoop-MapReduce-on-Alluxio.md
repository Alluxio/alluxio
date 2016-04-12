---
layout: global
title: 在Alluxio上运行Hadoop MapReduce
nickname: Apache Hadoop MapReduce
group: Frameworks
priority: 1
---

该文档介绍如何让Alluxio与Apache Hadoop MapReduce一起运行，从而让你轻松地运行文件存储在Alluxio上的MapReduce程序。

# 初始化设置

这部分的先决条件是你已安装了[Java](Java-Setup.html)。我们也假设你已经按照文档[本地模式](Running-Alluxio-Locally.html)或[集群](Running-Alluxio-on-a-Cluster.html)安装了Alluxio和Hadoop。
为了运行一些简单的map-reduce实例，我们也推荐你下载[map-reduce examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-examples/2.4.1)，或者如果你正在使用Hadoop 1，下载这个[examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-examples/1.2.1)。

# 编译Alluxio客户端

为了使Alluxio和你的Hadoop版本相对应，你必须重新编译Alluxio Client的Jar包，指明你的Hadoop版本。你可以在Alluxio目录下运行如下命令：

{% include Running-Hadoop-MapReduce-on-Alluxio/compile-Alluxio-Hadoop.md %}

`<YOUR_HADOOP_VERSION>`版本支持很多不同的Hadoop发行版。例如：`mvn install -Dhadoop.version=2.7.1 -DskipTests`将会编译出适合Apache Hadoop 2.7.1版本的Alluxio。 
请访问[构建Alluxio主分支](Building-Alluxio-Master-Branch.html#distro-support)页面来获取其他发行版本的支持信息。

编译成功后，新的Alluxio客户端Jar包可以在如下目录中找到：

    core/client/target/alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar

文档后续部分将会用到这个jar文件。

# 配置Hadoop

首先, 确保在Hadoop的安装目录下的`conf`目录中有`core-site.xml`文件，并且已经加入了如下属性：

{% include Running-Hadoop-MapReduce-on-Alluxio/config-core-site.md %}

该配置让你的MapReduce作业可以使用Alluxio来输入输出文件。如果你正在使用HDFS作为Alluxio的底层存储系统，同样有必要在`hdfs-site.xml`文件中添加这些属性：

其次, 为了让JobClient可以访问Alluxio客户端Jar文件，你可以在`hadoop-env.sh`文件中将`HADOOP_CLASSPATH`修改为:

{% include Running-Hadoop-MapReduce-on-Alluxio/config-hadoop.md %}

该配置让代码可以使用Alluxio的URI来创建和提交作业。

# 分发Alluxio客户端Jar包

为了让MapRedude作业可以在Alluxio上读写文件，Alluxio客户端Jar包必须被分发到集群的所有节点上。这使得TaskTracker和JobClient包含所有与Alluxio进行交互访问所需要的可执行文件。

[如何从Cloudera上加入第三方库](http://blog.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/)这篇文档介绍了分发Jar包的多种方式。文档中建议通过使用命令行的`-libjars`选项，使用分布式缓存来分发Alluxio客户端Jar包。另一种分发客户端Jar包的方式就是手动将其分发到Hadoop节点上。下面就是这两种主流方法的介绍：

1.**使用-libjars命令行选项**
你可以在使用`hadoop jar ...`的时候加入-libjars命令行选项，指定`/<PATH_TO_ALLUXIO>/core/client/target/alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`为参数。这条命令会把该Jar包放到Hadoop的DistributedCache中，使所有节点都可以访问到。例如，下面的命令就是将Alluxio客户端Jar包添加到`-libjars`选项中。

{% include Running-Hadoop-MapReduce-on-Alluxio/add-jar-libjars.md %}

2.**手动将Jar包分发到所有节点**
为了在每个节点上安装Alluxio，你必须将客户端Jar包`alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`（位于`/<PATH_TO_ALLUXIO>/core/client/target/`目录）放到每个MapReduce节点的`$HADOOP_HOME/lib`（由于版本不同也可能是`$HADOOP_HOME/share/hadoop/common/lib`）目录下，然后重新启动所有的TaskTracker。该方法要注意的是所有Jar包必须再次安装，因为每个Jar包都更新到了最新版本。另一方面，当该Jar包已经在每个节点上的时候，就没有必要使用`-libjars`命令行选项了。

# 在本地模式的Alluxio上运行Hadoop wordcount

首先，编译相应Hadoop版本的Alluxio：

{% include Running-Hadoop-MapReduce-on-Alluxio/compile-Alluxio-Hadoop-test.md %}

为了方便，我们假设是伪分布式的集群，通过运行如下命令启动：

{% include Running-Hadoop-MapReduce-on-Alluxio/start-cluster.md %}

配置Alluxio，将本地HDFS集群作为其底层存储系统。你需要修改`conf/alluxio-env.sh`，加入如下语句：

{% include Running-Hadoop-MapReduce-on-Alluxio/config-Alluxio.md %}

以本地模式启动Alluxio：

{% include Running-Hadoop-MapReduce-on-Alluxio/start-Alluxio.md %}

你可以在Alluxio中加入两个简单的文件来运行wordcount。在你的Alluxio目录中运行：

{% include Running-Hadoop-MapReduce-on-Alluxio/copy-from-local.md %}

该命令将`LICENSE`文件复制到Alluxio的文件命名空间中，并指定其路径为`/wordcount/input.txt`。

现在我们运行一个用于wordcount的MapReduce作业。

{% include Running-Hadoop-MapReduce-on-Alluxio/run-wordcount.md %}

作业完成后，wordcount的结果将存在Alluxio的`/wordcount/output`目录下。你可以通过运行如下命令来查看结果文件：

{% include Running-Hadoop-MapReduce-on-Alluxio/cat-result.md %}
