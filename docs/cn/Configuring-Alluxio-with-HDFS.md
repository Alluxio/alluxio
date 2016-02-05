---
layout: global
title: 在HDFS上配置Alluxio
nickname: Alluxio使用HDFS
group: Under Store
priority: 3
---

该向导介绍如何配置Alluxio从而使用[HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)作为底层文件系统。

# 初始步骤

要在许多机器上运行Alluxio集群，需要在这些机器上部署二进制文件。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

注意，在默认情况下，预编译的Alluxio二进制包适用于HDFS `2.2.0`，若使用其他版本的Hadoop，需要从Alluxio源代码重新编译，且编译时应照以下方法中的一种设置Hadoop版本号。假定Alluxio源代码的根目录为`${TACHYON_HOME}`。

* 修改`${TACHYON_HOME}/pom.xml`配置文件中的`hadoop.version`标签。例如，若使用Hadoop `2.6.0`，将该pom文件中的"`<hadoop.version>2.2.0</hadoop.version>`"修改为"`<hadoop.version>2.6.0</hadoop.version>`"，接着使用maven重新编译：

{% include Configuring-Alluxio-with-HDFS/mvn-package.md %}

* 另外，也可以选择使用maven编译时在命令行中指定对应的Hadoop版本号，例如，若使用Hadoop HDFS `2.6.0`：

{% include Configuring-Alluxio-with-HDFS/mvn-Dhadoop-package.md %}

如果一切正常，在`assembly/target`目录中应当能看到`tachyon-assemblies-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar`文件，使用该jar文件即可运行Alluxio Master和Worker。

# 配置Alluxio

要运行二进制包，一定要先创建配置文件，从template文件创建一个配置文件：

{% include Configuring-Alluxio-with-HDFS/copy-tachyon-env.md %}

接着修改`tachyon-env.sh`文件，将底层存储系统的地址设置为HDFS namenode的地址（例如，若你的HDFS namenode是在本地默认端口运行，则该地址为`hdfs://localhost:9000`）。

{% include Configuring-Alluxio-with-HDFS/underfs-address.md %}

# 使用HDFS在本地运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察是否正确运行：

{% include Configuring-Alluxio-with-HDFS/start-tachyon.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Configuring-Alluxio-with-HDFS/runTests.md %}

运行成功后，访问HDFS Web UI [http://localhost:50070](http://localhost:50070)，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像这样：`/tachyon/data/default_tests_files/BasicFile_STORE_SYNC_PERSIST`。

运行以下命令停止Alluxio：

{% include Configuring-Alluxio-with-HDFS/stop-tachyon.md %}
