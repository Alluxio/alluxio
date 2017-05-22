---
layout: global
title: 在HDFS安全认证模式上配置Alluxio
nickname: Alluxio使用secure HDFS
group: Under Store
priority: 3
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio从而使用[安全认证模式下的HDFS](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SecureMode.html)作为底层文件系统。Alluxio支持安全认证模式下的HDFS作为底层文件系统，通过[Kerberos](http://web.mit.edu/kerberos/)认证。

注：在HDFS安全认证模式下的Kerberos认证不是Alluxio通过Kerberos进行内部的认证

## 初始步骤

要在许多机器上运行Alluxio集群，需要在这些机器上部署二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

注意，在默认情况下，预编译的Alluxio二进制包适用于HDFS `2.2.0`，若使用其他版本的Hadoop，需要从Alluxio源代码重新编译，且编译时应照以下方法中的一种设置Hadoop版本号。假定Alluxio源代码的根目录为`${ALLUXIO_HOME}`。

* 修改`${ALLUXIO_HOME}/pom.xml`配置文件中的`hadoop.version`标签。例如，若使用Hadoop `2.6.0`，将该pom文件中的"`<hadoop.version>2.2.0</hadoop.version>`"修改为"`<hadoop.version>2.6.0</hadoop.version>`"，接着使用maven重新编译：

{% include Configuring-Alluxio-with-HDFS/mvn-package.md %}

* 另外，也可以选择使用maven编译时在命令行中指定对应的Hadoop版本号，例如，若使用Hadoop HDFS `2.6.0`：

{% include Configuring-Alluxio-with-HDFS/mvn-Dhadoop-package.md %}

如果一切正常，在`assembly/server/target`目录中应当能看到`alluxio-assembly-server-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`文件。使用该jar文件即可运行Alluxio Master和Worker。

## 配置Alluxio

要运行二进制包，一定要先创建配置文件，你可以使用`bootstrapConf` 命令来创建自己的配置文件。
举个例子，假如你正在本地运行Alluxio，那么就应该把`ALLUXIO_MASTER_HOSTNAME`设置为`localhost`

{% include Configuring-Alluxio-with-HDFS/bootstrapConf.md %}

另外你也可以从template文件创建一个配置文件然后手动修改它的内容。

{% include Common-Commands/copy-alluxio-env.md %}

接着修改`conf/alluxio-site.properties`文件，将底层存储系统的地址设置为HDFS namenode的地址以及你想挂载到Alluxio的HDFS目录。例如，若你的HDFS namenode是在本地默认端口运行，并且将HDFS根目录挂载到Alluxio，则该地址为`hdfs://localhost:9000`，或者，若仅仅挂载HDFS的`/alluxio/data`目录到Alluxio，则地址为`hdfs://localhost:9000/alluxio/data`。

{% include Configuring-Alluxio-with-HDFS/underfs-address.md %}

### HDFS配置文件
为了确保Alluxio客户端能够将HDFS配置加入到classpath，请将HDFS安全认证配置文件（`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`）拷贝到`${ALLUXIO_HOME}/conf/`目录下。

### Kerberos配置
可选配置项，你可以为自定义的Kerberos配置设置jvm级别的系统属性：`java.security.krb5.realm`和`java.security.krb5.kdc`。这些Kerberos配置将Java库路由到指定的Kerberos域和KDC服务器地址。如果两者都设置为空，Kerberos库将尊从机器上的默认Kerberos配置。例如：

* 如果你使用的是Hadoop，你可以将这两项配置添加到`{HADOOP_CONF_DIR}/hadoop-env.sh`文件的`HADOOP_OPTS`配置项。

{% include Configuring-Alluxio-with-secure-HDFS/hadoop-opts.md %}

* 如果你使用的是Spark，你可以将这两项配置添加到`{SPARK_CONF_DIR}/spark-env.sh`文件的`SPARK_JAVA_OPTS`配置项。

{% include Configuring-Alluxio-with-secure-HDFS/spark-opts.md %}

* 如果你使用的是Alluxio Shell，你可以将这两项配置添加到`conf/alluxio-env.sh`文件的`ALLUXIO_JAVA_OPTS`配置项。

{% include Configuring-Alluxio-with-secure-HDFS/alluxio-opts.md %}

### Alluxio服务器Kerberos认证
在`alluxio-site.properties`文件配置下面的Alluxio属性：

{% include Configuring-Alluxio-with-secure-HDFS/alluxio-properties-for-secure-hdfs-kerberos.md%}

或者，这些配置项可以在`conf/alluxio-env.sh`文件中设置。更多有关配置参数的设置可以参考[Configuration Settings](Configuration-Settings.html)。

## 使用安全认证模式下的HDFS在本地运行Alluxio

在这一步开始之前，请确保你的HDFS集群处于运行状态，并且挂载到Alluxio的HDFS目录已经存在。

在Alluxio节点运行`kinit`的时候，请使用相应master/worker上的principal和keytab文件来提供Kerberos票据缓存。一个已知的限制是Kerberos TGT可能会在达到最大更新周期后失效。你可以通过定期更新TGT来解决这个问题。否则，在启动Alluxio服务的时候，你可能会看到下面的错误：

```
javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)]
```

配置完成后，你可以在本地启动Alluxio，观察是否正确运行：

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问HDFS Web UI [http://localhost:50070](http://localhost:50070)，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像这样：`/default_tests_files/Basic_CACHE_THROUGH`。

运行以下命令停止Alluxio：

{% include Common-Commands/stop-alluxio.md %}
