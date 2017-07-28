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

要在许多机器上运行Alluxio集群，需要在这些机器上部署二进制包。
你可以或者[下载二进制包](http://www.alluxio.org/download)和对应版本的Hadoop(推荐)，
或者[从源码编译Alluxio](building-Alluxio-Master-Branch.html)(高级用户)。

注意，当从源码编译Alluxio时，默认预编译的Alluxio服务器二进制包适用于HDFS `2.2.0`。若使用其他版本的Hadoop，需要指定正确的Hadoop配置文件，然后在你的Alluxio目录下运行一下命令：

```bash
$ mvn install -P<YOUR_HADOOP_PROFILE> -DskipTests
```

Alluxio为不同的Hadoop版本提供了预先定义好的配置文件，包括`hadoop-1`, `hadoop-2.2`, `hadoop-2.3` ...`hadoop-2.8`。如果你想将Alluxio和一个具体的Hadoop发行版本一起编译，你可以在命令中指定版本`<YOUR_HADOOP_VERSION>`
比如：

```bash
$ mvn install -Phadoop-2.7 -Dhadoop.version=2.7.1 -DskipTests
```

这会将为Apache Hadoop 2.7.1编译Alluxio。
请访问[Building Alluxio Master Branch](Building-Alluxio-Master-Branch.html#distro-support)以获取更多关于支持其它版本的信息。

如果一切正常，在`assembly/server/target`目录中应当能看到`alluxio-assembly-server-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`文件并且使用该jar文件即可运行Alluxio Master和Worker。

## 配置Alluxio

### 基础配置

首先利用模板创建你的配置文件。

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

接着修改`conf/alluxio-site.properties`文件，将底层存储系统的地址设置为HDFS namenode的地址以及你想挂载到Alluxio的HDFS目录。例如，若你的HDFS namenode是在本地默认端口运行，并且将HDFS根目录映射到Alluxio，则该地址为`hdfs://localhost:9000`，或者，若仅仅映射HDFS的`/alluxio/data`目录到Alluxio，则地址为`hdfs://localhost:9000/alluxio/data`。

```
alluxio.underfs.address=hdfs://NAMENODE:PORT
```

### HDFS配置文件

为了确保Alluxio客户端能够将HDFS配置加入到classpath，请将HDFS安全认证配置文件（`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`）拷贝到`${ALLUXIO_HOME}/conf/`目录下。

### Kerberos配置

可选配置项，你可以为自定义的Kerberos配置设置jvm级别的系统属性：`java.security.krb5.realm`和`java.security.krb5.kdc`。这些Kerberos配置将Java库路由到指定的Kerberos域和KDC服务器地址。如果两者都设置为空，Kerberos库将尊从机器上的默认Kerberos配置。例如：

* 如果你使用的是Hadoop，你可以将这两项配置添加到`{HADOOP_CONF_DIR}/hadoop-env.sh`文件的`HADOOP_OPTS`配置项。

```bash
$ export HADOOP_OPTS="$HADOOP_OPTS -Djava.security.krb5.realm=<YOUR_KERBEROS_REALM> -Djava.security.krb5.kdc=<YOUR_KERBEROS_KDC_ADDRESS>"
```

* 如果你使用的是Spark，你可以将这两项配置添加到`{SPARK_CONF_DIR}/spark-env.sh`文件的`SPARK_JAVA_OPTS`配置项。

```properties
SPARK_JAVA_OPTS+=" -Djava.security.krb5.realm=<YOUR_KERBEROS_REALM> -Djava.security.krb5.kdc=<YOUR_KERBEROS_KDC_ADDRESS>"
```

* 如果你使用的是Alluxio Shell，你可以将这两项配置添加到`conf/alluxio-env.sh`文件的`ALLUXIO_JAVA_OPTS`配置项。

```properties
ALLUXIO_JAVA_OPTS+=" -Djava.security.krb5.realm=<YOUR_KERBEROS_REALM> -Djava.security.krb5.kdc=<YOUR_KERBEROS_KDC_ADDRESS>"
```

### Alluxio服务器Kerberos认证

在`alluxio-site.properties`文件配置下面的Alluxio属性：

```properties
alluxio.master.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.master.principal=hdfs/<_HOST>@<REALM>
alluxio.worker.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.worker.principal=hdfs/<_HOST>@<REALM>
```

或者，这些配置项可以在`conf/alluxio-env.sh`文件中设置。更多有关配置参数的设置可以参考[Configuration Settings](Configuration-Settings.html)。

## 使用安全认证模式下的HDFS在本地运行Alluxio

在这一步开始之前，请确保你的HDFS集群处于运行状态，并且挂载到Alluxio的HDFS目录已经存在。

在Alluxio节点运行`kinit`的时候，请使用相应master/worker上的principal和keytab文件来提供Kerberos票据缓存。一个已知的限制是Kerberos TGT可能会在达到最大更新周期后失效。你可以通过定期更新TGT来解决这个问题。否则，在启动Alluxio服务的时候，你可能会看到下面的错误：

```
javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)]
```

配置完成后，你可以在本地启动Alluxio，观察是否正确运行：

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

```bash
$ bin/alluxio runTests
```

运行成功后，访问HDFS Web UI [http://localhost:50070](http://localhost:50070)，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像这样：`/default_tests_files/Basic_CACHE_THROUGH`。

运行以下命令停止Alluxio：

```bash
$ bin/alluxio-stop.sh local
```

