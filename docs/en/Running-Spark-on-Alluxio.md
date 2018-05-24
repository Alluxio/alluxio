---
layout: global
title: Running Spark on Alluxio
nickname: Apache Spark
group: Frameworks
priority: 0
---

* Table of Contents
{:toc}

This guide describes how to run [Apache Spark](http://spark-project.org/) on Alluxio. HDFS is used
as an example of a distributed under storage system. Note that, Alluxio supports many other under
storage systems in addition to HDFS and enables frameworks like Spark to read data from or write
data to any number of those systems.

## Compatibility

Alluxio works together with Spark 1.1 or later out-of-the-box.

## Prerequisites

### General Setup

* Alluxio cluster has been set up in accordance to these guides for either
[Local Mode](Running-Alluxio-Locally.html) or [Cluster Mode](Running-Alluxio-on-a-Cluster.html).

* We recommend you to download the tarball from
  Alluxio [download page](http://www.alluxio.org/download).
  Alternatively, advanced users can choose to compile this client jar from the source code
  by following Follow the instructs [here](Building-Alluxio-Master-Branch.html#compute-framework-support).
  The Alluxio client jar can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}`.

* In order for Spark applications to read and write files in Alluxio, the Alluxio client jar must be distributed
  on the classpath of the application across different nodes
  (each node must have the client jar on the same local path {{site.ALLUXIO_CLIENT_JAR_PATH}}).

* Add the following line to `spark/conf/spark-defaults.conf`.

```bash
spark.driver.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
spark.executor.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
```

### Additional Setup for HDFS

* If Alluxio is run on top of a Hadoop 1.x cluster, add the following content to `${SPARK_HOME}/conf/core-site.xml`:

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```

* If you are running Alluxio in fault tolerant mode with zookeeper,
add the following line to `${SPARK_HOME}/conf/spark-defaults.conf`:

```bash
spark.driver.extraJavaOptions -Dalluxio.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181 -Dalluxio.zookeeper.enabled=true
spark.executor.extraJavaOptions -Dalluxio.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181 -Dalluxio.zookeeper.enabled=true
```
Alternatively you can add the properties to the previously created Hadoop configuration file `${SPARK_HOME}/conf/core-site.xml`:

```xml
<configuration>
  <property>
    <name>alluxio.zookeeper.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>alluxio.zookeeper.address</name>
    <value>[zookeeper_hostname]:2181</value>
  </property>
</configuration>
```

## Check Spark with Alluxio integration (Supports Spark 2.X)

Before running Spark on Alluxio, you might want to make sure that your Spark configuration has been
setup correctly for integrating with Alluxio. The Spark integration checker can help you achieve this.

When you have a running Spark cluster (or Spark standalone), you can run the following command in the Alluxio project directory:

```bash
$ integration/checker/bin/alluxio-checker.sh spark <spark master uri> [partition number]
```

Here `partition number` is optional.
You can use `-h` to display helpful information about the command.
This command will report potential problems that might prevent you from running Spark on Alluxio.

## Use Alluxio as Input and Output

This section shows how to use Alluxio as input and output sources for your Spark applications.

### Use Data Already in Alluxio

First, we will copy some local data to the Alluxio file system. Put the file `LICENSE` into Alluxio,
assuming you are in the Alluxio project directory:

```bash
$ bin/alluxio fs copyFromLocal LICENSE /LICENSE
```

Run the following commands from `spark-shell`, assuming Alluxio Master is running on `localhost`:

```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/LICENSE2")
```

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There
should be an output `LICENSE2` containing the doubled content of `LICENSE`.

### Use Data from HDFS

Alluxio supports transparently fetching the data from the under storage system, given the exact
path. Put a file `LICENSE_HDFS` into HDFS under the folder Alluxio is mounted to.

Assuming the root UFS address (`alluxio.master.mount.table.root.ufs`) is `hdfs://localhost:9000/alluxio/`, run

```bash
$ hdfs dfs -put -f ${ALLUXIO_HOME}/LICENSE hdfs://localhost:9000/alluxio/LICENSE_HDFS
```

Note that Alluxio has no notion of the file. You can verify this by going to the web UI. Run the
following commands from `spark-shell`, assuming Alluxio Master is running on `localhost`:

```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE_HDFS")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/LICENSE_HDFS2")
```

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There
should be an output `LICENSE_HDFS2` which doubles each line in the file `LICENSE_HDFS`. Also, the
`LICENSE_HDFS` file now appears in the Alluxio file system space.

> NOTE: Block caching on partial reads is enabled by default, but if you have turned off the option,
> it is possible that the `LICENSE_HDFS` file is not in Alluxio storage. This is
> because Alluxio only stores fully read blocks, and if the file is too small, the Spark job will
> have each executor read a partial block. To avoid this behavior, you can specify the partition
> count in Spark. For this example, we would set it to 1 as there is only 1 block.

```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE_HDFS", 1)
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/LICENSE_HDFS2")
```

### Using Fault Tolerant Mode

When running Alluxio with fault tolerant mode, you can point to any Alluxio master:

```scala
> val s = sc.textFile("alluxio://standbyHost:19998/LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://activeHost:19998/LICENSE2")
```

## Data Locality

If Spark task locality is `ANY` while it should be `NODE_LOCAL`, it is probably because Alluxio and
Spark use different network address representations, maybe one of them uses hostname while
another uses IP address. Please refer to [this jira ticket](
https://issues.apache.org/jira/browse/SPARK-10149) for more details, where you can find solutions
from the Spark community.

Note: Alluxio uses hostname to represent network address except in version 0.7.1 where IP address is
used. Spark v1.5.x ships with Alluxio v0.7.1 by default, in this case, by default, Spark and Alluxio
both use IP address to represent network address, so data locality should work out of the box.
But since release 0.8.0, to be consistent with HDFS, Alluxio represents network address by hostname.
There is a workaround when launching Spark to achieve data locality. Users can explicitly specify
hostnames by using the following script offered in Spark. Start Spark Worker in each slave node with
slave-hostname:

```bash
$ ${SPARK_HOME}/sbin/start-slave.sh -h <slave-hostname> <spark master uri>
```

For example:

```bash
$ ${SPARK_HOME}/sbin/start-slave.sh -h simple30 spark://simple27:7077
```

You can also set the `SPARK_LOCAL_HOSTNAME` in `$SPARK_HOME/conf/spark-env.sh` to achieve this. For
example:

```properties
SPARK_LOCAL_HOSTNAME=simple30
```

In either way, the Spark Worker addresses become hostnames and Locality Level becomes NODE_LOCAL as shown
in Spark WebUI below.

![hostname]({{site.data.img.screenshot_datalocality_sparkwebui}})

![locality]({{site.data.img.screenshot_datalocality_tasklocality}})

### Running Spark on YARN

To maximize the amount of locality your Spark jobs attain, you should use as many
executors as possible, hopefully at least one executor per node.
As with all methods of Alluxio deployment, there should also be an Alluxio worker on all computation nodes.

When a Spark job is run on YARN, Spark launches its executors without taking data locality into account.
Spark will then correctly take data locality into account when deciding how to distribute tasks to its
executors. For example, if `host1` contains `blockA` and a job using `blockA` is launched on the YARN
cluster with `--num-executors=1`, Spark might place the only executor on `host2` and have poor locality.
However, if `--num-executors=2` and executors are started on `host1` and `host2`, Spark will be smart
enough to prioritize placing the job on `host1`.

## `Class alluxio.hadoop.FileSystem not found` Issues with SparkSQL and Hive MetaStore

To run the `spark-shell` with the Alluxio client, the Alluxio client jar will have to be added to the classpath of the
Spark driver and Spark executors, as [described earlier](Running-Spark-on-Alluxio.html#general-setup).
However, sometimes SparkSQL may fail to save tables to the Hive MetaStore (location in Alluxio), with an error
message similar to the following:

```
org.apache.hadoop.hive.ql.metadata.HiveException: MetaException(message:java.lang.RuntimeException: java.lang.ClassNotFoundException: Class alluxio.hadoop.FileSystem not found)
```

The recommended solution is to configure
[`spark.sql.hive.metastore.sharedPrefixes`](http://spark.apache.org/docs/2.0.0/sql-programming-guide.html#interacting-with-different-versions-of-hive-metastore).
In Spark 1.4.0 and later, Spark uses an isolated classloader to load java classes for accessing the Hive MetaStore.
However, the isolated classloader ignores certain packages and allows the main classloader to load "shared" classes
(the Hadoop HDFS client is one of these "shared" classes). The Alluxio client should also be loaded by the main
classloader, and you can append the `alluxio` package to the configuration parameter
`spark.sql.hive.metastore.sharedPrefixes` to inform Spark to load Alluxio with the main classloader. For example, the
parameter may be set to:

```bash
spark.sql.hive.metastore.sharedPrefixes=com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,alluxio
```

## `java.io.IOException: No FileSystem for scheme: alluxio` Issue with Spark on YARN

If you use Spark on YARN with Alluxio and run into the exception `java.io.IOException: No FileSystem for scheme: alluxio`, please add the following content to `${SPARK_HOME}/conf/core-site.xml`:

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```
