---
layout: global
title: Running Spark on Alluxio
nickname: Apache Spark
group: Frameworks
priority: 0
---

* Table of Contents
{:toc}

This guide describes how to configure [Apache Spark](http://spark-project.org/) to access Alluxio.

## Overview

For Spark 1.1 or later, applications can access files in an Alluxio cluster through its
HDFS-compatible interface out-of-the-box.
Using Alluxio as the data access layer, Spark applications can transparently access
data in many different types and instances of persistent
storage services (e.g., AWS S3 buckets, Azure Object Store buckets, remote HDFS deployments
and etc). Data can be actively fetched or transparently cached into Alluxio to speed up
the I/O performance especially when Spark deployment is remote to data.
In addition, Alluxio can help simplify the architecture by decoupling
compute and persistent storage because when the real data path in persistent under storage is
hidden to Spark, the change of under storages can be independent from application logic, meanwhile
as a near-compute cache Alluxio can still provide compute frameworks like Spark data-locality.

## Prerequisites

* An Alluxio cluster has been set up and is running according to these guides for either
[Local Mode](Running-Alluxio-Locally.html) or [Cluster Mode](Running-Alluxio-on-a-Cluster.html).
This guide assumes the persistent under storage is a local HDFS
deployment. E.g., a line of
`alluxio.underfs.address= hdfs://localhost:9000/alluxio/` is included in `${ALLUXIO_HOME}/conf/alluxio-site.properties`.
Note that, Alluxio supports many other under
storage systems in addition to HDFS. To access data from any number of those systems is orthogonal
to this guide but covered by [Unified and Transparent Namespace](Unified-and-Transparent-Namespace.html).

* Make sure that the Alluxio client jar is available.
  This Alluxio client jar file can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the tarball
  downloaded from Alluxio [download page](http://www.alluxio.org/download).
  Alternatively, advanced users can choose to compile this client jar from the source code
  by following the [instructions](Building-Alluxio-From-Source.html).

## Configure Spark

### Basic Setup

Step 1, distribute the Alluxio client jar across the nodes where Spark drivers or executors are
running.
    In order for Spark applications to read and write files in Alluxio,
  on each node, put the client jar on the same local path (e.g. `{{site.ALLUXIO_CLIENT_JAR_PATH}}`).

Step 2, add the Alluxio client jar to the classpath of Spark.
Specifically, add the following line to `spark/conf/spark-defaults.conf`.

```bash
spark.driver.extraClassPath   {{site.ALLUXIO_CLIENT_JAR_PATH}}
spark.executor.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
```

### Additional Setup for Alluxio with HA

If you are running Alluxio in fault tolerant mode with Zookeeper running at nodes
`zkHost1:2181` and `zkHost2:2181`,
add the following lines to `${SPARK_HOME}/conf/spark-defaults.conf`:

```bash
spark.driver.extraJavaOptions   -Dalluxio.zookeeper.address=zkHost1:2181,zkHost2:2181 -Dalluxio.zookeeper.enabled=true
spark.executor.extraJavaOptions -Dalluxio.zookeeper.address=zkHost1:2181,zkHost2:2181 -Dalluxio.zookeeper.enabled=true
```

Alternatively you can add the properties to the Hadoop configuration file
`${SPARK_HOME}/conf/core-site.xml`:

```xml
<configuration>
  <property>
    <name>alluxio.zookeeper.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>alluxio.zookeeper.address</name>
    <value>zkHost1:2181,zkHost2:2181</value>
  </property>
</configuration>
```

### Check Spark is Correctly Set-up

To ensure that your Spark configuration has been setup correctly to work with Alluxio
before running Spark, a tool that comes with Alluxio v1.8 can help.

When you have a running Spark cluster (or Spark standalone) of version 2.x, you can run the
following command in the Alluxio project directory:

```bash
$ integration/checker/bin/alluxio-checker.sh spark <spark master uri> [partition number]
```
where `partition number` is optional. For example,

```bash
$ integration/checker/bin/alluxio-checker.sh spark spark://sparkMaster:7077
```

This command will report potential problems that might prevent you from running Spark on Alluxio.

You can use `-h` to display helpful information about the command.

## Examples: Use Alluxio as Input and Output

This section shows how to use Alluxio as input and output sources for your Spark applications.

### Access Data Only in Alluxio

First, we will copy some local data to the Alluxio file system. Put the file `LICENSE` into Alluxio,
assuming you are in the Alluxio project directory:

```bash
$ bin/alluxio fs copyFromLocal LICENSE /LICENSE
```

Run the following commands from `spark-shell`, assuming Alluxio Master is running on `localhost`:

```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/Output")
```

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There
should be directory `/Output` containing files of the doubled content of `LICENSE`.

### Access Data in Under Storage

Alluxio supports transparently fetching the data from the under storage system, given the exact
path. For this section, HDFS is used
as an example of a distributed under storage system.

Put a file `LICENSE_HDFS` into HDFS:

```bash
$ hdfs dfs -put -f ${ALLUXIO_HOME}/LICENSE hdfs://localhost:9000/alluxio/LICENSE_HDFS
```

Note that Alluxio has no notion of the file. You can verify this by going to the web UI. Run the
following commands from `spark-shell`, assuming Alluxio Master is running on `localhost`:

```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE_HDFS")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/Output_HDFS2")
```

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There
should be an output `LICENSE_HDFS2` which doubles each line in the file `LICENSE_HDFS`. Also, the
`LICENSE_HDFS` file now appears in the Alluxio file system space.

### Access Data from Alluxio in HA Mode

When Soark is correctly set up on [Alluxio with HA](#additional-setup-for-alluxio-with-ha),
you can refer to Alluxio authority in the URI without specifying an Alluxio master. This is because
the address of primary Alluxio master is now served by the configured Zookeeper service.

```scala
> val s = sc.textFile("alluxio:///LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio:///Output")
```

## Advanced Usage

### Customize Alluxio Setting for Spark Jobs

To customize Alluxio client-side properties in a Spark job, see
[how to configure Spark Jobs](Configuration-Settings.html#spark-jobs).

### Cache RDD into Alluxio

See the blog article
"[Effective Spark RDDs with Alluxio](https://www.alluxio.com/blog/effective-spark-rdds-with-alluxio)".

### Cache Dataframe into Alluxio

See the blog article
"[Effective Spark DataFrames with Alluxio](https://www.alluxio.com/blog/effective-spark-dataframes-with-alluxio)".

## Common Issues

### Data Locality of Spark Tasks

If Spark task locality is `ANY` while it should be `NODE_LOCAL`, it is probably because Alluxio and
Spark use different network address representations, maybe one of them uses hostname while
another uses IP address. Please refer to [this JIRA ticket](
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

### Data Locality of Spark Jobs on YARN

To maximize the amount of locality your Spark jobs attain, you should use as many
executors as possible, hopefully at least one executor per node.
As with all methods of Alluxio deployment, there should also be an Alluxio worker on all computation nodes.

When a Spark job is run on YARN, Spark launches its executors without taking data locality into account.
Spark will then correctly take data locality into account when deciding how to distribute tasks to its
executors. For example, if `host1` contains `blockA` and a job using `blockA` is launched on the YARN
cluster with `--num-executors=1`, Spark might place the only executor on `host2` and have poor locality.
However, if `--num-executors=2` and executors are started on `host1` and `host2`, Spark will be smart
enough to prioritize placing the job on `host1`.

### `Class alluxio.hadoop.FileSystem not found` Issues with SparkSQL and Hive MetaStore

To run the `spark-shell` with the Alluxio client, the Alluxio client jar will have to be added to the classpath of the
Spark driver and Spark executors, as [described earlier](Running-Spark-on-Alluxio.html#configure-spark).
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
parameter may be set to in `spark/conf/spark-defaults.conf`:

```bash
spark.sql.hive.metastore.sharedPrefixes=com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,alluxio
```

### `java.io.IOException: No FileSystem for scheme: alluxio` Issue with Spark on YARN

If you use Spark on YARN with Alluxio and run into the exception `java.io.IOException: No FileSystem for scheme: alluxio`, please add the following content to `${SPARK_HOME}/conf/core-site.xml`:

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```
