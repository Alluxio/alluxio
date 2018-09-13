---
layout: global
title: Running Spark on Alluxio
nickname: Apache Spark
group: Compute Applications
priority: 0
---

* Table of Contents
{:toc}

This guide describes how to configure [Apache Spark](http://spark-project.org/) to access Alluxio.

## Overview

Applications of Spark 1.1 or later can access an Alluxio cluster through its
HDFS-compatible interface out-of-the-box.
Using Alluxio as the data access layer, Spark applications can transparently access
data in many different types and instances of persistent
storage services (e.g., AWS S3 buckets, Azure Object Store buckets, remote HDFS deployments
and etc). Data can be actively fetched or transparently cached into Alluxio to speed up
the I/O performance especially when Spark deployment is remote to data.
In addition, Alluxio can help simplify the architecture by decoupling
compute and physical storage. When the real data path in persistent under storage is
hidden from Spark, a change to under storages can be independent from application logic; meanwhile
as a near-compute cache Alluxio can still provide compute frameworks like Spark data-locality.

## Prerequisites

* An Alluxio cluster has been set up and is running according to either
[Local Mode](Running-Alluxio-Locally.html) or [Cluster Mode](Running-Alluxio-on-a-Cluster.html).
This guide assumes the persistent under storage is a local HDFS
deployment. E.g., a line of
`alluxio.underfs.address= hdfs://localhost:9000/alluxio/` is included in `${ALLUXIO_HOME}/conf/alluxio-site.properties`.
Note that Alluxio supports many other under
storage systems in addition to HDFS. To access data from any number of those systems is orthogonal
to the focus of this guide but covered by
[Unified and Transparent Namespace](Unified-and-Transparent-Namespace.html).

* Make sure that the Alluxio client jar is available.
  This Alluxio client jar file can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the tarball
  downloaded from Alluxio [download page](http://www.alluxio.org/download).
  Alternatively, advanced users can compile this client jar from the source code
  by following the [instructions](Building-Alluxio-From-Source.html).

## Configure Spark

### Basic Setup

Step 1, distribute the Alluxio client jar across the nodes where Spark drivers or executors are
running. Specifically, put the client jar on the same local path (e.g. `{{site.ALLUXIO_CLIENT_JAR_PATH}}`) on each node.

Step 2, add the Alluxio client jar to the classpath of Spark drivers and executors
in order for Spark applications to use the client jar to read and write files in Alluxio.
Specifically, add the following line to `spark/conf/spark-defaults.conf` on every node running
Spark.

```bash
spark.driver.extraClassPath   {{site.ALLUXIO_CLIENT_JAR_PATH}}
spark.executor.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
```

### Additional Setup for Alluxio with HA

If you are running Alluxio in fault tolerant mode with a Zookeeper service running at
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

After Alluxio 1.8 (not included), users can encode the Zookeeper service address
inside an Alluxio URI (see [details](#access-data-from-alluxio-in-ha-mode)).
In this way, it requires no extra setup for Spark configuration.

### Check Spark is Correctly Set Up

To ensure that your Spark can correctly work with Alluxio
before running Spark, a tool that comes with Alluxio v1.8 can help check the configuration.

When you have a running Spark cluster (or Spark standalone) of version 2.x, you can run the
following command in the Alluxio project directory:

```bash
$ integration/checker/bin/alluxio-checker.sh spark <spark master uri>
```

For example,

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
$ bin/alluxio fs copyFromLocal LICENSE /Input
```

Run the following commands from `spark-shell`, assuming Alluxio Master is running on `localhost`:

```scala
> val s = sc.textFile("alluxio://localhost:19998/Input")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/Output")
```

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There
should be an output directory `/Output` which contains the doubled content of the input
file `Input`.

### Access Data in Under Storage

Alluxio supports transparently fetching the data from the under storage system, given the exact
path. For this section, HDFS is used
as an example of a distributed under storage system.

Put a file `Input_HDFS` into HDFS:

```bash
$ hdfs dfs -put -f ${ALLUXIO_HOME}/LICENSE hdfs://localhost:9000/alluxio/Input_HDFS
```

Note that Alluxio has no notion of the file. You can verify this by going to the web UI. Run the
following commands from `spark-shell`, assuming Alluxio Master is running on `localhost`:

```scala
> val s = sc.textFile("alluxio://localhost:19998/Input_HDFS")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/Output_HDFS")
```

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There
should be an output directory `Output_HDFS` which contains the doubled content of the input file
`Input_HDFS`.
Also, the input file `Input_HDFS` now will be 100% loaded in the Alluxio file system space.

### Access Data from Alluxio in HA Mode

If Spark is set up by the instructions in [Alluxio with HA](#additional-setup-for-alluxio-with-ha),
you can write URIs using the "`alluxio://`" scheme without specifying an Alluxio master in the authority. This is because
in HA mode, the address of primary Alluxio master will be served by the configured Zookeeper
service rather than a user-specified hostname derived from the URI.

```scala
> val s = sc.textFile("alluxio:///Input")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio:///Output")
```

Alternatively, if the Zookeeper address for Alluxio HA is not set in Spark
configuration, one can
specify the address of Zookeeper in the URI in the format of "`zk@zkHost1:2181;zkHost2:2181`":

```scala
> val s = sc.textFile("alluxio://zk@zkHost1:2181;zkHost2:2181/Input")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://zk@zkHost1:2181;zkHost2:2181/Output")
```

This feature of encoding Zookeeper service address into Alluxio URIs is not available in versions
1.8 and earlier.

> Note that you must use semicolons rather than commas to separate different Zookeeper addresses to
refer a URI of Alluxio in HA mode in Spark. Otherwise, the URI will be considered invalid by Spark.
Please refer to the instructions in [HDFS API to connect to Alluxio with high availability](Running-Alluxio-on-a-Cluster.html#hdfs-api).

## Advanced Usage

### Cache RDD into Alluxio

Storing RDDs in Alluxio memory is simply saving the RDD as a file to Alluxio.
Two common ways to save RDDs as files in Alluxio are

1. `saveAsTextFile`: writes the RDD as a text file, where each element is a line in the file,
1. `saveAsObjectFile`: writes the RDD out to a file, by using Java serialization on each element.

The saved RDDs in Alluxio can be read again (from memory) by using `sc.textFile` or
`sc.objectFile` respectively.

```scala
// as text file
> rdd.saveAsTextFile("alluxio://localhost:19998/rdd1")
> rdd = sc.textFile("alluxio://localhost:19998/rdd1")

// as object file
> rdd.saveAsObjectFile("alluxio://localhost:19998/rdd2")
> rdd = sc.objectFile("alluxio://localhost:19998/rdd2")
```

See the blog article
"[Effective Spark RDDs with Alluxio](https://www.alluxio.com/blog/effective-spark-rdds-with-alluxio)".

### Cache Dataframe into Alluxio

Storing Spark DataFrames in Alluxio memory is simply saving the DataFrame as a file to Alluxio.
DataFrames are commonly written as parquet files, with `df.write.parquet()`.
After the parquet is written to Alluxio, it can be read from memory by using `sqlContext.read.parquet()`.

```scala
> df.write.parquet("alluxio://localhost:19998/data.parquet")
> df = sqlContext.read.parquet("alluxio://localhost:19998/data.parquet")
```

See the blog article
"[Effective Spark DataFrames with Alluxio](https://www.alluxio.com/blog/effective-spark-dataframes-with-alluxio)".

### Customize Alluxio User Properties for Spark Jobs

To customize Alluxio client-side properties in a Spark job, see
[how to configure Spark Jobs](Configuration-Settings.html#spark-jobs).

## Frequently Asked Questions

### Incorrect Data Locality Level of Spark Tasks

If Spark task locality is `ANY` while it should be `NODE_LOCAL`, it is probably because Alluxio and
Spark use different network address representations, maybe one of them uses hostname while
another uses IP address. Please refer to JIRA ticket [SPARK-10149](
https://issues.apache.org/jira/browse/SPARK-10149) for more details, where you can find solutions
from the Spark community.

Note: Alluxio workers use hostnames to represent network addresses to be consistent with HDFS.
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

```bash
SPARK_LOCAL_HOSTNAME=simple30
```

In either way, the Spark Worker addresses become hostnames and Locality Level becomes `NODE_LOCAL` as shown
in Spark WebUI below.

![hostname]({{ site.baseurl }}/img/screenshot_datalocality_sparkwebui.png)

![locality]({{ site.baseurl }}/img/screenshot_datalocality_tasklocality.png)

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
parameter may be set in `spark/conf/spark-defaults.conf`:

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
