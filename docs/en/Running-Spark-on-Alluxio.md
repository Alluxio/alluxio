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

* Alluxio client will need to be compiled with the Spark specific profile. Build the entire project
from the top level `alluxio` directory with the following command:

{% include Running-Spark-on-Alluxio/spark-profile-build.md %}

* Add the following line to `spark/conf/spark-defaults.conf`.

{% include Running-Spark-on-Alluxio/earlier-spark-version-bash.md %}

### Additional Setup for HDFS

* If Alluxio is run on top of a Hadoop 1.x cluster, create a new file `spark/conf/core-site.xml`
with the following content:

{% include Running-Spark-on-Alluxio/Hadoop-1.x-configuration.md %}

* If you are running alluxio in fault tolerant mode with zookeeper and the Hadoop cluster is a 1.x,
add the following additionally entry to the previously created `spark/conf/core-site.xml`:

{% include Running-Spark-on-Alluxio/fault-tolerant-mode-with-zookeeper-xml.md %}

and the following line to `spark/conf/spark-defaults.conf`:

{% include Running-Spark-on-Alluxio/fault-tolerant-mode-with-zookeeper-bash.md %}

## Use Alluxio as Input and Output

This section shows how to use Alluxio as input and output sources for your Spark applications.

### Use Data Already in Alluxio

First, we will copy some local data to the Alluxio file system. Put the file `LICENSE` into Alluxio,
assuming you are in the Alluxio project directory:

{% include Running-Spark-on-Alluxio/license-local.md %}

Run the following commands from `spark-shell`, assuming Alluxio Master is running on `localhost`:

{% include Running-Spark-on-Alluxio/alluxio-local-in-out-scala.md %}

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There
should be an output file `LICENSE2` which doubles each line in the file `LICENSE`.

### Use Data from HDFS

Alluxio supports transparently fetching the data from the under storage system, given the exact
path. Put a file `LICENSE` into HDFS under the folder Alluxio is mounted to, by default this is
/alluxio, meaning any files in hdfs under this folder will be discoverable by Alluxio. You can
modify this setting by changing the `ALLUXIO_UNDERFS_ADDRESS` property in alluxio-env.sh on the
server.

Assuming the namenode is running on `localhost` and you are using the default mount directory
`/alluxio`:

{% include Running-Spark-on-Alluxio/license-hdfs.md %}

Note that Alluxio has no notion of the file. You can verify this by going to the web UI. Run the
following commands from `spark-shell`, assuming Alluxio Master is running on `localhost`:

{% include Running-Spark-on-Alluxio/alluxio-hdfs-in-out-scala.md %}

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There
should be an output file `LICENSE2` which doubles each line in the file `LICENSE`. Also, the
`LICENSE` file now appears in the Alluxio file system space.

> NOTE: Block caching on partial reads is enabled by default, but if you have turned off the option,
> it is possible that the `LICENSE` file is not in Alluxio storage (Not In-Memory). This is
> because Alluxio only stores fully read blocks, and if the file is too small, the Spark job will
> have each executor read a partial block. To avoid this behavior, you can specify the partition
> count in Spark. For this example, we would set it to 1 as there is only 1 block.

{% include Running-Spark-on-Alluxio/alluxio-one-partition.md %}

### Using Fault Tolerant Mode

When running Alluxio with fault tolerant mode, you can point to any Alluxio master:

{% include Running-Spark-on-Alluxio/any-Alluxio-master.md %}

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

{% include Running-Spark-on-Alluxio/slave-hostname.md %}

For example:

{% include Running-Spark-on-Alluxio/slave-hostname-example.md %}

You can also set the `SPARK_LOCAL_HOSTNAME` in `$SPARK_HOME/conf/spark-env.sh` to achieve this. For
example:

{% include Running-Spark-on-Alluxio/spark-local-hostname-example.md %}

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

## `Failed to login` Issues with Spark Shell

To run the `spark-shell` with the Alluxio client, the Alluxio client jar will have to be added to the classpath of the
Spark driver and Spark executors, as described earlier. However, sometimes Alluxio will fail to determine the security
user and will result in an error message similar to: `Failed to login: No Alluxio User is found.` Here are some
solutions.

### [Recommended] Configure `spark.sql.hive.metastore.sharedPrefixes` for Spark 1.4.0+

This is the recommended solution for this issue.

In Spark 1.4.0 and later, Spark uses an isolated classloader to load java classes for accessing the hive metastore.
However, the isolated classloader ignores certain packages and allows the main classloader to load "shared" classes
(the Hadoop HDFS client is one of these "shared" classes). The Alluxio client should also be loaded by the main
classloader, and you can append the `alluxio` package to the configuration parameter
`spark.sql.hive.metastore.sharedPrefixes` to inform Spark to load Alluxio with the main classloader. For example, the
parameter may be set to:

```bash
spark.sql.hive.metastore.sharedPrefixes=com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,alluxio
```

### [Workaround] Specify `fs.alluxio.impl` for Hadoop Configuration

If the recommended solution described above is infeasible, this is a workaround which can also solve this issue.

Specifying the Hadoop configuration `fs.alluxio.impl` may also help in resolving this error. `fs.alluxio.impl` should
be set to `alluxio.hadoop.FileSystem` and if you are using Alluxio in fault tolerant mode, `fs.alluxio-ft.impl` should
be set to `alluxio.hadoop.FaultTolerantFileSystem`. There are a few alternatives to set these parameters.

#### Update `hadoopConfiguration` in SparkContext

You can update the Hadoop configuration in the SparkContext by:

```scala
sc.hadoopConfiguration.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem")
sc.hadoopConfiguration.set("fs.alluxio-ft.impl", "alluxio.hadoop.FaultTolerantFileSystem")
```

This should be done early in your `spark-shell` session, before any Alluxio operations.

#### Update Hadoop Configuration Files

You can also add the properties to Hadoop's configuration files, and point Spark to the Hadoop configuration files.
The following should be added to Hadoop's `core-site.xml`.

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
  <property>
    <name>fs.alluxio-ft.impl</name>
    <value>alluxio.hadoop.FaultTolerantFileSystem</value>
  </property>
</configuration>
```

You can point Spark to the Hadoop configuration files by setting `HADOOP_CONF_DIR` in `spark-env.sh`.
