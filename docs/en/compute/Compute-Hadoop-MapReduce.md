---
layout: global
title: Running Hadoop MapReduce on Alluxio
nickname: Apache Hadoop MapReduce
group: Compute Applications
priority: 1
---

* Table of Contents
{:toc}

This guide describes how to get Alluxio running with Apache Hadoop MapReduce, so that you can
easily run your MapReduce programs with files stored on Alluxio.

## Prerequisites

* An Alluxio cluster has been set up and is running according to 
[Local Mode]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-Locally.md %}), 
[Cluster Mode]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-On-a-Cluster.md %})
or other deployment ways.
* Make sure that the Alluxio client jar is available.
This Alluxio client jar file can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the tarball 
downloaded from Alluxio [download page](http://www.alluxio.org/download).
Alternatively, advanced users can compile this client jar from the source code 
by following the [instructions]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %}).
* In order to run some simple map-reduce examples, we also recommend you download the 
[map-reduce examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-examples)
based on your hadoop version, or if you are using Hadoop 1, this
[examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-examples/1.2.1).

## Basic Setup

### Configuring Hadoop

Add the following two properties to the `core-site.xml` file of your Hadoop installation:

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
  <description>The Alluxio FileSystem (Hadoop 1.x and 2.x)</description>
</property>
<property>
  <name>fs.AbstractFileSystem.alluxio.impl</name>
  <value>alluxio.hadoop.AlluxioFileSystem</value>
  <description>The Alluxio AbstractFileSystem (Hadoop 2.x)</description>
</property>
```

This will allow your MapReduce jobs to recognize URIs with Alluxio scheme `alluxio://` in
their input and output files.

### Distributing the Alluxio Client Jar

In order for the MapReduce applications to read and write files in Alluxio, the Alluxio client jar
must be distributed on the classpath of the application across different nodes.

You can use the `-libjars` command line option when using `hadoop jar ...`,
specifying `{{site.ALLUXIO_CLIENT_JAR_PATH}}`
as the argument of `-libjars`. Hadoop will place the jar in the Hadoop DistributedCache, making it
available to all the nodes. For example, the following command adds the Alluxio client jar to the
`-libjars` option:

```bash
$ bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount -libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} <INPUT FILES> <OUTPUT DIRECTORY>
```

Sometimes, you also need to set the `HADOOP_CLASSPATH` environment variable to make Alluxio client
jar available to the client JVM which is created when you run the `hadoop jar` command:

```bash
$  export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

Alternative ways are described in the [Advanced Setup](#advanced-setup)

## Example

For simplicity, we will assume a pseudo-distributed Hadoop cluster, started by running (depends on
the hadoop version, you might need to replace `./bin` with `./sbin`.):

```bash
$ cd $HADOOP_HOME
$ bin/stop-all.sh
$ bin/start-all.sh
```

Start Alluxio locally:

```bash
$ bin/alluxio-start.sh local SudoMount
```

You can add a sample file to Alluxio to run wordcount on. From your Alluxio directory:

```bash
$ bin/alluxio fs copyFromLocal LICENSE /wordcount/input.txt
```

This command will copy the `LICENSE` file into the Alluxio namespace with the path
`/wordcount/input.txt`.

Now we can run a MapReduce job (using Hadoop 2.7.3 as example) for wordcount.

```bash
$ bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount -libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} alluxio://localhost:19998/wordcount/input.txt alluxio://localhost:19998/wordcount/output
```

After this job completes, the result of the wordcount will be in the `/wordcount/output` directory
in Alluxio. You can see the resulting files by running:

```bash
$ bin/alluxio fs ls /wordcount/output
$ bin/alluxio fs cat /wordcount/output/part-r-00000
```

> Tips：The previous wordcount example is also applicable to Alluxio in fault tolerant mode with Zookeeper. 
Please follow the instructions in 
[HDFS API to connect to Alluxio with high availability]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-On-a-Cluster.md %}#hdfs-api).

## Advanced Setup

### Distributing the Alluxio Client Jar

This guide on	
[how to include 3rd party libraries from Cloudera](http://blog.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/)	
describes several ways to distribute the jars. From that guide, the recommended way to distributed	
the Alluxio client jar is to use the distributed cache, via the `-libjars` command line option.	
Another way to distribute the client jar is to manually distribute it to all the Hadoop nodes.

You could place the client jar `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the `$HADOOP_HOME/lib`
(may be `$HADOOP_HOME/share/hadoop/common/lib` for different versions of Hadoop) directory of every
MapReduce node, and then restart Hadoop. Alternatively, add this jar to
`mapreduce.application.classpath` system property for your Hadoop deployment
to ensure this jar is on the classpath.

Note that the jars must be installed again for each update to a new release. On the other hand,
when the jar is already on every node, then the `-libjars` command line option is not needed.

## Troubleshooting

### Check MapReduce with Alluxio integration (Supports Hadoop 2.X)

Before running MapReduce on Alluxio, you might want to make sure that your configuration has been
setup correctly for integrating with Alluxio. The MapReduce integration checker can help you achieve this.

When you have a running Hadoop cluster (or standalone), you can run the following command in the Alluxio project directory:

```bash
$ integration/checker/bin/alluxio-checker.sh mapreduce
```

You can use `-h` to display helpful information about the command.
This command will report potential problems that might prevent you from running MapReduce on Alluxio.

### Q: Why do I see exceptions like "No FileSystem for scheme: alluxio"?

A: This error message is seen when your MapReduce application tries to access
Alluxio as an HDFS-compatible file system, but the `alluxio://` scheme is not recognized by the
application. Please make sure your HDFS configuration file `core-site.xml` has the following property:

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```

### Q: Why do I see exceptions like "java.lang.RuntimeException: java.lang.ClassNotFoundException: Class alluxio.hadoop.FileSystem not found"?

A: This error message is seen when your MapReduce application tries to access
Alluxio as an HDFS-compatible file system, the `alluxio://` scheme has been
configured correctly but the Alluxio client jar is not found on the classpath of your application.

You can append the client jar to `$HADOOP_CLASSPATH`:

```bash
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

If the corresponding classpath has been set but exceptions still exist, users can check
whether the path is valid by:

```bash
$ ls {{site.ALLUXIO_CLIENT_JAR_PATH}}
```
