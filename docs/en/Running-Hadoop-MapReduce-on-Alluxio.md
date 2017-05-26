---
layout: global
title: Running Hadoop MapReduce on Alluxio
nickname: Apache Hadoop MapReduce
group: Frameworks
priority: 1
---

* Table of Contents
{:toc}

This guide describes how to get Alluxio running with Apache Hadoop MapReduce, so that you can
easily run your MapReduce programs with files stored on Alluxio.

## Initial Setup

The prerequisite for this guide includes

- You have [Java](Java-Setup.html).
- You have set up an Alluxio cluster in accordance to these guides
[Local Mode](Running-Alluxio-Locally.html) or [Cluster Mode](Running-Alluxio-on-a-Cluster.html).
- In order to run some simple map-reduce examples, we also recommend you download the
[map-reduce examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-examples)
based on your hadoop version, or if you are using Hadoop 1, this
[examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-examples/1.2.1).

## Prepare the Alluxio client jar

For the MapReduce applications to communicate with Alluxio service, it is required to have the
Alluxio client jar on their classpaths. We recommend you to download this client jar from
Alluxio [download page](http://www.alluxio.org/download).

Alternatively, advanced users can choose to compile this client jar from the source code.
You can run the following command in the Alluxio directory:

```bash
$ mvn install -P<HADOOP_PROFILE> -DskipTests
```

Available Hadoop profiles include `hadoop-1`, `hadoop-2.2`, `hadoop-2.3` ... `hadoop-2.8` for
different distributions of Hadoop. You can further set a specific Hadoop release version to
compile. For example,

```bash
$ mvn install -Phadoop-2.7 -Dhadoop.version=2.7.1 -DskipTests
```

After the compilation succeeds, the new Alluxio client jar can be found at
`{{site.ALLUXIO_CLIENT_JAR_PATH}}`. This is the jar that you should use for the rest of this guide.

## Configuring Hadoop

Add the following three properties to the `core-site.xml` file in `conf` directory of your Hadoop
installation:

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
  <description>The Alluxio FileSystem (Hadoop 1.x and 2.x)</description>
</property>
<property>
  <name>fs.alluxio-ft.impl</name>
  <value>alluxio.hadoop.FaultTolerantFileSystem</value>
  <description>The Alluxio FileSystem (Hadoop 1.x and 2.x) with fault tolerant support</description>
</property>
<property>
  <name>fs.AbstractFileSystem.alluxio.impl</name>
  <value>alluxio.hadoop.AlluxioFileSystem</value>
  <description>The Alluxio AbstractFileSystem (Hadoop 2.x)</description>
</property>
```

This will allow your MapReduce jobs to recognize URIs with Alluxio scheme (i.e., `alluxio://`) in
their input and output files.

Optionally, modify `$HADOOP_CLASSPATH` by changing `hadoop-env.sh` also in `conf` directory to
have:

```bash
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

This ensures Alluxio client jar available for the MapReduce job client that creates and submits
jobs to interact with URIs with Alluxio scheme.


## Distributing the Alluxio Client Jar

In order for the MapReduce applications to be able to read and write files in Alluxio, the Alluxio client jar
must be distributed on the classpath of the application across different nodes.

This guide on
[how to include 3rd party libraries from Cloudera](http://blog.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/)
describes several ways to distribute the jars. From that guide, the recommended way to distributed
the Alluxio client jar is to use the distributed cache, via the `-libjars` command line option.
Another way to distribute the client jar is to manually distribute it to all the Hadoop nodes.
Below are instructions for the two main alternatives:

1.**Using the -libjars command line option.**
You can use the `-libjars` command line option when using `hadoop jar ...`,
specifying `{{site.ALLUXIO_CLIENT_JAR_PATH}}`
as the argument of `-libjars`. Hadoop will place the jar in the Hadoop DistributedCache, making it
available to all the nodes. For example, the following command adds the Alluxio client jar to the
`-libjars` option:

```bash
$ bin/hadoop jar libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount -libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} <INPUT FILES> <OUTPUT DIRECTORY>
```

2.**Distributing the client jars to all nodes manually.**
To install Alluxio on each node, place the client jar
`{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the `$HADOOP_HOME/lib`
(may be `$HADOOP_HOME/share/hadoop/common/lib` for different versions of Hadoop) directory of every
MapReduce node, and then restart Hadoop. Alternatively, add this jar to
`mapreduce.application.classpath` system property for your Hadoop deployment
to ensure this jar is on the classpath.
Note that the jars must be installed again for each update to a new release. On the other hand,
when the jar is already on every node, then the `-libjars` command line option is not needed.

## Running Hadoop wordcount with Alluxio Locally

For simplicity, we will assume a pseudo-distributed Hadoop cluster, started by running (depends on
the hadoop version, you might need to replace `./bin` with `./sbin`.):

```bash
$ cd $HADOOP_HOME
$ bin/stop-all.sh
$ bin/start-all.sh
```

Start Alluxio locally:

```bash
$ bin/alluxio-start.sh local
```

You can add a sample file to Alluxio to run wordcount on. From your Alluxio directory:

```bash
$ bin/alluxio fs copyFromLocal LICENSE /wordcount/input.txt
```

This command will copy the `LICENSE` file into the Alluxio namespace with the path
`/wordcount/input.txt`.

Now we can run a MapReduce job (using Hadoop 2.7.3 as example) for wordcount.

```bash
$ bin/hadoop jar libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount -libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} alluxio://localhost:19998/wordcount/input.txt alluxio://localhost:19998/wordcount/output
```

After this job completes, the result of the wordcount will be in the `/wordcount/output` directory
in Alluxio. You can see the resulting files by running:

```bash
$ bin/alluxio fs ls /wordcount/output
$ bin/alluxio fs cat /wordcount/output/part-r-00000
```
