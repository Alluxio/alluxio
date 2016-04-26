---
layout: global
title: Running Hadoop MapReduce on Alluxio
nickname: Apache Hadoop MapReduce
group: Frameworks
priority: 1
---

This guide describes how to get Alluxio running with Apache Hadoop MapReduce, so that you can easily
run your MapReduce programs with files stored on Alluxio.

# Initial Setup

The prerequisite for this part is that you have [Java](Java-Setup.html). We also assume that you
have set up Alluxio and Hadoop in accordance to these guides
[Local Mode](Running-Alluxio-Locally.html) or [Cluster Mode](Running-Alluxio-on-a-Cluster.html).
In order to run some simple map-reduce examples, we also recommend you download the
[map-reduce examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-examples/2.4.1),
or if you are using Hadoop 1, this [examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-examples/1.2.1).

# Compiling the Alluxio Client

In order to use Alluxio with your version of Hadoop, you will have to re-compile the Alluxio client
jar, specifying your Hadoop version. You can do this by running the following in your Alluxio
directory:

{% include Running-Hadoop-MapReduce-on-Alluxio/compile-Alluxio-Hadoop.md %}

The version `<YOUR_HADOOP_VERSION>` supports many different distributions of Hadoop. For example,
`mvn install -Dhadoop.version=2.7.1 -DskipTests` would compile Alluxio for the Apache Hadoop version 2.7.1.
Please visit the
[Building Alluxio Master Branch](Building-Alluxio-Master-Branch.html#distro-support) page for more
information about support for other distributions.

After the compilation succeeds, the new Alluxio client jar can be found at:

    core/client/target/alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar

This is the jar that you should use for the rest of this guide.

# Configuring Hadoop

You need to add the following three properties to `core-site.xml` file in your Hadoop installation
`conf` directory:

{% include Running-Hadoop-MapReduce-on-Alluxio/config-core-site.md %}

This will allow your MapReduce jobs to use Alluxio for their input and output files. If you are
using HDFS as the under storage system for Alluxio, it may be necessary to add these properties to
the `hdfs-site.xml` file as well.

In order for the Alluxio client jar to be available to the JobClient, you can modify
`HADOOP_CLASSPATH` by changing `hadoop-env.sh` to:

{% include Running-Hadoop-MapReduce-on-Alluxio/config-hadoop.md %}

This allows the code that creates and submits the Job to use URIs with Alluxio scheme.

# Distributing the Alluxio Client Jar

In order for the MapReduce job to be able to read and write files in Alluxio, the Alluxio client jar
must be distributed to all the nodes in the cluster. This allows the TaskTracker and JobClient to
have all the requisite executables to interface with Alluxio.

This guide on
[how to include 3rd party libraries from Cloudera](http://blog.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/)
describes several ways to distribute the jars. From that guide, the recommended way to distributed
the Alluxio client jar is to use the distributed cache, via the `-libjars` command line option.
Another way to distribute the client jar is to manually distribute it to all the Hadoop nodes.
Below are instructions for the 2 main alternatives:

1.**Using the -libjars command line option.**
You can run a job by using the `-libjars` command line option when using `hadoop jar ...`,
specifying
`/<PATH_TO_ALLUXIO>/core/client/target/alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`
as the argument. This will place the jar in the Hadoop DistributedCache, making it available to all
the nodes. For example, the following command adds the Alluxio client jar to the `-libjars` option:

{% include Running-Hadoop-MapReduce-on-Alluxio/add-jar-libjars.md %}

2.**Distributing the jars to all nodes manually.**
For installing Alluxio on each node, you must place the client jar
`alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`
(located in the `/<PATH_TO_ALLUXIO>/core/client/target/` directory), in the `$HADOOP_HOME/lib`
(may be `$HADOOP_HOME/share/hadoop/common/lib` for different versions of Hadoop) directory of every
MapReduce node, and then restart all of the TaskTrackers. One caveat of this approach is that the
jars must be installed again for each update to a new release. On the other hand, when the jar is
already on every node, then the `-libjars` command line option is not needed.

# Running Hadoop wordcount with Alluxio Locally

First, compile Alluxio with the appropriate Hadoop version:

{% include Running-Hadoop-MapReduce-on-Alluxio/compile-Alluxio-Hadoop-test.md %}

For simplicity, we will assume a pseudo-distributed Hadoop cluster, started by running:

{% include Running-Hadoop-MapReduce-on-Alluxio/start-cluster.md %}

Configure Alluxio to use the local HDFS cluster as its under storage system. You can do this by
modifying `conf/alluxio-env.sh` to include:

{% include Running-Hadoop-MapReduce-on-Alluxio/config-Alluxio.md %}

Start Alluxio locally:

{% include Running-Hadoop-MapReduce-on-Alluxio/start-Alluxio.md %}

You can add a sample file to Alluxio to run wordcount on. From your Alluxio directory:

{% include Running-Hadoop-MapReduce-on-Alluxio/copy-from-local.md %}

This command will copy the `LICENSE` file into the Alluxio namespace with the path
`/wordcount/input.txt`.

Now we can run a MapReduce job for wordcount.

{% include Running-Hadoop-MapReduce-on-Alluxio/run-wordcount.md %}

After this job completes, the result of the wordcount will be in the `/wordcount/output` directory
in Alluxio. You can see the resulting files by running:

{% include Running-Hadoop-MapReduce-on-Alluxio/cat-result.md %}
