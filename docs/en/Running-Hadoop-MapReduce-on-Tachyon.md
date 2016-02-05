---
layout: global
title: Running Hadoop MapReduce on Tachyon
nickname: Apache Hadoop MapReduce
group: Frameworks
priority: 1
---

This guide describes how to get Tachyon running with Apache Hadoop MapReduce, so that you can easily
run your MapReduce programs with files stored on Tachyon.

# Initial Setup

The prerequisite for this part is that you have [Java](Java-Setup.html). We also assume that you have 
set up Tachyon and Hadoop in accordance to these guides [Local Mode](Running-Tachyon-Locally.html) or
[Cluster Mode](Running-Tachyon-on-a-Cluster.html)

## Using Hadoop 1.x

If running a Hadoop 1.x cluster, ensure that the `core-site.xml` file in your Hadoop installation
`conf` directory has the following properties added:

{% include Running-Hadoop-MapReduce-on-Tachyon/config-core-site.md %}

This will allow your MapReduce jobs to use Tachyon for their input and output files. If you are
using HDFS as the under storage system for Tachyon, it may be necessary to add these properties to
the `hdfs-site.xml` file as well.

## Using Hadoop 2.x

If you are using a 2.x Hadoop cluster, you should not need the properties above in your
`core-site.xml` file. However, in some cases you may encounter the error:
`java.io.IOException: No FileSystem for scheme: tachyon`. For instance, this may happen when Yarn 
(as opposed to Hadoop) tries to access Tachyon files. If this error is encountered, add these 
properties to your `core-site.xml` file, and restart Yarn.

{% include Running-Hadoop-MapReduce-on-Tachyon/config-core-site.md %}

# Compiling the Tachyon Client

In order to use Tachyon with your version of Hadoop, you will have to re-compile the Tachyon client
jar, specifying your Hadoop version. You can do this by running the following in your Tachyon
directory:

{% include Running-Hadoop-MapReduce-on-Tachyon/compile-Tachyon-Hadoop.md %}

The version `<YOUR_HADOOP_VERSION>` supports many different distributions of Hadoop. For example,
`mvn install -Dhadoop.version=2.7.1` would compile Tachyon for the Apache Hadoop version 2.7.1.
Please visit the
[Building Tachyon Master Branch](Building-Tachyon-Master-Branch.html#distro-support) page for more
information about support for other distributions.

After the compilation succeeds, the new Tachyon client jar can be found at:

    core/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar

This is the jar that you should use for the rest of this guide.

# Configuring Hadoop

In order for the Tachyon client jar to be available to the JobClient, you can modify
`HADOOP_CLASSPATH` by changing `hadoop-env.sh` to:

{% include Running-Hadoop-MapReduce-on-Tachyon/config-hadoop.md %}

This allows the code that creates and submits the Job to use URIs with Tachyon scheme.

# Distributing the Tachyon Client Jar

In order for the MapReduce job to be able to read and write files in Tachyon, the Tachyon client jar
must be distributed to all the nodes in the cluster. This allows the TaskTracker and JobClient to
have all the requisite executables to interface with Tachyon.

This guide on
[how to include 3rd party libraries from Cloudera](http://blog.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/)
describes several ways to distribute the jars. From that guide, the recommended way to distributed
the Tachyon client jar is to use the distributed cache, via the `-libjars` command line option.
Another way to distribute the client jar is to manually distribute it to all the Hadoop nodes.
Below are instructions for the 2 main alternatives:

1.**Using the -libjars command line option.**
You can run a job by using the `-libjars` command line option when using `hadoop jar ...`, 
specifying
`/<PATH_TO_TACHYON>/core/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar`
as the argument. This will place the jar in the Hadoop DistributedCache, making it available to all
the nodes. For example, the following command adds the Tachyon client jar to the `-libjars` option:

{% include Running-Hadoop-MapReduce-on-Tachyon/add-jar-libjars.md %}

2.**Distributing the jars to all nodes manually.**
For installing Tachyon on each node, you must place the client jar
`tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar`
(located in the `/<PATH_TO_TACHYON>/core/client/target/` directory), in the `$HADOOP_HOME/lib`
(may be `$HADOOP_HOME/share/hadoop/common/lib` for different versions of Hadoop) directory of every
MapReduce node, and then restart all of the TaskTrackers. One caveat of this approach is that the
jars must be installed again for each update to a new release. On the other hand, when the jar is 
already on every node, then the `-libjars` command line option is not needed.

# Running Hadoop wordcount with Tachyon Locally

First, compile Tachyon with the appropriate Hadoop version:

{% include Running-Hadoop-MapReduce-on-Tachyon/compile-Tachyon-Hadoop-test.md %}

For simplicity, we will assume a pseudo-distributed Hadoop cluster, started by running:

{% include Running-Hadoop-MapReduce-on-Tachyon/start-cluster.md %}

Configure Tachyon to use the local HDFS cluster as its under storage system. You can do this by
modifying `conf/tachyon-env.sh` to include:

{% include Running-Hadoop-MapReduce-on-Tachyon/config-Tachyon.md %}

Start Tachyon locally:

{% include Running-Hadoop-MapReduce-on-Tachyon/start-Tachyon.md %}

You can add a sample file to Tachyon to run wordcount on. From your Tachyon directory:

{% include Running-Hadoop-MapReduce-on-Tachyon/copy-from-local.md %}

This command will copy the `LICENSE` file into the Tachyon namespace with the path
`/wordcount/input.txt`.

Now we can run a MapReduce job for wordcount.

{% include Running-Hadoop-MapReduce-on-Tachyon/run-wordcount.md %}

After this job completes, the result of the wordcount will be in the `/wordcount/output` directory
in Tachyon. You can see the resulting files by running:

{% include Running-Hadoop-MapReduce-on-Tachyon/cat-result.md %}

You can also see the file in the under storage system HDFS name node web UI. The local HDFS cluster
web UI can be found at [localhost:50070](http://localhost:50070/).
