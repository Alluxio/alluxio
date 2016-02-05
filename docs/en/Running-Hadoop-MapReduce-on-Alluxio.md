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

The prerequisite for this part is that you have [Java](Java-Setup.html). We also assume that you have 
set up Alluxio and Hadoop in accordance to these guides [Local Mode](Running-Alluxio-Locally.html) or
[Cluster Mode](Running-Alluxio-on-a-Cluster.html)

## Using Hadoop 1.x

If running a Hadoop 1.x cluster, ensure that the `core-site.xml` file in your Hadoop installation
`conf` directory has the following properties added:

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.TFS</value>
</property>
<property>
  <name>fs.alluxio-ft.impl</name>
  <value>alluxio.hadoop.TFSFT</value>
</property>
```

This will allow your MapReduce jobs to use Alluxio for their input and output files. If you are
using HDFS as the under storage system for Alluxio, it may be necessary to add these properties to
the `hdfs-site.xml` file as well.

## Using Hadoop 2.x

If you are using a 2.x Hadoop cluster, you should not need the properties above in your
`core-site.xml` file. However, in some cases you may encounter the error:
`java.io.IOException: No FileSystem for scheme: alluxio`. For instance, this may happen when Yarn 
(as opposed to Hadoop) tries to access Alluxio files. If this error is encountered, add these 
properties to your `core-site.xml` file, and restart Yarn.

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.TFS</value>
</property>
<property>
  <name>fs.alluxio-ft.impl</name>
  <value>alluxio.hadoop.TFSFT</value>
</property>
```

# Compiling the Alluxio Client

In order to use Alluxio with your version of Hadoop, you will have to re-compile the Alluxio client
jar, specifying your Hadoop version. You can do this by running the following in your Alluxio
directory:

```bash
$ mvn install -Dhadoop.version=<YOUR_HADOOP_VERSION>
```

The version `<YOUR_HADOOP_VERSION>` supports many different distributions of Hadoop. For example,
`mvn install -Dhadoop.version=2.7.1` would compile Alluxio for the Apache Hadoop version 2.7.1.
Please visit the
[Building Alluxio Master Branch](Building-Alluxio-Master-Branch.html#distro-support) page for more
information about support for other distributions.

After the compilation succeeds, the new Alluxio client jar can be found at:

    core/client/target/alluxio-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar

This is the jar that you should use for the rest of this guide.

# Configuring Hadoop

In order for the Alluxio client jar to be available to the JobClient, you can modify
`HADOOP_CLASSPATH` by changing `hadoop-env.sh` to:

```bash
$ export HADOOP_CLASSPATH=/<PATH_TO_TACHYON>/core/client/target/alluxio-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar
```

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
`/<PATH_TO_TACHYON>/core/client/target/alluxio-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`
as the argument. This will place the jar in the Hadoop DistributedCache, making it available to all
the nodes. For example, the following command adds the Alluxio client jar to the `-libjars` option:

```bash
$ hadoop jar hadoop-examples-1.2.1.jar wordcount -libjars /<PATH_TO_TACHYON>/core/client/target/alluxio-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar <INPUT FILES> <OUTPUT DIRECTORY>`
```

2.**Distributing the jars to all nodes manually.**
For installing Alluxio on each node, you must place the client jar
`alluxio-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`
(located in the `/<PATH_TO_TACHYON>/core/client/target/` directory), in the `$HADOOP_HOME/lib`
(may be `$HADOOP_HOME/share/hadoop/common/lib` for different versions of Hadoop) directory of every
MapReduce node, and then restart all of the TaskTrackers. One caveat of this approach is that the
jars must be installed again for each update to a new release. On the other hand, when the jar is 
already on every node, then the `-libjars` command line option is not needed.

# Running Hadoop wordcount with Alluxio Locally

First, compile Alluxio with the appropriate Hadoop version:

```bash
$ mvn clean install -Dhadoop.version=<YOUR_HADOOP_VERSION>
```

For simplicity, we will assume a pseudo-distributed Hadoop cluster, started by running:

```bash
$ cd $HADOOP_HOME
$ ./bin/stop-all.sh
$ ./bin/start-all.sh
```

Configure Alluxio to use the local HDFS cluster as its under storage system. You can do this by
modifying `conf/alluxio-env.sh` to include:

```bash
export ALLUXIO_UNDERFS_ADDRESS=hdfs://localhost:9000
```

Start Alluxio locally:

```bash
$ ./bin/alluxio-stop.sh all
$ ./bin/alluxio-start.sh local
```

You can add a sample file to Alluxio to run wordcount on. From your Alluxio directory:

```bash
$ ./bin/alluxio tfs copyFromLocal LICENSE /wordcount/input.txt
```

This command will copy the `LICENSE` file into the Alluxio namespace with the path
`/wordcount/input.txt`.

Now we can run a MapReduce job for wordcount.

```bash
$ bin/hadoop jar hadoop-examples-1.2.1.jar wordcount -libjars /<PATH_TO_TACHYON>/core/client/target/alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar -Dalluxio.user.file.understoragetype.default=SYNC_PERSIST alluxio://localhost:19998/wordcount/input.txt alluxio://localhost:19998/wordcount/output
```

After this job completes, the result of the wordcount will be in the `/wordcount/output` directory
in Alluxio. You can see the resulting files by running:

```bash
$ ./bin/alluxio tfs ls /wordcount/output
$ ./bin/alluxio tfs cat /wordcount/output/part-r-00000
```

You can also see the file in the under storage system HDFS name node web UI. The local HDFS cluster
web UI can be found at [localhost:50070](http://localhost:50070/).
