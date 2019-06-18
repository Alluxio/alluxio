---
layout: global
title: Running Hadoop MapReduce on Alluxio
nickname: Apache Hadoop MapReduce
group: Data Applications
priority: 1
---

This guide describes how to get Alluxio running with Apache Hadoop MapReduce, so that you can
easily run your MapReduce programs with files stored on Alluxio.

* Table of Contents
{:toc}

## Prerequisites

* Alluxio has been set up and is running.
* Make sure that the Alluxio client jar is available.
This Alluxio client jar file can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the tarball
downloaded from Alluxio [download page](http://www.alluxio.io/download).
Alternatively, advanced users can compile this client jar from the source code
by following the [instructions]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}).
* In order to run map-reduce examples, we also recommend you download the
[map-reduce examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-examples)
based on your Hadoop version, or if you are using Hadoop 1, this
[examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-examples/1.2.1).

## Basic Setup

### Configuring Hadoop Core-site Properties

Note that, this step is only required for Hadoop 1.x and can be skipped by users of Hadoop 2.x or
later. Add the following property to the `core-site.xml` file of your Hadoop installation:

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
  <description>The Alluxio FileSystem</description>
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
./bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount -libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} <INPUT FILES> <OUTPUT DIRECTORY>
```

Sometimes, you also need to set the `HADOOP_CLASSPATH` environment variable to make Alluxio client
jar available to the client JVM which is created when you run the `hadoop jar` command:

```bash
 export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

Alternative ways are described in the [Advanced Setup](#advanced-setup)

## Example

For simplicity, we will assume a pseudo-distributed Hadoop cluster, started by running:

```bash
cd $HADOOP_HOME
./bin/stop-all.sh
./bin/start-all.sh
```

Depending on the Hadoop version, you may need to replace `./bin` with `./sbin`.

Start Alluxio locally:

```bash
./bin/alluxio-start.sh local SudoMount
```

You can add a sample file to Alluxio to run wordcount on. From your Alluxio directory:

```bash
./bin/alluxio fs mkdir /wordcount
./bin/alluxio fs copyFromLocal LICENSE /wordcount/input.txt
```

This command will copy the `LICENSE` file into the Alluxio namespace with the path
`/wordcount/input.txt`.

Now we can run a MapReduce job (using Hadoop 2.7.3 as example) for wordcount.

```bash
./bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount -libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} alluxio://localhost:19998/wordcount/input.txt alluxio://localhost:19998/wordcount/output
```

After this job completes, the result of the wordcount will be in the `/wordcount/output` directory
in Alluxio. You can see the resulting files by running:

```bash
./bin/alluxio fs ls /wordcount/output
./bin/alluxio fs cat /wordcount/output/part-r-00000
```

> Tips：The previous wordcount example is also applicable to Alluxio in HA mode.
Please follow the instructions in
[HDFS API to connect to Alluxio with high availability]({{ '/en/deploy/Running-Alluxio-On-a-Cluster.html' | relativize_url }}#configure-alluxio-clients-for-ha).

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

### Customize Alluxio User Properties for All MapReduce Jobs

Alluxio configuration parameters can be added to the Hadoop `core-site.xml` file to affect all MapReduce jobs.
Let us use the setup of Hadoop to interact with the Alluxio service in HA Mode as an example.

When Alluxio is running in HA mode, add the HA mode client configuration in `core-site.xml` file of your Hadoop installation.
For example, if the Alluxio is using internal leader election, add the `alluxio.master.rpc.addresses` property:

```xml
<configuration>
  <property>
    <name>alluxio.master.rpc.addresses</name>
    <value>master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998</value>
  </property>
</configuration>
```

See [HA mode client configuration parameters]({{ '/en/deploy/Running-Alluxio-On-a-Cluster.html' | relativize_url }}#ha-configuration-parameters)
for more details.

### Customize Alluxio User Properties for Individual MapReduce Jobs

Hadoop MapReduce users can add `"-Dproperty=value"` after the `hadoop jar` or `yarn jar` command
and the properties will be propagated to all the tasks of this job.  For example, the following
MapReduce job of wordcount sets write type to `CACHE_THROUGH` when writing to Alluxio:

```bash
./bin/hadoop jar libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount \
-Dalluxio.user.file.writetype.default=CACHE_THROUGH \
-libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} \
<INPUT FILES> <OUTPUT DIRECTORY>
```

## Troubleshooting

### Logging Configuration

Logs with Hadoop can be modified in many different ways. If you wish to directly modify the
`log4j.properties` file for Hadoop, then you can add or modify appenders within
`${HADOOP_HOME}/conf/log4j.properties` on each of the nodes in your cluster.

You may also modify the configuration values in
[`mapred-site.xml`](https://hadoop.apache.org/docs/r2.7.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)
in your installation. If you simply wish to modify log levels then your can change
`mapreduce.map.log.level` or `mapreduce.reduce.log.level`.

If you arn using YARN then you may also wish to modify some of the `yarn.log.*` properties which
can be found in [`yarn-site.xml`](https://hadoop.apache.org/docs/r2.7.6/hadoop-yarn/hadoop-yarn-common/yarn-default.xml)

### Check MapReduce with Alluxio integration (Supports Hadoop 2.X)

Before running MapReduce on Alluxio, you might want to make sure that your configuration has been
setup correctly for integrating with Alluxio. The MapReduce integration checker can help you achieve this.

When you have a running Hadoop cluster (or standalone), you can run the following command in the Alluxio project directory:

```bash
integration/checker/bin/alluxio-checker.sh mapreduce
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
export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

If the corresponding classpath has been set but exceptions still exist, users can check
whether the path is valid by:

```bash
ls {{site.ALLUXIO_CLIENT_JAR_PATH}}
```
