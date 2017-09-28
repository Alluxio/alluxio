---
layout: global
title: Configuration Settings
group: Features
priority: 1
---

* Table of Contents
{:toc}

Alluxio can be configured by setting the values of supported [configuration properties
](Configuration-Properties.html). To learn about how users can customize how an application
(e.g., a Spark or MapReduce job) interacts with Alluxio, see [how to configure Alluxio
applications](#configure-applications); to learn about how Alluxio admins can customize Alluxio
service, see [how to configure Alluxio clusters](#configure-alluxio-cluster).

# Configure Applications

Customizing how an application job interacts with Alluxio service is application specific. Here
we provide recommendations for a few common applications.

## Alluxio Shell Commands

Alluxio shell users can put JVM system properties `-Dproperty=value` after `fs` command and
before the subcommand (e.g., `copyFromLocal`) to specify Alluxio properties
from the command line. For example, the following Alluxio shell command sets the write type to
`CACHE_THROUGH` when copying files to Alluxio:

```bash
$ bin/alluxio fs -Dalluxio.user.file.writetype.default=CACHE_THROUGH copyFromLocal README.md /README.md
```

## Spark Jobs

Spark users can use pass JVM system properties to Spark jobs by adding `"-Dproperty=value"` to
`spark.executor.extraJavaOptions` for Spark executors and `spark.driver.extraJavaOptions` for
Spark drivers. For example, to submit a Spark job with the write `CACHE_THROUGH` when writing to
 Alluxio:

```bash
$ spark-submit \
--conf 'spark.driver.extraJavaOptions=-Dalluxio.user.file.writetype.default=CACHE_THROUGH' \
--conf 'spark.executor.extraJavaOptions=-Dalluxio.user.file.writetype.default=CACHE_THROUGH' \
...
```

In the Spark Shell, this can be achieved by:

```scala
val conf = new SparkConf()
    .set("spark.driver.extraJavaOptions", "-Dalluxio.user.file.writetype.default=CACHE_THROUGH")
    .set("spark.executor.extraJavaOptions", "-Dalluxio.user.file.writetype.default=CACHE_THROUGH")
val sc = new SparkContext(conf)
```

## Hadoop MapReduce Jobs

Hadoop MapReduce users can add `"-Dproperty=value"` after the `hadoop jar` or `yarn jar` command
and the properties will be propagated to all the tasks of this job.  For example, the following
MapReduce job of wordcount sets write type to `CACHE_THROUGH` when writing to Alluxio:

```bash
$ bin/hadoop jar libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount \
-Dalluxio.user.file.writetype.default=CACHE_THROUGH \
-libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} \
<INPUT FILES> <OUTPUT DIRECTORY>
```

# Configure Alluxio Cluster

## Use Site-Property Files (Recommended)

Alluxio admins can create and customize the property file `alluxio-site.properties` to
configuration an Alluxio cluster. If this file does not exist, it can be created from the
template file under `${ALLUXIO_HOME}/conf`:

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Make sure that this file is distributed to `${ALLUXIO_HOME}/conf` on every Alluxio nodes (masters
and workers) before starting the cluster.

> Note that, `${ALLUXIO_HOME}/conf/alluxio-site.properties` are only loaded by Alluxio server
> processes. Applications interacting with Alluxio service through Alluxio client will not load
> these site property files, unless `${ALLUXIO_HOME}/conf` is on the application classpath.

## Use Environment variables

Alluxio supports a few frequently used configuration settings via the environment
variables, including:

<table class="table table-striped">
<tr><th>Environment Variable</th><th>Description</th></tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_MASTER_HOSTNAME</code></td>
  <td>hostname of Alluxio master, defaults to localhost.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_UNDERFS_ADDRESS</code></td>
  <td>under storage system address, defaults to
<code class="highlighter-rouge">${ALLUXIO_HOME}/underFSStorage</code> which is a local file system.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_RAM_FOLDER</code></td>
  <td>the directory where a worker stores in-memory data, defaults to <code class="highlighter-rouge">/mnt/ramdisk</code>.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_JAVA_OPTS</code></td>
  <td>Java VM options for both Master, Worker and Alluxio Shell configuration.
  Note that, by default <code class="highlighter-rouge">ALLUXIO_JAVA_OPTS</code> is included in both
<code class="highlighter-rouge">ALLUXIO_MASTER_JAVA_OPTS</code>,
<code class="highlighter-rouge">ALLUXIO_WORKER_JAVA_OPTS</code> and
<code class="highlighter-rouge">ALLUXIO_USER_JAVA_OPTS</code>.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_MASTER_JAVA_OPTS</code></td>
  <td>additional Java VM options for Master configuration.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_WORKER_JAVA_OPTS</code></td>
  <td>additional Java VM options for Worker configuration. </td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_USER_JAVA_OPTS</code></td>
  <td>additional Java VM options for Alluxio shell configuration.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_CLASSPATH</code></td>
  <td>additional classpath entries for Alluxio processes. This is empty by default.</td>
</tr>
</table>

For example, if you would like to setup an Alluxio master at `localhost` that talks to an HDFS
cluster with a namenode also running at `localhost`, and enable Java remote debugging at port 7001,
you can do so before starting master process using:

```bash
$ export ALLUXIO_MASTER_HOSTNAME="localhost"
$ export ALLUXIO_UNDERFS_ADDRESS="hdfs://localhost:9000"
$ export ALLUXIO_MASTER_JAVA_OPTS="$ALLUXIO_JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=7001"
```

Users can either set these variables through the shell or in `conf/alluxio-env.sh`. If this file
does not exist yet, you can create one by copying the template:

```bash
$ cp conf/alluxio-env.sh.template conf/alluxio-env.sh
```

# Order of Configuration Sources

If an Alluxio property is configured in multiple sources, its value is determined by the source
earliest in this list:

1. [JVM system properties (i.e., `-Dproperty=key`)](http://docs.oracle.com/javase/jndi/tutorial/beyond/env/source.html#SYS)
2. [Environment variables](#use-environment-variables)
3. [Property files](#use-site-property-files-recommended). When an Alluxio cluster starts, each
server process including master and worker searches `alluxio-site.properties` in a list paths of
`${HOME}/.alluxio/`, `/etc/alluxio/` and `${ALLUXIO_HOME}/conf` in order, and will skip the
remaining paths once this `alluxio-site.properties` file is found.

If no above user-specified configuration is found for a property, Alluxio runtime will fallback to
its [default property value](Configuration-Properties.html).
