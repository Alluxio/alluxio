---
layout: global
title: Configuration Settings
group: Features
priority: 1
---

* Table of Contents
{:toc}

Alluxio can be configured by setting the values of supported [configuration properties
](Configuration-Parameters.html). For a user who wants to customize how an application  (e.g., a
Spark or MapReduce job) interacting with Alluxio, check
[how to configure Alluxio applications](#configure- applications);
for Alluxio admin who wants to customize Alluxio service, check
[how to configure Alluxio clusters](#configure-alluxio-cluster).

# Configure Applications

Customizing how an application job interacts with Alluxio can be application specific. Here we
provide recommendation for a few common applications, as well as an general solution.

## Alluxio Shell Commands

Alluxio shell users can use JVM system property `-Dproperty=value` to specify an Alluxio property in commandline. For example, the following Alluxio shell command sets the write type to
`CACHE_THROUGH` when copying files to Alluxio:

```bash
$ bin/alluxio fs -Dalluxio.user.file.writetype.default=CACHE_THROUGH copyFromLocal README.md /README.md
```


## Spark Jobs

Spark users can pass JVM system properties to Spark jobs by adding `"-Dproperty=value"` to
`spark.executor.extraJavaOptions` for Spark executors and `spark.driver.extraJavaOptions` for
Spark drivers. For example, the following Spark job sets write type to `CACHE_THROUGH` when
writing to Alluxio:

```bash
$ spark-submit \
--conf 'spark.driver.extraJavaOptions=-Dalluxio.user.file.writetype.default=CACHE_THROUGH' \
--conf 'spark.executor.extraJavaOptions=-Dalluxio.user.file.writetype.default=CACHE_THROUGH' \
...
```


## Hadoop MapReduce Jobs

Hadoop MapReduce users can add `"-Dproperty=value"` in `hadoop jar` command-lines. For example, the
following MapReduce job of wordcount sets write type to `CACHE_THROUGH` when writing to Alluxio:

```bash
$ bin/hadoop jar libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount \
-Dalluxio.user.file.writetype.default=CACHE_THROUGH \
-libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} \
<INPUT FILES> <OUTPUT DIRECTORY>
```


## General Java Applications

For a general application using Alluxio client, one can create a
[Java property file](https://en.wikipedia.org/wiki/.properties) named
`alluxio-site.properties` and append the directory of this property file to the application
classpath. Note that, this requires the privilege to set application classpath before launching
the application, and also to deploy the property file on every node running the application.

For example one can make the following change for Spark or Hadoop cluster:

```bash
$ edit /path/alluxio-site.properties
$ export SPARK_CLASSPATH=${SPARK_CLASSPATH}:/path # for Spark jobs
$ export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/path # for Hadoop jobs
```

# Configure Alluxio Cluster

## Use Site-Property Files (Recommended)

Alluxio admin can create and customize Java property file `alluxio-site.properties` to
configuration an Alluxio cluster. If this file does not exist, it can be simply created from a
template file:

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

On startup, Alluxio runtime searches `alluxio-site.properties` in `${HOME}/.alluxio/`,
`/etc/alluxio/`
and the classpath of the relevant Java VM process in order including `${ALLUXIO_HOME}/conf`, and skips the remaining paths once this `alluxio-site.properties` file is found.

## Use Environment variables

Alluxio supports a few basic and very frequently used configuration settings via the environment variables in
`conf/alluxio-env.sh`, including:

<table class="table table-striped">
<tr><th>Environment Variable</th><th>Meaning</th></tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_MASTER_HOSTNAME</code></td>
  <td>hostname of Alluxio master, defaults to localhost.</td>
</tr>
<tr>
  <td><del><code class="highlighter-rouge">ALLUXIO_MASTER_ADDRESS</code></del></td>
  <td>deprecated by <code class="highlighter-rouge">ALLUXIO_MASTER_HOSTNAME</code> since version 1.1 and
will be remove in version 2.0.</td>
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

For example, if you would like to setup an Alluxio master at `localhost` that talks to an HDFS cluster with a namenode
also running at `localhost`, and enable Java remote debugging at port 7001, you can do so before starting master process using:

{% include Configuration-Settings/more-conf.md %}

Users can either set these variables through shell or in `conf/alluxio-env.sh`. If this file does not exist yet, you can create one from a template we provided in the source code using:

{% include Common-Commands/copy-alluxio-env.md %}

# Order of Configuration Sources

If an Alluxio property is configured in potentially multiple sources, its value gets decided in the
following order from the highest to the lowest:

1. [JVM System Properties (`-Dproperty=key`)](http://docs.oracle.com/javase/jndi/tutorial/beyond/env/source.html#SYS)
2. [Environment variable](#use-environment-variables)
3. [Property file](#use-site-property-files-recommended)
4. [Default property value](Configuration-Parameters.html). If no above configuration
sources is found, Alluxio runtime will fallback to the default configuration.
