---
layout: global
title: Configuration Settings
group: Features
priority: 1
---

* Table of Contents
{:toc}

This page explains the configuration settings of Alluxio and also provides recommendation on how to customize the configuration
for Alluxio in different contexts.

## Configuration in Alluxio

Alluxio runtime respects three sources of configuration settings:

1. [Application settings](#application-settings). Setting Alluxio configuration in this way is application-specific,
and is required each time when running an application instance (e.g., a Spark job).
2. [Environment variables](#environment-variables). This is an easy and fast way to set the basic configuration
to manage Alluxio servers and run Alluxio shell commands.
Note that, configuration set through environment variables may not be realized by applications.
3. [Property files](#property-files). This is a general approach to customize any
[supported Alluxio configuration](#appendix). Configuration in those files can be respected by Alluxio servers,
as well as applications.

The priority to load configuration settings, from the highest to the lowest, is
application settings (if any), environment variables, property files and the defaults.

### Application settings

Alluxio shell users can use `-Dkey=property` to specify an Alluxio configuration value in commandline. For example,

{% include Configuration-Settings/specify-conf.md %}

Spark users can add `"-Dkey=property"` to `${SPARK_DAEMON_JAVA_OPTS}` in `conf/spark-env.sh`, or add it to
`spark.executor.extraJavaOptions` (for Spark executors) and `spark.driver.extraJavaOptions` (for Spark drivers).

Hadoop MapReduce users can set `"-Dkey=property"` in `hadoop jar` command-lines to pass it down to Alluxio:

{% include Configuration-Settings/hadoop-specify-conf.md %}

Note that, setting Alluxio configuration in this way is application specific and required for each job or command.

### Environment variables

> When you want to [start Alluxio server processes](Running-Alluxio-Locally.html), or [use Alluxio command line interfaces](Command-Line-Interface.html) with your specific configuration tuning, it is often fast and easy to set environment variables to customize basic Alluxio configuration. However, these environment variables will not affect application processes like Spark or MapReduce that use Alluxio as a client.


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

Users can either set these variables through shell or in `conf/alluxio-env.sh`. If this file does not exist yet,
Alluxio can help you bootstrap the `conf/alluxio-env.sh` file by running

{% include Common-Commands/bootstrapConf.md %}

Alternatively, you can create one from a template we provided in the source code using:

{% include Common-Commands/copy-alluxio-env.md %}

### Property files

> Alluxio site property file `alluxio-site.properties` can overwrite Alluxio configuration regardless the JVM is an Alluxio server process or a job using Alluxio client. For the site property file to be loaded, either the parent directory of this file is a part of the classpath of your target JVM process, or the file is in one of the pre-defined paths.

Using Alluxio supported environment variables has two limitations:
first it only provides basic Alluxio settings, and second it does not affect non-Alluxio JVMs like Spark or MapReduce.
To address them, Alluxio uses site property file `alluxio-site.properties` for users to customize all supported configuration settings, regardless of the JVM process.
On startup, Alluxio runtime checks if the configuration
property file exists and if so, it uses the content to override the default configuration.
To be specific, it searches `alluxio-site.properties` in `${HOME}/.alluxio/`, `/etc/alluxio/` (can be customized by changing the default value
of `alluxio.site.conf.dir`) and the classpath of the relevant Java VM process in order, and skips the remaining paths once
a file is found.

For example, `${ALLUXIO_HOME}/conf/` is by default on the classpath of Alluxio master, worker and shell JVM processes.
So you can simply create `${ALLUXIO_HOME}/conf/alluxio-site.properties` by

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Then customize it to fit your configuration tuning needs to start Alluxio servers or to use Alluxio shell commands:

{% include Common-Commands/copy-alluxio-site-properties.md %}

For applications like Spark or MapReduce to use Alluxio property files, you can append the directory of your site property files to your application classpath.
For example

```bash
$ export SPARK_CLASSPATH=${ALLUXIO_HOME}/conf:${SPARK_CLASSPATH} # for Spark jobs
$ export HADOOP_CLASSPATH=${ALLUXIO_HOME}/conf:${HADOOP_CLASSPATH} # for Hadoop jobs
```

Alternatively, with access to paths like `/etc/`, one can copy the site properties to `/etc/alluxio/`. This configuration will be shared
across processes regardless the JVM is an Alluxio server or a job using Alluxio client.


## Appendix

All Alluxio configuration settings fall into one of the six categories:
[Common](#common-configuration) (shared by Master and Worker),
[Master specific](#master-configuration), [Worker specific](#worker-configuration),
[User specific](#user-configuration), [Cluster specific](#cluster-management) (used for running
Alluxio with cluster managers like Mesos and YARN), and
[Security specific](#security-configuration) (shared by Master, Worker, and User).

### Common Configuration

The common configuration contains constants shared by different components.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.common-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.common-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### Master Configuration

The master configuration specifies information regarding the master node, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.master-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.master-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### Worker Configuration

The worker configuration specifies information regarding the worker nodes, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.worker-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.worker-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>


### User Configuration

The user configuration specifies values regarding file system access.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.user-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.user-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### Resource Manager Configuration

When running Alluxio with resource managers like Mesos and YARN, Alluxio has additional configuration options.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.cluster-management %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.cluster-management[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### Security Configuration

The security configuration specifies information regarding the security features, such as authentication and file permission. Settings for authentication take effect for master, worker, and user. Settings for file permission only take effect for master. See [Security](Security.html) for more information about security features.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.security-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.security-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### Configure Multihomed Networks

Alluxio configuration provides a way to take advantage of multi-homed networks. If you have more than one NICs and you want your Alluxio master to listen on all NICs, you can specify `alluxio.master.bind.host` to be `0.0.0.0`. As a result, Alluxio clients can reach the master node from connecting to any of its NIC. This is also the same case for other settings suffixed with `bind.host`.
