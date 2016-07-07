---
layout: global
title: Configuration Settings
group: Features
priority: 1
---

* Table of Contents
{:toc}

This page explains the configuration system of Alluxio and also provides recommendation on how to customize the configuration
for Alluxio in different contexts.

# Configuration in Alluxio

Alluxio runtime respects three sources of configuration settings:

1. [Application settings](#application-settings). Setting Alluxio configuration in this way is application-specific,
and is required each time when running an application instance (e.g., a Spark job).
2. [Environment variables](#environment-variables). This is an easy and fast way to set the basic properties
to manage Alluxio servers and run Alluxio shell commands.
Note that, configuration set through environment variables may not be realized by applications.
3. [Property files](#property-files). This is a general approach to customize any
[supported Alluxio configure properties](#appendix). Configuration in those files can be respected by Alluxio servers,
as well as applications.


The priority to load property values, from the highest to the lowest, is
application settings (if any), environment variables, property files and the defaults.

## Application settings

Alluxio shell users can use `-Dkey=property` to specify an Alluxio configuration value in commandline. For example,

{% include Configuration-Settings/specify-conf.md %}

Spark users can add `"-Dkey=property"` to `${SPARK_DAEMON_JAVA_OPTS}` in `conf/spark-env.sh`, or add it to
`spark.executor.extraJavaOptions` (for Spark executors) and `spark.driver.extraJavaOptions` (for Spark drivers).

Hadoop MapReduce users can set `"-Dkey=property"` in `hadoop jar` command-lines to pass it down to Alluxio:

{% include Configuration-Settings/hadoop-specify-conf.md %}

Note that, setting Alluxio configuration in this way is application specific and required for each job or command.

## Environment variables

There are a few basic and very frequently used Alluxio configuration properties that can be set via the
following environment variables:

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
</table>

For example, if you would like to setup an Alluxio master at `localhost` that talks to an HDFS cluster with a namenode
also running at `localhost`, and enable Java remote debugging at port 7001, you can do so using:

{% include Configuration-Settings/more-conf.md %}

Users can either set these variables through shell or in `conf/alluxio-env.sh`. If this file does not exist yet,
Alluxio can help you bootstrap the `conf/alluxio-env.sh` file by running

{% include Common-Commands/bootstrap-conf.md %}

Alternatively, you can create one from a template we provided in the source code using:

{% include Common-Commands/copy-alluxio-env.md %}


Note that `conf/alluxio-env.sh` is sourced when you
[launch Alluxio servers](Running-Alluxio-Locally.html), or [use Alluxio command line interfaces](Command-Line-Interface.html),
but not for applications.


## Property files

In addition to these environment variables that only provide basic settings, Alluxio also provides a
more general approach for users to customize all supported configuration properties via property files.
For each Alluxio site deployment, both servers or application clients can override the default property values via
`alluxio-site.properties` file. On startup, Alluxio checks if the configuration
property file exists and if so, it uses the content to override the default values of configuration properties.
This property file is searched in `${HOME}/.alluxio/`, `/etc/alluxio/` (can be customized by changing the default value
of `alluxio.site.conf.dir`) and the classpath of the Java VM (in
which Alluxio is running) in order.

For example, one can copy the site properties template in directory
`${ALLUXIO_HOME}/conf` to `${HOME}/.alluxio/` and edit it to fit your configuration tuning needs.

{% include Common-Commands/copy-alluxio-site-properties.md %}


Note that, once set, configuration in those property files can be shared across Alluxio servers and those jobs using Alluxio clients.

# Appendix
All Alluxio configuration properties fall into one of the six categories:
[Common](#common-configuration) (shared by Master and Worker),
[Master specific](#master-configuration), [Worker specific](#worker-configuration),
[User specific](#user-configuration), [Cluster specific](#cluster-management) (used for running
Alluxio with cluster managers like Mesos and YARN), and
[Security specific](#security-configuration) (shared by Master, Worker, and User).

## Common Configuration

The common configuration contains constants shared by different components.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.common-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.common-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Master Configuration

The master configuration specifies information regarding the master node, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.master-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.master-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Worker Configuration

The worker configuration specifies information regarding the worker nodes, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.worker-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.worker-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>


## User Configuration

The user configuration specifies values regarding file system access.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.user-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.user-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Cluster Management

When running Alluxio with cluster managers like Mesos and YARN, Alluxio has additional
configuration options.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.cluster-management %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.cluster-management.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Security Configuration

The security configuration specifies information regarding the security features,
such as authentication and file permission. Properties for authentication take effect for master,
worker, and user. Properties for file permission only take effect for master.
See [Security](Security.html) for more information about security features.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.security-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.security-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Configure multihomed networks

Alluxio configuration provides a way to take advantage of multi-homed networks. If you have more
than one NICs and you want your Alluxio master to listen on all NICs, you can specify
`alluxio.master.bind.host` to be `0.0.0.0`. As a result, Alluxio clients can reach the master node
from connecting to any of its NIC. This is also the same case for other properties suffixed with
`bind.host`.
