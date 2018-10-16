---
layout: global
title: Configuration Settings
group: Basic
priority: 0
---

* Table of Contents
{:toc}

An Alluxio cluster can be configured by setting the values of Alluxio
[configuration properties](
{{ site.baseurl }}{% link en/reference/Properties-List.md %}). The two major components to configure
are
[Alluxio servers](#configure-alluxio-cluster) (masters and workers), and
[Alluxio clients]((#configure-applications)) (part of applications).

## Configure Applications

Customizing how an application job interacts with Alluxio service is application specific. Here
we provide recommendations for a few common applications.
Note that, it is only valid for users to set client-side configurations
(e.g., properties prefixed with `alluxio.user`); setting server-side properties
(e.g., properties prefixed with `alluxio.master` and `alluxio.worker`) at application
will not take effect.

### Alluxio Shell Commands

Alluxio shell users can put JVM system properties `-Dproperty=value` after `fs` command and
before the subcommand (e.g., `copyFromLocal`) to specify Alluxio user properties
from the command line. For example, the following Alluxio shell command sets the write type to
`CACHE_THROUGH` when copying files to Alluxio:

```bash
$ bin/alluxio fs -Dalluxio.user.file.writetype.default=CACHE_THROUGH copyFromLocal README.md /README.md
```

Note that, as a part of Alluxio deployment, Alluxio shell will also take the configuration in
`${ALLUXIO_HOME}/conf/alluxio-site.properties` when it is run from Alluxio installation
`${ALLUXIO_HOME}`.

### Spark

To customize Alluxio client-side properties in Spark,
Spark users can use pass Alluxio properties as JVM system properties.
See examples for [the entire Spark Service]({{ site.baseurl }}{% link en/compute/Spark.md
%}#customize-alluxio-user-properties-for-all-spark-jobs)
or for [individual Spark Jobs]({{ site.baseurl }}{% link en/compute/Spark.md
 %}#customize-alluxio-user-properties-for-individual-spark-jobs).

### Hadoop MapReduce

See examples to configure Alluxio properties for [the entire MapReduce service]({{ site.baseurl }}{% link en/compute/Hadoop-MapReduce.md %}#customize-alluxio-user-properties-for-all-mapreduce-jobs)
or for [individual MapReduce jobs]({{ site.baseurl }}{% link en/compute/Hadoop-MapReduce.md
%}#customize-alluxio-user-properties-for-individual-mapreduce-jobs).

### Hive

When accessing Alluxio service, Hive can be configured to use customized Alluxio
client-side properties for the entire service.
See [examples]({{ site.baseurl }}{% link
en/compute/Hive.md %}#customize-alluxio-user-properties).

### Presto

When accessing Alluxio service, Presto can be configured to use customized Alluxio
client-side properties for the entire service.
See [examples]({{ site.baseurl }}{% link
en/compute/Presto.md %}#customize-alluxio-user-properties).

## Configure Alluxio Cluster

### Use Site-Property Files (Recommended)

Alluxio admins can create and customize the property file `alluxio-site.properties` to
configure an Alluxio server (master or worker). If this file does not exist, it can be created from the
template file under `${ALLUXIO_HOME}/conf`:

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Make sure that this file is distributed to `${ALLUXIO_HOME}/conf` on every Alluxio server (masters
and workers) before starting the cluster. Any updates to the server configuration requires a
restart of the Alluxio master or worker process.

### Use Environment Variables

Alluxio supports a few frequently used configuration settings via the environment
variables, including:

<table class="table table-striped">
<tr><th>Environment Variable</th><th>Description</th></tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_CONF_DIR</code></td>
  <td>path to Alluxio configuration directory.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_LOGS_DIR</code></td>
  <td>path to Alluxio logs directory.</td>
</tr>
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
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_LOGSERVER_HOSTNAME</code></td>
  <td>host name of the log server. This is empty by default.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_LOGSERVER_PORT</code></td>
  <td>port number of the log server. This is 45600 by default.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_LOGSERVER_LOGS_DIR</code></td>
  <td>path to the local directory where Alluxio log server stores logs received from Alluxio servers.</td>
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

### Specify Cluster-Wide Defaults

Since v1.8, each Alluxio client can initialize its configuration with the cluster-wide
configuration values retrieved from masters. In addition to client applications, Alluxio workers
can also initialize its configuration from the masters.
To be specific, when different client applications (Alluxio Shell CLI, Spark jobs, MapReduce jobs)
or the Alluxio workers connect to an Alluxio master,
they will initialize their own Alluxio configuration properties with the default values
supplied by the masters based on the master-side `${ALLUXIO_HOME}/conf/alluxio-site.properties` files.
As a result, cluster admins can put
client-side settings (e.g., `alluxio.user.*`), or
network transport settings (e.g., `alluxio.security.authentication.type`), or
worker settings (e.g., `alluxio.worker.*`)
in `${ALLUXIO_HOME}/conf/alluxio-site.properties` on all the masters,
which will be distributed and become cluster-wide default values for new Alluxio clients and workers.

For example, a common Alluxio property `alluxio.user.file.writetype.default` is default to
`MUST_CACHE` which only writes to Alluxio space. In an Alluxio cluster
deployment where data persistence is preferred and all jobs need to write through to both UFS and Alluxio, with Alluxio v1.8 or later the admin can simply add
`alluxio.user.file.writetype.default=CACHE_THROUGH` to all the masters'
`${ALLUXIO_HOME}/conf/alluxio-site.properties`. After restarting the cluster, all the new jobs will
automatically set property `alluxio.user.file.writetype.default` to `CACHE_THROUGH` as its default
value.

Clients can still ignore or overwrite the cluster-wide default values, by either
specifying the user property `alluxio.user.conf.cluster.default.enabled=false` to
decline loading the cluster-wide default values or
following the approaches described in
[Configure Alluxio for Applications](Configuration-Settings.html#configure-applications) to
overwrite the same properties.

> Note that, before v1.8, `${ALLUXIO_HOME}/conf/alluxio-site.properties` file is only loaded by
Alluxio server
> processes and will be ignored by applications interacting with Alluxio service through Alluxio
client,
> unless `${ALLUXIO_HOME}/conf` is on applications' classpath.

## Configuration Sources

An Alluxio property can be possibly configured in multiple sources. In this case, its final value
is determined by the following priority list (from highest priority to lowest):

1. [JVM system properties (i.e., `-Dproperty=key`)](http://docs.oracle.com/javase/jndi/tutorial/beyond/env/source.html#SYS)
2. [Environment variables](#use-environment-variables)
3. [Property files](#use-site-property-files-recommended). When an Alluxio cluster starts, each
server process including master and worker searches `alluxio-site.properties` in a list paths of
`${HOME}/.alluxio/`, `/etc/alluxio/` and `${ALLUXIO_HOME}/conf` in order, and will skip the
remaining paths once this `alluxio-site.properties` file is found.
4. [Cluster default values](#specify-cluster-wide-defaults). An Alluxio client may initialize its
configuration based on the cluster-wide default configuration served by the masters.

If no above user-specified configuration is found for a property, Alluxio runtime will fallback to
its [default property value](Configuration-Properties.html).

To check the value of a specific configuration property and the source of its value, users can use
the following commandline:

```bash
$ bin/alluxio getConf alluxio.worker.port
29998
$ bin/alluxio getConf --source alluxio.worker.port
DEFAULT
```

To list all of the configuration properties with sources:

```bash
$ bin/alluxio getConf --source
alluxio.conf.dir=/Users/bob/alluxio/conf (SYSTEM_PROPERTY)
alluxio.debug=false (DEFAULT)
...
```

Users can also specify `--master` option to list all of the cluster-wide defaults configuration properties
served the masters. Note that, with `--master` option `getConf` will query the master and thus require
the master nodes running; without `--master` option this command only checks the local configuration.

```bash
$ bin/alluxio getConf --master --source
alluxio.conf.dir=/Users/bob/alluxio/conf (SYSTEM_PROPERTY)
alluxio.debug=false (DEFAULT)
...
```

## Server Configuration Checker

Server-side configuration checker helps discover configuration errors and warnings.
Suspected configuration errors are reported through the web UI, `doctor` CLI, and master logs.

The web UI shows the result of the server configuration check.

![webUi]({{ site.baseurl }}/img/screenshot_configuration_checker_webui.png)

Users can also run the `fsadmin doctor` command to get the same results.

```bash
$ bin/alluxio fsadmin doctor configuration
```

Configuration warnings can also be seen in the master logs.

![masterLogs]({{ site.baseurl }}/img/screenshot_configuration_checker_masterlogs.png)
