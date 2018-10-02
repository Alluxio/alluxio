---
layout: global
title: Deploy Alluxio using YARN
nickname: Deploy Alluxio with YARN
group: Deploying Alluxio
priority: 6
---

This document discusses how to run Alluxio in a YARN environment. There are two
approaches: [running Alluxio alongside YARN](#running-alluxio-alongside-yarn), and
[running Alluxio as an application on top of YARN](#running-alluxio-as-a-yarn-application).
We support both approaches, but recommend running Alluxio alongside YARN for a few reasons:

- Alluxio servers are long-running daemons like HDFS. HDFS is not supposed to run on YARN.
- Alluxio's architecture is designed to have one worker per host, but this is not
well supported by YARN.
- Running Alluxio on top of YARN adds the additional complexity of managing Alluxio's
ApplicationMaster.
- Multi-master high availability mode is not currently supported by our YARN integration.
- The YARN integration does not support restarting workers in case they crash.

## Running Alluxio Alongside YARN

Alluxio should be run alongside YARN so that all YARN nodes have access to a local Alluxio worker.
For YARN and Alluxio to play nicely together on the same nodes, you must partition the node's
memory and compute resources between YARN and Alluxio. To do this, first determine how much memory
and cpu will be needed by Alluxio, then subtract those resources from what YARN would otherwise use.

### Memory

In `yarn-site.xml`, set

```
yarn.nodemanager.resource.memory-mb = Total RAM - Other services RAM - Alluxio RAM
```

On Alluxio worker nodes, Alluxio RAM usage is 1GB plus the ramdisk size configured by
`alluxio.worker.memory.size`.
On Alluxio master nodes, Alluxio RAM usage is proportional to the number of files. Allocate at
least 1GB. 32GB is recommended for a production deployment.

### CPU vcores

In `yarn-site.xml`, set

```
yarn.nodemanager.resource.cpu-vcores = Total cores - Other services vcores - Alluxio vcores
```

On Alluxio worker nodes, Alluxio needs only one or two vcores.
On Alluxio master nodes, we recommend at least 4 vcores.

### Restart YARN

After updating the YARN configuration, restart YARN so that it picks up the changes:

```bash
$ ${HADOOP_HOME}/sbin/stop-yarn.sh
$ ${HADOOP_HOME}/sbin/start-yarn.sh
```

## Running Alluxio as a YARN Application

This section goes over how to run Alluxio servers (master and workers) on a Yarn
cluster. In this mode, you run an Alluxio ApplicationMaster which communicates with
YARN to request containers for launching the master and workers.

### Prerequisites

**A running YARN cluster**

**[Alluxio downloaded locally](https://www.alluxio.org/download)**

### Launch Alluxio

Use the script `integration/yarn/bin/alluxio-yarn.sh` to start Alluxio. This script takes three arguments:

1. The total number of Alluxio workers to start. (required)
2. An HDFS path to distribute the binaries for Alluxio ApplicationMaster. (required)
3. The Yarn name for the node on which to run the Alluxio Master (optional, defaults to `${ALLUXIO_MASTER_HOSTNAME}`)

For example, to launch an Alluxio cluster with 3 worker nodes, where an HDFS temp directory is
`hdfs://${HDFS_MASTER}:9000/tmp/` and the master hostname is `${ALLUXIO_MASTER}`, you would run

```bash
$ # If Yarn does not reside in `HADOOP_HOME`, set the environment variable `YARN_HOME` to the base path of Yarn.
$ export HADOOP_HOME=<path to hadoop home>
$ ${HADOOP_HOME}/bin/hadoop fs -mkdir hdfs://${HDFS_MASTER}:9000/tmp
$ ${ALLUXIO_HOME}/integration/yarn/bin/alluxio-yarn.sh 3 hdfs://${HDFS_MASTER}:9000/tmp/ ${ALLUXIO_MASTER}
```

You may also start the Alluxio Master node separately from Yarn in which case the above startup will
automatically detect the Master at the address provided and skip initialization of a new instance.
This is useful if you have a particular host you'd like to run the Master on, which isn't part of
your Yarn cluster, like an AWS EMR Master Instance.

The script will launch an Alluxio Application Master on Yarn, which will then request containers for the
Alluxio master and workers. You can check the YARN UI in the browser to watch the status of the
Alluxio job.

Running the script will produce output containing something like

```
INFO impl.YarnClientImpl: Submitted application application_1445469376652_0002
```

This application ID can be used to destroy the application by running

```bash
$ ${HADOOP_HOME}/bin/yarn application -kill application_1445469376652_0002
```

The ID can also be found in the YARN web UI.

### Verify the Cluster

Once you have the Alluxio application running, you can access the web UI at
master_hostname:19999. To test IO functionality, configure
`alluxio.master.hostname=master_hostname` in `conf/alluxio-site.properties` and run

```bash
$ ${ALLUXIO_HOME}/bin/alluxio runTests
```

### Configuration

To customize Alluxio master and worker with specific properties (e.g., tiered storage setup on each
worker), see [Configuration settings](Configuration-Settings.html). To ensure your configuration can be
read by both the ApplicationMaster and Alluxio master/workers, put `alluxio-site.properties` in
`/etc/alluxio/alluxio-site.properties`.

### Troubleshooting

If Alluxio doesn't come up after running `alluxio-yarn.sh`, check the YARN application logs
to see what happened. Some common issues to look out for:

- **Not enough memory/cpu**: Make sure you have enough YARN capacity to run the Alluxio master
and all workers. The CPU requirements are configured by `alluxio.integration.master.resource.cpu`
and `alluxio.integration.worker.resource.cpu` (both `1` by default). Memory requirements are
configured by `alluxio.integration.master.resource.mem` and `alluxio.integration.worker.resource.mem`,
both `1024MB` by default. Alluxio will avoid placing multiple workers on the same node.
- **Hostname mismatch**: The hostname you specify for the Alluxio master must exactly
match the hostname used by YARN for one of the hosts in the cluster. If you pass an IP address but
YARN is using hostnames, the master cannot be started.

The Alluxio [user mailing list](https://groups.google.com/forum/#!forum/alluxio-users) is
a good resource for getting help if the log messages aren't enough.

## Configure yarn.application.classpath

To enable your YARN applications to communicate with Alluxio servers, add the alluxio
client jar to each YARN node, either by adding it to a directory on your
`yarn.application.classpath`, or updating the classpath to include the client jar.
For example,

```
<property>
 <name>yarn.application.classpath</name>
 <value>$HADOOP_CONF_DIR,
 $HADOOP_COMMON_HOME/share/hadoop/common/*,
 $HADOOP_COMMON_HOME/share/hadoop/common/lib/*,
 $HADOOP_HDFS_HOME/share/hadoop/hdfs/*,
 $HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,
 $HADOOP_YARN_HOME/share/hadoop/yarn/*,
 $HADOOP_YARN_HOME/share/hadoop/yarn/lib/*,
 $ALLUXIO_HOME/client/*</value>
</property>
```

Make sure to replace `$ALLUXIO_HOME` with the path that you've installed Alluxio to.
