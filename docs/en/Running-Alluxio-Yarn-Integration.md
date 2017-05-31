---
layout: global
title: Running Alluxio YARN Integration
nickname: Alluxio YARN Integration
group: Deploying Alluxio
priority: 6
---

This guide explains the process for running Alluxio as an application in a YARN cluster. For a
self-contained tutorial on running Alluxio + YARN on EC2, see [this guide](Running-Alluxio-on-EC2.html).

Note: YARN is not well-suited for long-running applications such as Alluxio. We recommend
following [these instructions](Running-Alluxio-Yarn-Standalone.html) instead of running
Alluxio as a YARN application.

## Prerequisites

**A running YARN cluster**

**Alluxio downloaded locally**

```bash
$ curl http://downloads.alluxio.org/downloads/files/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz | tar xz
```

## Build YARN Integration

```bash
$ mvn clean install -Dhadoop.version=<your hadoop version> -Pyarn -Dlicense.skip -DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip
```

Make sure to replace <your hadoop version> with the version of Hadoop that you are using.

## Configuration

To customize Alluxio master and worker with specific properties (e.g., tiered storage setup on each
worker), see [Configuration settings](Configuration-Settings.html). To ensure your configuration can be
read by both the ApplicationMaster and Alluxio master/workers, put `alluxio-site.properties` in
`/etc/alluxio/alluxio-site.properties`.

If Yarn does not reside in `HADOOP_HOME`, set the environment variable `YARN_HOME` to the base path of Yarn.

## Run Alluxio Application

Use the script `integration/yarn/bin/alluxio-yarn.sh` to start Alluxio. This script takes three arguments:

1. The total number of Alluxio workers to start. (required)
2. An HDFS path to distribute the binaries for Alluxio ApplicationMaster. (required)
3. The Yarn name for the node on which to run the Alluxio Master (optional, defaults to `${ALLUXIO_MASTER_HOSTNAME}`)

For example, to launch an Alluxio cluster with 3 worker nodes, where an HDFS temp directory is
`hdfs://masterhost:9000/tmp/` and the master hostname is `masterhost`, you would run

```bash
$ export HADOOP_HOME=/hadoop
$ /hadoop/bin/hadoop fs -mkdir hdfs://masterhost:9000/tmp
$ /alluxio/integration/yarn/bin/alluxio-yarn.sh 3 hdfs://masterhost:9000/tmp/ masterhost
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
$ /hadoop/bin/yarn application -kill application_1445469376652_0002
```

The ID can also be found in the YARN web UI.

## Test Alluxio

Once you have the Alluxio application running, you can check its health by configuring
`alluxio.master.hostname=masterhost` in `conf/alluxio-site.properties` and running

```bash
$ /alluxio/bin/alluxio runTests
```
