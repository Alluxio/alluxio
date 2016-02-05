---
layout: global
title: Alluxio Standalone on a Cluster
nickname: Alluxio Standalone on a Cluster
group: User Guide
priority: 2
---

## Standalone cluster

First download the `Alluxio` tar file, and extract it.

{% include Running-Alluxio-on-a-Cluster/download-extract-Alluxio-tar.md %}

In the `alluxio/conf` directory, copy `alluxio-env.sh.template` to `alluxio-env.sh`. Make sure
`JAVA_HOME` points to a valid Java 6/7 installation. Update `ALLUXIO_MASTER_ADDRESS` to the hostname
of the machine you plan to run Alluxio Master on. Add the IP addresses of all the worker nodes to
the `alluxio/conf/workers` file. Finally, sync all the information to worker nodes. You can use

{% include Running-Alluxio-on-a-Cluster/sync-info.md %}

to sync files and folders to all hosts specified in the `alluxio/conf/workers` file.

Now, you can start Alluxio:

{% include Running-Alluxio-on-a-Cluster/start-Alluxio.md %}

To verify that Alluxio is running, you can visit
[http://alluxio.master.hostname:19999](http://alluxio.master.hostname:19999), check the log in the
direcotry `alluxio/logs`, or run a sample program:

{% include Running-Alluxio-on-a-Cluster/run-tests.md %}

**Note**: If you are using EC2, make sure the security group settings on the master node allows
 incoming connections on the alluxio web UI port.

## Using the bootstrap-conf argument to the bin/alluxio script

The alluxio script also contains logic to create a basic config for a cluster. If you run:

{% include Running-Alluxio-on-a-Cluster/bootstrap-conf.md %}

and there is no existing `alluxio/conf/alluxio-env.sh` file, then the script will create one
with the appropriate settings for a cluster with a master node running at `<alluxio_master_hostname>`.

This script needs to be run on each node you wish to configure.

The script will configure your workers to use 2/3 of the total memory on each worker. This amount
can be changed by editing the created `alluxio/conf/alluxio-env.sh` file on the worker.

## EC2 cluster with Spark

If you use Spark to launch an EC2 cluster, `Alluxio` will be installed and configured by default.
