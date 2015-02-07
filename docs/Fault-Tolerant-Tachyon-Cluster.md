---
layout: global
title: Fault Tolerant Tachyon Cluster
---

# Fault Tolerance

Fault Tolerance in Tachyon is based upon a multi-master approach where multiple master processes
are run. One of these processes is elected the leader and is used by all workers and clients as the
primary point of contact. The other masters act as standbys using the shared journal to ensure that
they maintain the same file system metadata as the leader and can rapidly take over in the event of
the leader failing.

If the leader fails a new leader is automatically selected from the available standby masters and
Tachyon proceeds as usual. Note that while the switchover to a standby master happens clients may
experience brief delays or transient errors.

# Prerequisites

There are two prerequisites to set up a fault tolerant Tachyon cluster:

* [ZooKeeper](Fault-Tolerant-Tachyon-Cluster#zookeeper)
* A shared reliable under filesystem on which to place the journal

Currently [HDFS](Fault-Tolerant-Tachyon-Cluster#hdfs), Amazon S3, or GlusterFS can be used as under
filesystem layers. Also, please see [Configuration Settings](Configuration-Settings.html) for a more
succinct description of all the configuration options Tachyon has.

## HDFS

For information about setting up HDFS, see
[Getting Started With Hadoop](http://wiki.apache.org/hadoop/GettingStartedWithHadoop).

Note the name of machine running your NameNode, as you will need to tell Tachyon where this is. In
your tachyon-env.sh (or environment) you'll need to include:

    export TACHYON_UNDERFS_ADDRESS=hdfs://[namenodeserver]:[namenodeport]

## ZooKeeper

Tachyon uses ZooKeeper to achieve master fault tolerance. It is also required in order to use shared
storage (such as HDFS) for writing logs and images.

ZooKeeper must be set up independently (see
[ZooKeeper Getting Started](http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html))
and then in `conf/tachyon-env.sh`, these java options should be used:

<table class="table">
<tr><th>Property Name</th><th>Example</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.usezookeeper</td>
  <td>true</td>
  <td>
     Whether or not Master processes should use ZooKeeper.
  </td>
</tr>
<tr>
  <td>tachyon.zookeeper.address</td>
  <td>localhost:2181</td>
  <td>
    The hostname and port ZooKeeper is running on.
  </td>
</tr>
</table>

## Configuring Tachyon

Once you have HDFS and ZooKeeper running, you need to set up your `tachyon-env.sh` appropriately on
each host. Some settings are relevant for Master Nodes, while others for Workers. Here we separate
these concerns, however it is also fine to run a Master and Worker(s) on a single node.

## Externally Visible Address

In the following sections we refer to an "externally visible address". This is simply the address of
an interface on the machine being configured that can be seen by other nodes in the Tachyon cluster.
On EC2, you should the `ip-x-x-x-x` address. In particular, don't use `localhost` or `127.0.0.1`, as
other nodes will then be unable to reach your node.

## Master Configuration

For a master node the ZooKeeper and HDFS variables must be set, as described above.

In addition, the following variable must also be set:

    export TACHYON_MASTER_ADDRESS=[externally visible address of this machine]

Finally, configure your `TACHYON_JAVA_OPTS` to include:

    -Dtachyon.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/tachyon/journal

You can then start a master node on the machine and it will either become leader, or wait until the
current master dies and then offer to be the new leader.

## Worker Configuration

For a worker, it is only necessary to set the `TACHYON_MASTER_ADDRESS` option as:

    export TACHYON_MASTER_ADDRESS=[address of one of the master nodes in the system]

Any master address can be used, when configured to use Zookeeper workers will look up the
current leaders address and use that as the master to connect to.