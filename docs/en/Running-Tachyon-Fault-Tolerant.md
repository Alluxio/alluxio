---
layout: global
title: Tachyon Standalone with Fault Tolerance
nickname: Tachyon Standalone with Fault Tolerance
group: User Guide
priority: 3
---

Fault Tolerance in Tachyon is based upon a multi-master approach where multiple master processes
are run. One of these processes is elected the leader and is used by all workers and clients as the
primary point of contact. The other masters act as standbys using the shared journal to ensure that
they maintain the same file system metadata as a new leader and can rapidly take over in the event
of the leader failing.

If the leader fails, a new leader is automatically selected from the available standby masters and
Tachyon proceeds as usual. Note that while the switchover to a standby master happens clients may
experience brief delays or transient errors.

## Prerequisites

There are two prerequisites for setting up a fault tolerant Tachyon cluster:

* [ZooKeeper](http://zookeeper.apache.org/)
* A shared reliable under filesystem on which to place the journal

Tachyon requires ZooKeeper for fault tolerance, for leader selection. This ensures that there is at
most one leader master for Tachyon at any given time.

Tachyon also requires a shared under filesystem on which to place the journal. This shared
filesystem must be accessible by all the masters, and possible options include
[HDFS](Configuring-Tachyon-with-HDFS.html), [Amazon S3](Configuring-Tachyon-with-S3.html), or
[GlusterFS](Configuring-Tachyon-with-GlusterFS.html). The leader master writes the journal to the
shared filesystem, while the other (standby) masters continually replay the journal entries to stay
up-to-date.

### ZooKeeper

Tachyon uses ZooKeeper to achieve master fault tolerance. Tachyon masters use ZooKeeper for leader
election. Tachyon clients also use ZooKeeper to inquire about the identity and address of the
current leader.

ZooKeeper must be set up independently
(see [ZooKeeper Getting Started](http://zookeeper.apache.org/doc/r3.4.5/zookeeperStarted.html)).

After ZooKeeper is deployed, note the address and port for configuring Tachyon below.

### Shared Filesystem for Journal

Tachyon requires a shared filesystem to store the journal. All masters must be able to read and
write to the shared filesystem. Only the leader master will be writing to the journal at any given
time, but all masters read the shared journal to replay the state of Tachyon.

The shared filesystem must be set up independently from Tachyon, and should already be running
before Tachyon is started.

For example, if using HDFS for the shared journal, you need to note address and port running your
NameNode, as you will need to configure Tachyon below.

## Configuring Tachyon

Once you have ZooKeeper and your shared filesystem running, you need to set up your `tachyon-env.sh`
appropriately on each host.

### Externally Visible Address

In the following sections we refer to an "externally visible address". This is simply the address of
an interface on the machine being configured that can be seen by other nodes in the Tachyon cluster.
On EC2, you should use the `ip-x-x-x-x` address. In particular, don't use `localhost` or `127.0.0.1`, as
other nodes will then be unable to reach your node.

### Configuring Fault Tolerant Tachyon

In order to enable fault tolerance for Tachyon, additional configuration settings must be set for
Tachyon masters, workers, and clients. In `conf/tachyon-env.sh`, these java options must be set:

<table class="table">
<tr><th>Property Name</th><th>Value</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.zookeeper.enabled</td>
  <td>true</td>
  <td>
     If true, masters will use ZooKeeper to enable fault tolerance.
  </td>
</tr>
<tr>
  <td>tachyon.zookeeper.address</td>
  <td>[zookeeper_hostname]:2181</td>
  <td>
    The hostname and port ZooKeeper is running on. Separate multiple addresses with commas.
  </td>
</tr>
</table>

To set these options, you can configure your `TACHYON_JAVA_OPTS` to include:

    -Dtachyon.zookeeper.enabled=true
    -Dtachyon.zookeeper.address=[zookeeper_hostname]:2181

If you are using a cluster of ZooKeeper nodes, you can specify multiple addresses by separating them
with commas, like:

    -Dtachyon.zookeeper.address=[zookeeper_hostname1]:2181,[zookeeper_hostname2]:2181,[zookeeper_hostname3]:2181

Alternatively, these configuration settings can be set in the `tachyon-site.properties` file. More
details about setting configuration parameters can be found in
[Configuration Settings](Configuration-Settings.html).

### Master Configuration

In addition to the above configuration settings, Tachyon masters need additional configuration. The
following variable must be set appropriately in `conf/tachyon-env.sh`:

    export TACHYON_MASTER_ADDRESS=[externally visible address of this machine]

Also, specify the correct journal folder by setting `tachyon.master.journal.folder` appropriately
for `TACHYON_JAVA_OPTS`. For example, if you are using HDFS for the journal, you can add:

    -Dtachyon.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/path/to/tachyon/journal

Once all the Tachyon masters are configured in this way, they can all be started for fault tolerant
Tachyon. One of the masters will become the leader, and the others will replay the journal and wait
until the current master dies.

### Worker Configuration

As long as the config parameters above are correctly set, the worker will be able to consult with
ZooKeeper, and find the current leader master to connect to. Therefore, `TACHYON_MASTER_ADDRESS`
does not have to be set for the workers.

### Client Configuration

No additional configuration parameters are required for fault tolerant mode. As long as both:

    -Dtachyon.zookeeper.enabled=true
    -Dtachyon.zookeeper.address=[zookeeper_hostname]:2181

are set appropriately for your client application, the application will be able to consult with
ZooKeeper for the current leader master.
