---
layout: global
title: Alluxio Standalone with Fault Tolerance
nickname: Alluxio Standalone with Fault Tolerance
group: User Guide
priority: 3
---

Fault Tolerance in Alluxio is based upon a multi-master approach where multiple master processes
are running. One of these processes is elected the leader and is used by all workers and clients as the
primary point of contact. The other masters act as standbys using the shared journal to ensure that
they maintain the same file system metadata as a new leader and can rapidly take over in the event
of the leader failing.

If the leader fails, a new leader is automatically selected from the available standby masters and
Alluxio proceeds as usual. Note that while the switchover to a standby master happens clients may
experience brief delays or transient errors.

## Prerequisites

There are two prerequisites for setting up a fault tolerant Alluxio cluster:

* [ZooKeeper](http://zookeeper.apache.org/)
* A shared reliable under filesystem on which to place the journal

Alluxio requires ZooKeeper for fault tolerance, for leader selection. This ensures that there is at
most one leader master for Alluxio at any given time.

Alluxio also requires a shared under filesystem on which to place the journal. This shared
filesystem must be accessible by all the masters, and possible options include
[HDFS](Configuring-Alluxio-with-HDFS.html), [Amazon S3](Configuring-Alluxio-with-S3.html), or
[GlusterFS](Configuring-Alluxio-with-GlusterFS.html). The leader master writes the journal to the
shared filesystem, while the other (standby) masters continually replay the journal entries to stay
up-to-date.

### ZooKeeper

Alluxio uses ZooKeeper to achieve master fault tolerance. Alluxio masters use ZooKeeper for leader
election. Alluxio clients also use ZooKeeper to inquire about the identity and address of the
current leader.

ZooKeeper must be set up independently
(see [ZooKeeper Getting Started](http://zookeeper.apache.org/doc/r3.4.5/zookeeperStarted.html)).

After ZooKeeper is deployed, note the address and port for configuring Alluxio below.

### Shared Filesystem for Journal

Alluxio requires a shared filesystem to store the journal. All masters must be able to read and
write to the shared filesystem. Only the leader master will be writing to the journal at any given
time, but all masters read the shared journal to replay the state of Alluxio.

The shared filesystem must be set up independently from Alluxio, and should already be running
before Alluxio is started.

For example, if using HDFS for the shared journal, you need to note address and port running your
NameNode, as you will need to configure Alluxio below.

## Configuring Alluxio

Once you have ZooKeeper and your shared filesystem running, you need to set up your `alluxio-env.sh`
appropriately on each host.

### Externally Visible Address

In the following sections we refer to an "externally visible address". This is simply the address of
an interface on the machine being configured that can be seen by other nodes in the Alluxio cluster.
On EC2, you should use the `ip-x-x-x-x` address. In particular, don't use `localhost` or `127.0.0.1`, as
other nodes will then be unable to reach your node.

### Configuring Fault Tolerant Alluxio

In order to enable fault tolerance for Alluxio, additional configuration settings must be set for
Alluxio masters, workers, and clients. In `conf/alluxio-env.sh`, these java options must be set:

<table class="table">
<tr><th>Property Name</th><th>Value</th><th>Meaning</th></tr>
{% for item in site.data.table.java-options-for-fault-tolerance %}
<tr>
  <td>{{item.PropertyName}}</td>
  <td>{{item.Value}}</td>
  <td>{{site.data.table.en.java-options-for-fault-tolerance.[item.PropertyName]}}</td>
</tr>
{% endfor %}
</table>

To set these options, you can configure your `ALLUXIO_JAVA_OPTS` to include:

    -Dalluxio.zookeeper.enabled=true
    -Dalluxio.zookeeper.address=[zookeeper_hostname]:2181

If you are using a cluster of ZooKeeper nodes, you can specify multiple addresses by separating them
with commas, like:

    -Dalluxio.zookeeper.address=[zookeeper_hostname1]:2181,[zookeeper_hostname2]:2181,[zookeeper_hostname3]:2181

Alternatively, these configuration settings can be set in the `alluxio-site.properties` file. More
details about setting configuration parameters can be found in
[Configuration Settings](Configuration-Settings.html).

### Master Configuration

In addition to the above configuration settings, Alluxio masters need additional configuration. The
following variable must be set appropriately in `conf/alluxio-env.sh`:

    export ALLUXIO_MASTER_ADDRESS=[externally visible address of this machine]

Also, specify the correct journal folder by setting `alluxio.master.journal.folder` appropriately
for `ALLUXIO_JAVA_OPTS`. For example, if you are using HDFS for the journal, you can add:

    -Dalluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/path/to/alluxio/journal

Once all the Alluxio masters are configured in this way, they can all be started for fault tolerant
Alluxio. One of the masters will become the leader, and the others will replay the journal and wait
until the current master dies.

### Worker Configuration

As long as the config parameters above are correctly set, the worker will be able to consult with
ZooKeeper, and find the current leader master to connect to. Therefore, `ALLUXIO_MASTER_ADDRESS`
does not have to be set for the workers.

### Client Configuration

No additional configuration parameters are required for fault tolerant mode. As long as both:

    -Dalluxio.zookeeper.enabled=true
    -Dalluxio.zookeeper.address=[zookeeper_hostname]:2181

are set appropriately for your client application, the application will be able to consult with
ZooKeeper for the current leader master.
