---
layout: global
title: Running Alluxio on a Cluster
nickname: Alluxio on Cluster
group: Deploying Alluxio
priority: 2
---

* Table of Contents
{:toc}

## Running Alluxio with a Single Master

The simplest way to deploy Alluxio on a cluster is to use one master. However, this single master becomes
the single point of failure (SPOF) in an Alluxio cluster: if that machine or process became unavailable,
the cluster as a whole would be unavailable. We highly recommend running Alluxio masters in
[High Availability](#running-alluxio-with-high-availability) mode in production.

### Download Alluxio

To deploy Alluxio in a cluster, first download the Alluxio tar file, and extract it in every node.
To deploy Alluxio in a cluster, first [download](https://alluxio.org/download) the Alluxio tar file, and
extract it in every node.

### Configure Alluxio

In the `${ALLUXIO_HOME}/conf` directory, create the `conf/alluxio-site.properties` configuration
file from the template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Update `alluxio.master.hostname` in `conf/alluxio-site.properties` to the hostname of the machine
you plan to run Alluxio Master on. Add the IP addresses or hostnames of all the worker nodes to the
`conf/workers` file. You cannot use local file system as Alluxio's under storage system if there
are multiple nodes in the cluster. Instead you need to set up a shared storage to which all Alluxio servers
have access. The shared storage can be network file system (NFS), HDFS, S3, and so on. For example,
you can refer to [Configuring Alluxio with S3](Configuring-Alluxio-with-S3.html) and follow its
instructions to set up S3 as Alluxio's under storage.

Finally, sync all the information to the worker nodes. You can use

{% include Running-Alluxio-on-a-Cluster/sync-info.md %}

to sync files and folders to all hosts specified in the `alluxio/conf/workers` file. If you have
downloaded and extracted Alluxio tar file on the master only, you can use the `copyDir` command
to sync the entire extracted Alluxio directory to workers. You can also use this command
to sync any change to `conf/alluxio-site.properties` to the workers.

### Start Alluxio

Now, you can start Alluxio:

{% include Running-Alluxio-on-a-Cluster/start-Alluxio.md %}

To verify that Alluxio is running, you can visit `http://<alluxio_master_hostname>:19999`, check the
log in the directory `alluxio/logs`, or run a sample program:

{% include Running-Alluxio-on-a-Cluster/run-tests.md %}

**Note**: If you are using EC2, make sure the security group settings on the master node allows
 incoming connections on the alluxio web UI port.

## Running Alluxio with High Availability

High availability in Alluxio is based upon a multi-master approach, where multiple master processes
are running in the system. One of these processes is elected the leader and is used by all workers
and clients as the primary point of contact. The other masters act as standby, and use the shared
journal to ensure that they maintain the same file system metadata as the leader. A standby master can
 rapidly take over in the event of the leader failing.

If the leader fails, a new leader is automatically selected from the available standby masters and
Alluxio proceeds as usual. Note that while the failover to a standby master happens clients may
experience brief delays or transient errors.

### Prerequisites

There are two prerequisites for setting up a Alluxio cluster with high availability:

* [ZooKeeper](http://zookeeper.apache.org/)
* A shared reliable under filesystem on which to place the journal

Alluxio requires ZooKeeper for leader selection. This ensures that there is at most one leader
master for Alluxio at any given time.

Alluxio also requires a shared under filesystem on which to place the journal. This shared
filesystem must be accessible by all the masters, and possible options include
[HDFS](Configuring-Alluxio-with-HDFS.html) and
[GlusterFS](Configuring-Alluxio-with-GlusterFS.html). The leader master writes the journal to the
shared filesystem, while the other (standby) masters continually replay the journal entries to stay
up-to-date.

#### ZooKeeper

Alluxio uses ZooKeeper to achieve master high availability. Alluxio masters use ZooKeeper for leader
election. Alluxio clients also use ZooKeeper to inquire about the identity and address of the
current leader.

ZooKeeper must be set up independently
(see [ZooKeeper Getting Started](http://zookeeper.apache.org/doc/r3.4.5/zookeeperStarted.html)).

After ZooKeeper is deployed, note the address and port for configuring Alluxio below.

#### Shared Filesystem for Journal

Alluxio requires a shared filesystem to store the journal. All masters must be able to read and
write to the shared filesystem. Only the leader master will be writing to the journal at any given
time, but all masters read the shared journal to replay the state of Alluxio.

The shared filesystem must be set up independently from Alluxio, and should already be running
before Alluxio is started.

For example, if using HDFS for the shared journal, you need to note address and port running your
NameNode, as you will need to configure Alluxio below.

### Configuring Alluxio

Once you have ZooKeeper and your shared filesystem running, you need to set up your `alluxio-site.properties`
appropriately on each host.

#### Externally Visible Address

In the following sections we refer to an "externally visible address". This is simply the address of
an interface on the machine being configured that can be seen by other nodes in the Alluxio cluster.
On EC2, you should use the `ip-x-x-x-x` address. In particular, don't use `localhost` or `127.0.0.1`, as
other nodes will then be unable to reach your node.

#### Configuring Fault Tolerant Alluxio

In order to configure high availability for Alluxio, additional configuration settings must be set for
Alluxio masters, workers, and clients. In `conf/alluxio-site.properties`, these java options must be set:

<table class="table">
<tr><th>Property Name</th><th>Value</th><th>Meaning</th></tr>
{% for item in site.data.table.java-options-for-fault-tolerance %}
<tr>
  <td>{{item.PropertyName}}</td>
  <td>{{item.Value}}</td>
  <td>{{site.data.table.en.java-options-for-fault-tolerance[item.PropertyName]}}</td>
</tr>
{% endfor %}
</table>

To set these options, you can configure your `conf/alluxio-site.properties` to include:

    alluxio.zookeeper.enabled=true
    alluxio.zookeeper.address=[zookeeper_hostname]:2181

If you are using a cluster of ZooKeeper nodes, you can specify multiple addresses by separating them
with commas, like:

    alluxio.zookeeper.address=[zookeeper_hostname1]:2181,[zookeeper_hostname2]:2181,[zookeeper_hostname3]:2181

#### Master Configuration

In addition to the above configuration settings, Alluxio masters need additional configuration. For
each master, the following variable must be set appropriately in `conf/alluxio-site.properties`:

    alluxio.master.hostname=[externally visible address of this machine]

Also, specify the correct journal folder by setting `alluxio.master.journal.folder` appropriately in
`conf/alluxio-site.properties`. For example, if you are using HDFS for the journal, you can add:

    alluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/path/to/alluxio/journal

In addition, be sure to list all the masters' addresses in `conf/masters`. This allows the
startup script to start Alluxio master processes on the appropriate machines. 

Once all the Alluxio masters are configured in this way, they can all be started to achieve highly
available Alluxio. One of the masters will become the leader, and the others will replay the journal
and wait until the current master dies.

#### Worker Configuration

As long as the config parameters above are correctly set, the worker will be able to consult with
ZooKeeper, and find the current leader master to connect to. Therefore, `alluxio.master.hostname`
does not have to be set for the workers.

#### Client Configuration

No additional configuration parameters are required for high availability mode. As long as both:

```properties
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=[zookeeper_hostname]:2181
```

are set appropriately for your client application, the application will be able to consult with
ZooKeeper for the current leader master.

##### HDFS API

When communicating with Alluxio in HA mode using the HDFS API, ensure the client side zookeeper
configuration is properly set. Use `alluxio://` for the scheme but the host and port may be omitted.
Any host provided in the URL is ignored; `alluxio.zookeeper.address` is used instead for finding the
Alluxio leader master.

```
hadoop fs -ls alluxio:///directory
```

#### Automatic Fail Over

To test automatic fail over, ssh into current Alluxio master leader, and find process ID of
the `AlluxioMaster` process with:

```bash
$ jps | grep AlluxioMaster
```

Then kill the leader with:

```bash
$ kill -9 <leader pid found via the above command>
```

Then you can see the leader with the following command:

```bash
$ ./bin/alluxio fs leader
```

The output of the command should show the new leader. You may need to wait for a moment for the
new leader to be elected.

Visit Alluxio web UI at `http://{NEW_LEADER_MASTER_IP}:{NEW_LEADER_MASTER_WEB_PORT}`. Click `Browse` in
the navigation bar, and you should see all files are still there.
