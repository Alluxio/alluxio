---
layout: global
title: Deploy Alluxio on a Cluster
nickname: Cluster
group: Deploying Alluxio
priority: 2
---

* Table of Contents
{:toc}

## Alluxio Single Master Cluster

The simplest way to deploy Alluxio on a cluster is to use one master. However, this single master becomes
the single point of failure (SPOF) in an Alluxio cluster: if that machine or process became unavailable,
the cluster as a whole would be unavailable. We highly recommend running Alluxio masters in
[High Availability](#alluxio-ha-cluster) mode in production.

### Prerequisites

* A single master node, and 1 or more worker nodes
* SSH login without password to all nodes. You can add a public SSH key for the host into
`~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html) for more details.
* A shared storage system to mount to Alluxio (accessible by all Alluxio nodes). For example, HDFS or Amazon S3.

### Setup

The following sections describe how to install and configure Alluxio with a single master in a cluster.

#### Alluxio download and installation

To deploy Alluxio in a cluster, first [download](https://alluxio.org/download) the Alluxio tar file,
and copy it to every node (master node, worker nodes). Extract the tarball to the same path on
every node.

#### Configuration

On the master node of the installation, create the `conf/alluxio-site.properties` configuration file from the template.

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

This configuration file (`conf/alluxio-site.properties`) is read by all Alluxio masters and Alluxio
workers. The configuration parameters which must be set are:
- `alluxio.master.hostname=<MASTER_HOSTNAME>`
  - This is set to the hostname of the master node.
  - Examples: `alluxio.master.hostname=1.2.3.4`, `alluxio.master.hostname=node1.a.com`
- `alluxio.master.mount.table.root.ufs=<STORAGE_URI>`
  - This is set to the URI of the shared storage system to mount to the Alluxio root. This shared
  shared storage system must be accessible by the master node and all worker nodes.
  - Examples: `alluxio.master.mount.table.root.ufs=hdfs://1.2.3.4:9000/alluxio/root/`, `alluxio.master.mount.table.root.ufs=s3a://bucket/dir/`

This is the minimal configuration to start Alluxio, but additional configuration may be added.

Since this same configuration file will be the same for the master and all the workers, this configuration
file must be copied to all the other Alluxio nodes. The simplest way to do this is
to use the `copyDir` shell command on the master node. In order to do so, add the IP addresses or
hostnames of all the worker nodes to the `conf/workers` file. Then run:

```bash
./bin/alluxio copyDir conf/
```

This will copy the `conf/` directory to all the workers specified in the `conf/workers` file. Once
this command suceeds, all the Alluxio nodes will be correctly configured.

#### Format Alluxio

Before Alluxio can be started for the first time, the journal must be formatted.

> Formatting the journal will delete all metadata from Alluxio. However, the data in under storage will be untouched.

On the master node, format Alluxio with the following command:

```bash
./bin/alluxio format
```

### Launch Alluxio

To start the Alluxio cluster, on the master node, make sure the `conf/workers` file is correct
with all the hostnames of the workers.

On the master node, start the Alluxio cluster with the following command:

```bash
./bin/alluxio-start.sh all SudoMount
```

This will start the master on the node you are running it on, and start all the workers on all the
nodes specified in the `conf/workers` file.

### Verify Alluxio Cluster

To verify that Alluxio is running, visit `http://<alluxio_master_hostname>:19999` to see the status
page of the Alluxio master.

Alluxio comes with a simple program that writes and reads sample files in Alluxio. Run the sample program with:

```bash
./bin/alluxio runTests
```

## Alluxio HA Cluster

High availability in Alluxio is based upon a multi-master approach, where multiple master processes
are running in the system. One of these processes is elected the leader and is used by all workers
and clients as the primary point of contact. The other masters act as standby, and use a shared
journal to ensure that they maintain the same file system metadata as the leader. The standby masters
do not serve any client or worker requests.

If the leader master fails, a standby master will automatically be chosen to take over and become the
new leader. Once the new leader starts serving, Alluxio clients and workers proceed as usual. Note
that during the failover to a standby master, clients may experience brief delays or transient errors.

Alluxio can either use an embedded journal or UFS-based journal for maintaining state across restarts. 
The embedded journal comes with its own leader election, while UFS journaling relies on Zookeeper for leader election.
See [journal management documentation]({{ '/en/operation/Journal.html' | relativize_url }}) for more information about choosing
and configuring Alluxio journal system. 

### Common Configurations

The common prerequisites of setting up HA cluster are:
* Multiple master nodes, and 1 or more worker nodes
* SSH login without password to all nodes. You can add a public SSH key for the host into
`~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html) for more details.
* A shared storage system to mount to Alluxio (accessible by all Alluxio nodes). For example, HDFS or Amazon S3. 

To deploy Alluxio in a cluster, first [download](https://alluxio.org/download) the Alluxio tar file,
and copy it to every node (master nodes, worker nodes). Extract the tarball to the same path on
every node.

Each Alluxio node (masters and workers) must be configured for HA. On each Alluxio node,
create the `conf/alluxio-site.properties` configuration file from the template.

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Add the following properties to the `conf/alluxio-site.properties` file:
- `alluxio.master.hostname=<MASTER_HOSTNAME>`
  - This must be set to the correct externally visible hostname for each master node.
  (Workers ignore this parameter when `alluxio.zookeeper.enabled` is enabled)
  - Examples: `alluxio.master.hostname=1.2.3.4`, `alluxio.master.hostname=node1.a.com`
- `alluxio.master.mount.table.root.ufs=<STORAGE_URI>`
  - This is set to the URI of the shared storage system to mount to the Alluxio root. This shared
  shared storage system must be accessible by all master nodes and all worker nodes.
  - Examples: `alluxio.master.mount.table.root.ufs=hdfs://1.2.3.4:9000/alluxio/root/`, `alluxio.master.mount.table.root.ufs=s3a://bucket/dir/`
  
### Setting up HA cluster with internal leader election

The minimal configuration to set up a HA cluster is to give the embedded journal addresses to all nodes inside the cluster.
Alluxio's internal leader election will determine the leader master. The default embedded journal port is `19200`.

```
alluxio.master.embedded.journal.addresses=master_hostname_1:19200,master_hostname_2:19200,master_hostname_3:19200
```

Note that embedded journal feature relies on Copycat which has built-in leader election. 
The built-in leader election cannot work with Zookeeper since we cannot have two leaders which might not match. 
Enabling embedded journal enables Alluxio's internal leader election. 
See [embedded journal configuration documentation]({{ '/en/operation/Journal.html' | relativize_url }}#Embedded-Journal-Configuration) 
for alternative ways to set up HA cluster with internal leader election.

### Setting up HA cluster with Zookeeper-based leader election

The additional prerequisites of setting up Zookeeper HA cluster are:
* A [ZooKeeper](http://zookeeper.apache.org/) cluster. Alluxio masters use ZooKeeper for leader election,
and Alluxio clients and workers use ZooKeeper to inquire about the identity of the current leader master.
* A shared storage system on which to place the journal (accessible by all Alluxio nodes). The leader
master writes to the journal on this shared storage system, while the standby masters continually
replay the journal entries to stay up-to-date. 
The journal storage system is recommended to be:
  - Highly available. All metadata modifications on the master requires writing to the journal, so any
  downtime of the journal storage system will directly impact the Alluxio master availability.
  - Filesystem, not an object store. The Alluxio master writes to journal files to this storage
  system, and utilizes filesystem operations such as rename and flush. Object stores do not support
  these operations, and/or perform them slowly, so when the journal is stored on an object store,
  the Alluxio master operation throughput is significantly reduced.

The configuration parameters which must be set are:
- `alluxio.zookeeper.enabled=true`
  - This enables the HA mode for the masters, and informs workers that HA mode is enabled.
- `alluxio.zookeeper.address=<ZOOKEEPER_ADDRESS>`
  - The ZooKeeper address must be specified when `alluxio.zookeeper.enabled` is enabled.
  - The HA masters use ZooKeeper for leader election, and the workers use ZooKeeper to discover the leader master.
  - Multiple ZooKeeper addresses can be specified by delimiting with commas
  - Examples: `alluxio.zookeeper.address=1.2.3.4:2181`, `alluxio.zookeeper.address=zk1:2181,zk2:2181,zk3:2181`
- `alluxio.master.journal.type=UFS`
  - This uses UFS as the journal place. Note that Zookeeper cannot work 
  with journal type `EMBEDDED` (use a journal embedded in the masters).
- `alluxio.master.journal.folder=<JOURNAL_URI>`
  - This is set to the URI of the shared journal location for the Alluxio leader master to write the journal to,
  and for standby masters to replay journal entries from. This shared shared storage system must be
  accessible by all master nodes.
  - Examples: `alluxio.master.journal.folder=hdfs://1.2.3.4:9000/alluxio/journal/`

Make sure all master nodes and all worker nodes have configured their respective
`conf/alluxio-site.properties` configuration file appropriately.

Once all the Alluxio masters and workers are configured in this way, Alluxio is ready to
be formatted started.

### Format Alluxio

Before Alluxio can be started for the first time, the journal must be formatted.

> Formatting the journal will delete all metadata from Alluxio. However, the data in under storage will be untouched.

On any master node, format Alluxio with the following command:

```bash
./bin/alluxio format
```

### Launch Alluxio

To start the Alluxio cluster with the provided scripts, on any master node, list all the
worker hostnames in the `conf/workers` file, and list all the masters in the `conf/masters` file. This
will allow the start scripts to start the appropriate processes on the appropriate nodes.

On the master node, start the Alluxio cluster with the following command:

```bash
./bin/alluxio-start.sh all SudoMount
```

This will start Alluxio masters on all the nodes specified in `conf/masters`, and start the workers on all the
nodes specified in `conf/workers`.

### Verify Alluxio Cluster

To verify that Alluxio is running, you can visit the web UI of the leader master. To determine the
leader master, run:

```bash
./bin/alluxio fs leader
```

Then, visit `http://<LEADER_HOSTNAME>:19999` to see the status page of the Alluxio leader master.

Alluxio comes with a simple program that writes and reads sample files in Alluxio. Run the sample program with:

```bash
./bin/alluxio runTests
```

### Configure Alluxio Clients for HA

When an Alluxio client interacts with Alluxio in HA mode, the client must know about the Alluxio HA 
cluster, so that the client knows how to discover the Alluxio leader master. There are 2 ways to configure the client for Alluxio HA.

#### HA Configuration Parameters

When connecting to Alluxio HA cluster using internal leader election,
Alluxio clients need the `alluxio.master.rpc.addresses` information to decide the node addresses
to query to find out the Alluxio leader master which is serving rpc requests.
- `alluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998`

When connecting to Alluxio HA cluster using Zookeeper-based leader election,
the following properties are needed to connect to Zookeeper to get the leader master information.

- `alluxio.zookeeper.enabled=true`
- `alluxio.zookeeper.address=<ZOOKEEPER_ADDRESS>`
  - The ZooKeeper address must be specified when `alluxio.zookeeper.enabled` is enabled.
  - Multiple ZooKeeper addresses can be specified by delimiting with commas

When the application is configured with these parameters, the Alluxio URI can be simplified to
`alluxio:///path`, since the HA cluster information is already configured.

For example, if using Hadoop, you can configure the properties in `core-site.xml`, and then use the
Hadoop CLI with an Alluxio URI.

```bash
hadoop fs -ls alluxio:///directory
```

#### HA Authority

Users can fully specify the HA cluster information in the URI to connect to an Alluxio HA cluster.
Configuration derived from the HA authority takes precedence over all other forms of configuration, e.g. site properties or environment variables.

When using internal leader election, use `alluxio://master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998`
When using Zookeeper leader election, use `alluxio://zk@<ZOOKEEPER_ADDRESS>`.

For many applications (e.g., Hadoop, HBase, Hive and Flink), you can use a comma as the
delimiter for multiple addresses in the URI, like `alluxio://master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998` 
and `alluxio://zk@zkHost1:2181,zkHost2:2181,zkHost3:2181/path`.

For some other applications (e.g., Spark), you need to use semicolons as the delimiter for multiple
addresses, like `alluxio://master_hostname_1:19998;master_hostname_2:19998;master_hostname_3:19998` 
and `alluxio://zk@zkHost1:2181;zkHost2:2181;zkHost3:2181/path`. 

## Operations

Below are common operations to perform on an Alluxio cluster.

### Modify Server Configuration

In order to update the server configuration for a node, you have to update the `conf/alluxio-site.properties`
file on that node, stop the server, and then start the server. The configuration file is read on startup.

### Stop or Restart Alluxio

To stop an Alluxio cluster, the simplest way is to use the provided tools. To use the tools, list all the
worker hostnames in the `conf/workers` file, and list all the masters in the `conf/masters` file.
Then, you can stop Alluxio with:

```bash
./bin/alluxio-stop.sh all
```

This will stop all the processes on all nodes listed in `conf/workers` and `conf/masters`.

You can stop just the masters and just the workers with the following commands:

```bash
./bin/alluxio-stop.sh masters # stops all masters in conf/masters
./bin/alluxio-stop.sh workers # stops all workers in conf/workers
```

If you do not want to use `ssh` to login to all the nodes and stop all the processes, you can run
commands on each node individually to stop each component. For any node, you can stop a master or worker with:

```bash
./bin/alluxio-stop.sh master # stops the local master
./bin/alluxio-stop.sh worker # stops the local worker
```

Starting Alluxio is similar. If `conf/workers` and `conf/masters` are both populated, you can start
the cluster with:

```bash
./bin/alluxio-start.sh all SudoMount
```

You can start just the masters and just the workers with the following commands:

```bash
./bin/alluxio-start.sh masters           # starts all masters in conf/masters
./bin/alluxio-start.sh workers SudoMount # starts all workers in conf/workers
```

If you do not want to use `ssh` to login to all the nodes and start all the processes, you can run
commands on each node individually to start each component. For any node, you can start a master or worker with:

```bash
./bin/alluxio-start.sh master           # starts the local master
./bin/alluxio-start.sh worker SudoMount # starts the local worker
```

### Format the Journal

On any master node, format the Alluxio journal with the following command:

```bash
./bin/alluxio format
```

> Formatting the journal will delete all metadata from Alluxio. However, the data in under storage will be untouched.

### Add/Remove Workers

Adding a worker to an Alluxio cluster is as simple as starting a new Alluxio worker process, with the
appropriate configuration. In most cases, the new worker's configuration should be the same as all
the other workers' configuration. Once the worker is started, it will register itself with the Alluxio
master, and become part of the Alluxio cluster.

Removing a worker is as simple as stopping the worker process. Once the worker is stopped, and after
a timeout on the master (configured by master parameter `alluxio.master.worker.timeout`), the master
will consider the worker as "lost", and no longer consider it as part of the cluster.

### Add/Remove Masters

In order to add a master, the Alluxio cluster must be in HA mode. If you are running the cluster as
a single master cluster, you must configure it to be an HA cluster to be able to have more than 1
master.

#### Alluxio HA cluster with internal leader election

When internal leader election is used, Alluxio masters are determined. Adding or removing 
master nodes requires:

* Shut down the whole cluster
* Add/remove one or more Alluxio master
* Format the whole cluster
* Update the cluster-wide embedded journal configuration
* Start the whole cluster

Note that all previously stored data and metadata in Alluxio filesystem will be erased.

#### Alluxio HA cluster with Zookeeper election

To add a master to an HA Alluxio cluster, you can simply start a new Alluxio master process, with
the appropriate configuration. The configuration for the new master should be the same as other masters,
except that the parameter `alluxio.master.hostname=<MASTER_HOSTNAME>` should reflect the new hostname.
Once the new master is started, it will start interacting with ZooKeeper to participate in leader election.

Removing a master is as simple as stopping the master process. If the cluster is a single master cluster,
stopping the master will essentially shutdown the cluster, since the single master is down. If the
Alluxio cluster is an HA cluster, stopping the leader master will force ZooKeeper to elect a new leader master
and failover to that new leader. If a standby master is stopped, then the operation of the cluster is
unaffected. Keep in mind, Alluxio masters high availability depends on the availability on standby
masters. If there are not enough standby masters, the availability of the leader master will be affected.
It is recommended to have at least 3 masters for an HA Alluxio cluster.
