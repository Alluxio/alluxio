---
layout: global
title: Deploy Alluxio on a Cluster with HA
nickname: Cluster with HA
group: Deploying Alluxio
priority: 2.5
---

* Table of Contents
{:toc}


## Overview

An Alluxio service with High availability (or HA) achieved by running multiple Alluxio master processes
on different nodes in the system. One of these masters is elected the **leading master** which serves all
workers and clients as the primary point of contact. The other master processes act as **standby
masters** which maintain the same file system state with the leading master by trailing
a shared journal. Note that, standby masters do not serve any client or worker requests; however,
if the leading master fails, one standby master will automatically be chosen to take over and become
the new leading master. Once the new leading master starts serving, Alluxio clients and workers
proceed as usual. During the failover to a standby master, clients may experience brief
delays or transient errors.

The major challenges here to achieve HA are maintaining the shared file system state
across service restarts and maintain consensus among masters on **leading master**
after failover. In Alluxio 2.0, there are two different ways to achieve these two goals:

- [Option1](#option1-raft-based-embedded-journal): Use an internal replicated state machine based on RAFT to
 both store the file system
journal and select leading masters. This approach is introduced in Alluxio 2.0 and requires no
dependency on external services.
- [Option2](#option2-zookeeper-and-shared-journal-storage):
Leverage an external Zookeeper service for leader election on the leading master
and a shared storage (e.g., the root UFS) for shared journal.
See [journal management documentation]({{ '/en/operation/Journal.html' | relativize_url }}) for more information about choosing and configuring Alluxio journal system.

## Prerequisites

* To deploy a Alluxio cluster, first [download](https://alluxio.io/download) the pre-compiled Alluxio
  binary file, extract the tarball and copy the extracted directory to all nodes (including nodes
  running masters and workers).
* Enable SSH login without password from master node to worker nodes. You can add a public SSH key for
  the host into `~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html) for more details.
* TCP traffic across all nodes is allowed. For basic functionality make sure RPC port (default :19998) is open
  on all nodes.

## Basic Setup
### Option1: Raft-based Embedded Journal

The minimal configuration to set up a HA cluster is to give the embedded journal addresses to
all nodes inside the cluster. On each Alluxio node, create the `conf/alluxio-site.properties` configuration file from the template.

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Add the following properties to the `conf/alluxio-site.properties` file:

```
alluxio.master.hostname=<MASTER_HOSTNAME> # Only needed on master node
alluxio.master.mount.table.root.ufs=<STORAGE_URI>
alluxio.master.embedded.journal.addresses=<EMBEDDED_JOURNAL_ADDRESS>
```

Explanation:
- The first property `alluxio.master.hostname=<MASTER_HOSTNAME>` is required on each master node
to be its externally visible hostname. This parameter is ignored on workers.
  Examples include `alluxio.master.hostname=1.2.3.4`, `alluxio.master.hostname=node1.a.com`.
- The second property `alluxio.master.mount.table.root.ufs=<STORAGE_URI>` sets the URI of the shared storage system to mount to the Alluxio root. This shared
  shared storage system must be accessible by all master nodes and all worker nodes.
  Examples include `alluxio.master.mount.table.root.ufs=hdfs://1.2.3.4:9000/alluxio/root/`, `alluxio.master.mount.table.root.ufs=s3://bucket/dir/`
- The third property `alluxio.master.embedded.journal.addresses` sets the sets of masters to
  participate Alluxio's internal leader election and determine the leading master. The default
  embedded journal port is `19200`. An example: `alluxio.master.embedded.journal.addresses=master_hostname_1:19200,master_hostname_2:19200,master_hostname_3:19200`

Note that embedded journal feature relies on [Copycat](https://github.com/atomix/copycat) which has built-in leader election.
The built-in leader election cannot work with Zookeeper since we cannot have two leaders which might not match.
Enabling embedded journal enables Alluxio's internal leader election.
See [embedded journal configuration documentation]({{ '/en/operation/Journal.html' | relativize_url }}#embedded-journal-configuration)
for more details and alternative ways to set up HA cluster with internal leader election.

### Option2: Zookeeper and Shared Journal Storage

The additional prerequisites of setting up Zookeeper HA cluster are:
1. A [ZooKeeper](http://zookeeper.apache.org/) cluster. Alluxio masters use ZooKeeper for leader election,
and Alluxio clients and workers use ZooKeeper to inquire about the identity of the current leading master.
1. A shared storage system on which to place the journal (accessible by all Alluxio masters). The leading
master writes to the journal on this shared storage system, while the standby masters continually
replay the journal entries to stay up-to-date.
The journal storage system is recommended to be:
  - Highly available. All metadata modifications on the master requires writing to the journal, so any
  downtime of the journal storage system will directly impact the Alluxio master availability.
  - Filesystem, not an object store. The Alluxio master writes to journal files to this storage
  system, and utilizes filesystem operations such as rename and flush. Object stores do not support
  these operations, and/or perform them slowly, so when the journal is stored on an object store,
  the Alluxio master operation throughput is significantly reduced.

The minimal configuration parameters which must be set are:

```
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=<ZOOKEEPER_ADDRESS>
alluxio.master.journal.type=UFS
alluxio.master.journal.folder=<JOURNAL_URI>
```

Explanation:
- Property `alluxio.zookeeper.enabled=true` enables the HA mode for the masters, and informs
workers that HA mode is enabled.
- Property `alluxio.zookeeper.address=<ZOOKEEPER_ADDRESS>` sets the ZooKeeper address when `alluxio.zookeeper.enabled` is enabled. The HA masters use ZooKeeper for leader election, and the workers use ZooKeeper to
  discover the leading master. Multiple ZooKeeper addresses can be specified by delimiting with commas.
  Examples include `alluxio.zookeeper.address=1.2.3.4:2181`, `alluxio.zookeeper.address=zk1:2181,
  zk2:2181,zk3:2181`
- Property `alluxio.master.journal.type=UFS` indicates UFS is used as the journal place. Note that
Zookeeper cannot work with journal type `EMBEDDED` (use a journal embedded in the masters).
- Property `alluxio.master.journal.folder=<JOURNAL_URI>` sets the URI of the shared journal location for the Alluxio leading master to write the journal to,
  and for standby masters to replay journal entries from. This shared shared storage system must be
  accessible by all master nodes.
  Examples include `alluxio.master.journal.folder=hdfs://1.2.3.4:9000/alluxio/journal/`

For clusters with large namespaces, increased CPU overhead on leader could cause delays on Zookeeper client heartbeats.
For this reason, we recommend setting Zookeeper client session timeout to at least 2 minutes on large clusters with namespace
size more than several hundred millions of files.
- `alluxio.zookeeper.session.timeout=120s`
  - Zookeeper server's tick time must also be configured as such to allow
    this timeout. The current implementation requires that the timeout be a minimum of 2 times the tickTime (as set in the server configuration)
    and a maximum of 20 times the tickTime.

Make sure all master nodes and all worker nodes have configured their respective
`conf/alluxio-site.properties` configuration file appropriately.

Once all the Alluxio masters and workers are configured in this way, Alluxio is ready to
be formatted started.

## Start an Alluxio Cluster with HA

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

To verify that Alluxio is running, you can visit the web UI of the leading master. To determine the
leading master, run:

```bash
./bin/alluxio fs leader
```

Then, visit `http://<LEADER_HOSTNAME>:19999` to see the status page of the Alluxio leading master.

Alluxio comes with a simple program that writes and reads sample files in Alluxio. Run the sample program with:

```bash
./bin/alluxio runTests
```

## Access an Alluxio Cluster with HA

When an application interacts with Alluxio in HA mode, the client must know about
the Alluxio HA cluster, so that the client knows how to discover the Alluxio leading master.
There are two ways to specify the HA Alluxio service address on the client side:

### Pre-configured Applications with Configuration Parameters

- When connecting to an Alluxio HA cluster using embedded journal,
Alluxio clients need the `alluxio.master.rpc.addresses` information to decide the node addresses
to query to find out the Alluxio leading master which is serving rpc requests.
  - Examples: `alluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,
 master_hostname_3:19998`

- When connecting to an Alluxio HA cluster using Zookeeper,
the following properties are needed to connect to Zookeeper to get the leading master information.
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

### Setting Alluxio Service with URL Authority {#ha-authority}

Users can fully specify the HA cluster information in the URI to connect to an Alluxio HA cluster.
Configuration derived from the HA authority takes precedence over all other forms of configuration, e.g. site properties or environment variables.

- When using embedded journal, use `alluxio://master_hostname_1:19998,
master_hostname_2:19998,master_hostname_3:19998`
- When using Zookeeper leader election, use `alluxio://zk@<ZOOKEEPER_ADDRESS>`.

For many applications (e.g., Hadoop, HBase, Hive and Flink), you can use a comma as the
delimiter for multiple addresses in the URI, like `alluxio://master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998`
and `alluxio://zk@zkHost1:2181,zkHost2:2181,zkHost3:2181/path`.

For some other applications (e.g., Spark), you need to use semicolons as the delimiter for multiple
addresses, like `alluxio://master_hostname_1:19998;master_hostname_2:19998;master_hostname_3:19998`
and `alluxio://zk@zkHost1:2181;zkHost2:2181;zkHost3:2181/path`.

## Common Operations

Below are common operations to perform on an Alluxio cluster.

### Stop Alluxio

To stop an Alluxio service, run:

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

### Restart Alluxio

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

### Add/Remove Workers Dynamically

Adding a worker to an Alluxio cluster dynamically is as simple as starting a new Alluxio worker
process, with the appropriate configuration. In most cases, the new worker's configuration should be the same as all
the other workers' configuration. Run the following command on the new worker to add

```bash
./bin/alluxio-start.sh worker SudoMount # starts the local worker
```

Once the worker is started, it will register itself with the Alluxio master, and become part of the Alluxio cluster.

Removing a worker is as simple as stopping the worker process.

```bash
./bin/alluxio-stop.sh worker # stops the local worker
```

Once the worker is stopped, and after
a timeout on the master (configured by master parameter `alluxio.master.worker.timeout`), the master
will consider the worker as "lost", and no longer consider it as part of the cluster.

### Add/Remove Masters

In order to add a master, the Alluxio cluster must be in HA mode. If you are running the cluster as
a single master cluster, you must configure it to be an HA cluster to be able to have more than 1
master.

#### Alluxio HA cluster with embedded journal

When internal leader election is used, Alluxio masters are determined. Adding or removing
master nodes requires:

* Shut down the whole cluster
* Add/remove one or more Alluxio master
* Format the whole cluster
* Update the cluster-wide embedded journal configuration
* Start the whole cluster

Note that all previously stored data and metadata in Alluxio filesystem will be erased.

#### Alluxio HA cluster with Zookeeper and Shared Journal

To add a master to an HA Alluxio cluster, you can simply start a new Alluxio master process, with
the appropriate configuration. The configuration for the new master should be the same as other masters,
except that the parameter `alluxio.master.hostname=<MASTER_HOSTNAME>` should reflect the new hostname.
Once the new master is started, it will start interacting with ZooKeeper to participate in leader election.

Removing a master is as simple as stopping the master process. If the cluster is a single master cluster,
stopping the master will essentially shutdown the cluster, since the single master is down. If the
Alluxio cluster is an HA cluster, stopping the leading master will force ZooKeeper to elect a new leading master
and failover to that new leader. If a standby master is stopped, then the operation of the cluster is
unaffected. Keep in mind, Alluxio masters high availability depends on the availability on standby
masters. If there are not enough standby masters, the availability of the leading master will be affected.
It is recommended to have at least 3 masters for an HA Alluxio cluster.

### Modify Configuration

In order to update the service configuration, you must first [stop the service](#stop-alluxio),
update the `conf/alluxio-site.properties` file on master node,
copy the file to all nodes (e.g., using `bin/alluxio copyDir conf/`),
and then [restart the service](#restart-alluxio).

However, if you only need to update some local configuration for a worker (e.g., change the mount
of storage capacity allocated to this worker or update the storage directory), the master node does
not need to be stopped and restarted. One can simply stop the local worker, update the configuration
(e.g., `conf/alluxio-site.properties`) file on this worker, and then restart the worker.
