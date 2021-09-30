---
layout: global
title: Deploy Alluxio on a Cluster with HA
nickname: Cluster with HA
group: Install Alluxio
priority: 3
---

* Table of Contents
{:toc}


## Overview

An Alluxio service with High availability (or HA) achieved by running multiple Alluxio master
processes on different nodes in the system.
One of these masters is elected the **leading master** which serves all workers and clients as the
primary point of contact.
The other master processes act as **standby masters** which maintain the same file system state with
the leading master by trailing a shared journal.
Standby masters do not serve any client or worker requests; however, if the leading master fails,
one standby master will automatically be chosen to take over and become the new leading master.
Once the new leading master starts serving, Alluxio clients and workers proceed as usual.
During the failover to a standby master, clients may experience brief delays or transient errors.

The major challenges here to achieving high-availability are maintaining the shared file system
state across service restarts and maintaining consensus among masters on **leading master** after
failover. In Alluxio 2.0, there are two different ways to achieve these two goals:

1. [Raft-based Journal](#raft-based-embedded-journal): Use an internal replicated state
  machine based on the Raft protocol to both store the file system journal and select a leading
  master.
  This approach is introduced in Alluxio 2.0 and requires no dependency on external services.
1. [Zookeeper with a shared Journal](#zookeeper-and-shared-journal-storage):
  Leverage an external Zookeeper service for leader election on the leading master and a shared
  storage (e.g., the root UFS) for shared journal.
  See [journal management documentation]({{ '/en/operation/Journal.html' | relativize_url }}) for
  more information about choosing and configuring Alluxio journal system.

## Prerequisites

* To deploy an Alluxio cluster, first [download](https://alluxio.io/download) the pre-compiled
  Alluxio binary file, extract the tarball and copy the extracted directory to all nodes (including
  nodes running masters and workers).
* Enable SSH login without password from all master nodes to all worker nodes. You can add a public SSH key
  for the host into `~/.ssh/authorized_keys`. See
  [this tutorial](http://www.linuxproblem.org/art_9.html) for more details.
* TCP traffic across all nodes is allowed. For basic functionality make sure RPC port (default :19998) is open
  on all nodes.

## Basic Setup
### Raft-based Embedded Journal

The minimal configuration to set up a HA cluster is to give the embedded journal addresses to all
nodes inside the cluster.
On each Alluxio node, create the `conf/alluxio-site.properties` configuration file from the
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Add the following properties to the `conf/alluxio-site.properties` file:

```properties
alluxio.master.hostname=<MASTER_HOSTNAME> # Only needed on master node
alluxio.master.mount.table.root.ufs=<STORAGE_URI>
alluxio.master.embedded.journal.addresses=<EMBEDDED_JOURNAL_ADDRESS>
```

Explanation:
- The first property `alluxio.master.hostname=<MASTER_HOSTNAME>` is required on each master node
  to be its own externally visible hostname.
  This is required on each individual component of the master quorum to have its own address set.
  On worker nodes, this parameter will be ignored.
  Examples include `alluxio.master.hostname=1.2.3.4`, `alluxio.master.hostname=node1.a.com`.
- The second property `alluxio.master.mount.table.root.ufs=<STORAGE_URI>` sets the URI of the
  shared under store to mount to the root of the Alluxio namespace Alluxio.
  This shared under store must be accessible by all master nodes and all worker nodes.
  Examples include `alluxio.master.mount.table.root.ufs=hdfs://1.2.3.4:9000/alluxio/root/` or `alluxio.master.mount.table.root.ufs=s3://bucket/dir/`
- The third property `alluxio.master.embedded.journal.addresses` sets the sets of masters to
  participate Alluxio's internal leader election and determine the leading master.
  The default embedded journal port is `19200`.
  An example: `alluxio.master.embedded.journal.addresses=master_hostname_1:19200,master_hostname_2:19200,master_hostname_3:19200`

Note that embedded journal feature relies on [Ratis](https://github.com/apache/incubator-ratis) which uses
leader election based on the Raft protocol and has its own format for storing journal entries.
The built-in leader election cannot work with Zookeeper since the journal formats between these
configuration may not match.
Enabling embedded journal enables Alluxio's internal leader election.
See [embedded journal configuration documentation]({{ '/en/operation/Journal.html' | relativize_url }}#embedded-journal-configuration)
for more details and alternative ways to set up HA cluster with internal leader election.

### Zookeeper and Shared Journal Storage

When using Zookeeper for HA there are additional prerequisites:
1. A [ZooKeeper](http://zookeeper.apache.org/) cluster. Alluxio masters use ZooKeeper for leader election,
and Alluxio clients and workers use ZooKeeper to inquire about the identity of the current leading master.
1. A shared storage system on which to place the journal (accessible by all Alluxio masters). The leading
master writes to the journal on this shared storage system, while the standby masters continually
replay the journal entries to stay up-to-date.
The journal storage system is recommended to be:
  - Highly available. All metadata modifications on the master requires writing to the journal, so any
  downtime of the journal storage system will directly impact the Alluxio master availability.
  - A filesystem, not an object store. The Alluxio master writes to journal files to this storage
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
- `alluxio.zookeeper.enabled=true` enables the HA mode for the masters, and informs
workers that HA mode is enabled.
- `alluxio.zookeeper.address=<ZOOKEEPER_ADDRESS>` sets the ZooKeeper address when
  `alluxio.zookeeper.enabled` is enabled.
  The HA masters use ZooKeeper for leader election, and the workers use ZooKeeper to discover the
  leading master.
  Multiple ZooKeeper addresses can be specified by delimiting with commas.
  Examples include `alluxio.zookeeper.address=1.2.3.4:2181`, `alluxio.zookeeper.address=zk1:2181,
  zk2:2181,zk3:2181`
- `alluxio.master.journal.type=UFS` indicates UFS is used as the journal place.
  Note that Zookeeper cannot work with journal type `EMBEDDED` (use a journal embedded in the
  masters).
- `alluxio.master.journal.folder=<JOURNAL_URI>` sets the URI of the shared journal location for the
  Alluxio leading master to write the journal to, and for standby masters to replay journal entries
  from.
  This shared shared storage system must be accessible by all master nodes.
  Examples include `alluxio.master.journal.folder=hdfs://1.2.3.4:9000/alluxio/journal/` or
  `alluxio.master.journal.folder=/mnt/nfs/journal/`.

Make sure all master nodes and all worker nodes have configured their respective
`conf/alluxio-site.properties` configuration file appropriately.

Once all the Alluxio masters and workers are configured in this way, Alluxio is ready to
be formatted and started.

#### Advanced Zookeeper setup

For clusters with large namespaces, increased CPU overhead on leader could cause delays on Zookeeper
client heartbeats. For this reason, we recommend setting Zookeeper client session timeout to at
least 2 minutes on large clusters with namespace size more than several hundred million of files.
- `alluxio.zookeeper.session.timeout=120s`
  - The Zookeeper server's min/max session timeout values must also be configured as such to allow
    this timeout.
    The defaults requires that the timeout be a minimum of 2 times the `tickTime` (as set in the
    server configuration) and a maximum of 20 times the `tickTime`.
    You could also manually configure `minSessionTimeout` and `maxSessionTimeout`.

Alluxio supports pluggable error handling policy on Zookeeper leader election.
- `alluxio.zookeeper.leader.connection.error.policy` specifies how connection errors are handled.
It can be either `SESSION` or `STANDARD`. It is set `SESSION` as default.
 
- The `SESSION` policy makes use of Zookeeper sessions to determine whether leader state is dirty. 
  This means suspended connections won't trigger stepping down of a current leader as long as it was
  able to reestablish the zookeeper connection with the same session.
  It provides more stability in maintaining the leadership state.
- The `STANDARD` policy treats any interruption to zookeeper server as an error. 
  Thus leader will step down upon missing a heartbeat, even though its internal zookeeper session
  was still intact with the zookeeper server.
  It provides more security against bugs and issues in zookeeper setup.

## Start an Alluxio Cluster with HA

### Format Alluxio

Before Alluxio can be started for the first time, the Alluxio master journal and worker storage must be formatted.

> Formatting the journal will delete all metadata from Alluxio.
> Formatting the worker storage will delete all data from the configured Alluxio storage.
> However, the data in under storage will be untouched.

On all the Alluxio master nodes, list all the worker hostnames in the `conf/workers` file, and list all the masters in the `conf/masters` file. 
This will allow alluxio scripts to run operations on the cluster nodes.
`format` Alluxio cluster with the following command in one of the master nodes:

```console
$ ./bin/alluxio format
```

### Launch Alluxio

In one of the master nodes, start the Alluxio cluster with the following command:

```console
$ ./bin/alluxio-start.sh all SudoMount
```

This will start Alluxio masters on all the nodes specified in `conf/masters`, and start the workers
on all the nodes specified in `conf/workers`.
Argument `SudoMount` indicates to mount the RamFS on each worker using `sudo` privilege, if it is
not already mounted.

### Verify Alluxio Cluster

To verify that Alluxio is running, you can visit the web UI of the leading master. To determine the
leading master, run:

```console
$ ./bin/alluxio fs leader
```

Then, visit `http://<LEADER_HOSTNAME>:19999` to see the status page of the Alluxio leading master.

Alluxio comes with a simple program that writes and reads sample files in Alluxio. Run the sample
program with:

```console
$ ./bin/alluxio runTests
```

## Access an Alluxio Cluster with HA

When an application interacts with Alluxio in HA mode, the client must know about
the connection information of Alluxio HA cluster, so that the client knows how to discover the Alluxio leading master.
The following sections list three ways to specify the HA Alluxio service address on the client side.

### Specify Alluxio Service in Configuration Parameters or Java Options

Users can pre-configure the service address of an Alluxio HA cluster in environment variables
or site properties, and then connect to the service using an Alluxio URI such as `alluxio:///path`.
For example, with Alluxio connection information in `core-site.xml` of Hadoop, Hadoop CLI can 
connect to the Alluxio cluster.

```console
$ hadoop fs -ls alluxio:///directory
```

Depending on the different approaches to achieve HA, different properties are required:

If using embedded journal, set `alluxio.master.rpc.addresses`.

```
alluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```

Or specify the properties in Java option. For example, for Spark applications, add the following to 
`spark.executor.extraJavaOptions` and `spark.driver.extraJavaOptions`:

```
-Dalluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```

If using Zookeeper, set the following Zookeeper related properties  
```
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=<ZOOKEEPER_ADDRESS>
```

Note that, the ZooKeeper address (`alluxio.zookeeper.address`) must be specified when
`alluxio.zookeeper.enabled` is enabled and vise versa.
Multiple ZooKeeper addresses can be specified by delimiting with commas.

### Specify Alluxio Service with URL Authority {#ha-authority}

Users can also fully specify the HA cluster information in the URI to connect to an Alluxio HA cluster.
Configuration derived from the HA authority takes precedence over all other forms of configuration,
e.g. site properties or environment variables.

- When using embedded journal, use `alluxio://master_hostname_1:19998,
master_hostname_2:19998,master_hostname_3:19998/path`
- When using Zookeeper leader election, use `alluxio://zk@<ZOOKEEPER_ADDRESS>/path`.

For many applications (e.g., Hadoop, HBase, Hive and Flink), you can use a comma as the
delimiter for multiple addresses in the URI, like
`alluxio://master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998/path`
and `alluxio://zk@zkHost1:2181,zkHost2:2181,zkHost3:2181/path`.

For some other applications (e.g., Spark) where comma is not accepted inside a URL authority, you
need to use semicolons as the delimiter for multiple addresses,
like `alluxio://master_hostname_1:19998;master_hostname_2:19998;master_hostname_3:19998`
and `alluxio://zk@zkHost1:2181;zkHost2:2181;zkHost3:2181/path`.

### Specify Alluxio Service with logical URL Authority 

Some frameworks may not accept either of these ways to connect to a highly available Alluxio HA cluster,
so Alluxio also supports connecting to an Alluxio HA cluster via a logical name. In order to use logical
names, the following configuration options need to be set in your environment variables or site properties.

#### Use logical name when using embedded journal

If you are using embedded journal, you need to configure the following configuration options and connect
to the highly available alluxio node via `alluxio://ebj@[logical-name]` , for example
`alluxio://ebj@my-alluxio-cluster`.

* alluxio.master.nameservices.[logical-name] unique identifier for each alluxio master node

A comma-separated ID of the alluxio master node that determine all the alluxio master nodes in the cluster.
For example, if you previously used `my-alluxio-cluster` as the logical name and wanted to
use `master1,master2,master3` as individual IDs for each alluxio master, you configure this as such:

```
alluxio.master.nameservices.my-alluxio-cluster=master1,master2,master3
```

* alluxio.master.rpc.address.[logical name]. [master node ID]  RPC Address for each alluxio master node

For each alluxio master node previously configured, set the full address of each alluxio master node, for example:

```
alluxio.master.rpc.address.my-alluxio-cluster.master1=master1:19998
alluxio.master.rpc.address.my-alluxio-cluster.master2=master2:19998
alluxio.master.rpc.address.my-alluxio-cluster.master3=master3:19998
```

#### Use logical name when using Zookeeper

If you are using zookeeper for leader election, you need to configure the following values and connect to
the highly available alluxio node via `alluxio://zk@[logical-name]` , for example `alluxio://zk@my-alluxio-cluster`.

* alluxio.master.zookeeper.nameservices.[logical-name] unique identifier for each Zookeeper node

A comma-separated zookeeper node ID that determine all the Zookeeper nodes in the cluster. For example,
if you previously used `my-alluxio-cluster` as the logical name and wanted to use `node1,node2,node3` as individual
IDs for each Zookeeper, you would configure this as such:

```
alluxio.master.zookeeper.nameservices.my-alluxio-cluster=node1,node2,node3
```

* alluxio.master.zookeeper.address.[logical-domain]. [Zookeeper node ID] Address foreach Zookeeper node
  

For each Zookeeper node previously configured, set the full address of each Zookeeper node, for example:

```
alluxio.master.zookeeper.address.my-alluxio-cluster.node1=host1:2181
alluxio.master.zookeeper.address.my-alluxio-cluster.node2=host2:2181
alluxio.master.zookeeper.address.my-alluxio-cluster.node3=host3:2181
```

## Common Operations

Below are common operations to perform on an Alluxio cluster.

### Stop Alluxio

To stop an Alluxio service, run:

```console
$ ./bin/alluxio-stop.sh all
```

This will stop all the processes on all nodes listed in `conf/workers` and `conf/masters`.

You can stop just the masters and just the workers with the following commands:

```console
$ ./bin/alluxio-stop.sh masters # stops all masters in conf/masters
$ ./bin/alluxio-stop.sh workers # stops all workers in conf/workers
```

If you do not want to use `ssh` to login to all the nodes and stop all the processes, you can run
commands on each node individually to stop each component.
For any node, you can stop a master or worker with:

```console
$ ./bin/alluxio-stop.sh master # stops the local master
$ ./bin/alluxio-stop.sh worker # stops the local worker
```

### Restart Alluxio

Starting Alluxio is similar. If `conf/workers` and `conf/masters` are both populated, you can start
the cluster with:

```console
$ ./bin/alluxio-start.sh all
```

You can start just the masters and just the workers with the following commands:

```console
$ ./bin/alluxio-start.sh masters # starts all masters in conf/masters
$ ./bin/alluxio-start.sh workers # starts all workers in conf/workers
```

If you do not want to use `ssh` to login to all the nodes and start all the processes, you can run
commands on each node individually to start each component. For any node, you can start a master or
worker with:

```console
$ ./bin/alluxio-start.sh master # starts the local master
$ ./bin/alluxio-start.sh worker # starts the local worker
```

### Add/Remove Workers Dynamically

Adding a worker to an Alluxio cluster dynamically is as simple as starting a new Alluxio worker
process, with the appropriate configuration.
In most cases, the new worker's configuration should be the same as all the other workers' configuration.
Run the following command on the new worker to add

```console
$ ./bin/alluxio-start.sh worker SudoMount # starts the local worker
```

Once the worker is started, it will register itself with the Alluxio leading master and become part of the Alluxio cluster.

Removing a worker is as simple as stopping the worker process.

```console
$ ./bin/alluxio-stop.sh worker # stops the local worker
```

Once the worker is stopped, and after
a timeout on the master (configured by master parameter `alluxio.master.worker.timeout`), the master
will consider the worker as "lost", and no longer consider it as part of the cluster.

### Add/Remove Masters

In order to add a master, the Alluxio cluster must operate in HA mode.
If you are running the cluster as a single master cluster, you must configure it to be an HA cluster
before having more than one master.

See the [journal management documentation]({{ '/en/operation/Journal.html' | relativize_url }}) for
more information about adding and removing masters.

### Update Master-side Configuration

In order to update the master-side configuration, you can first [stop the service](#stop-alluxio),
update the `conf/alluxio-site.properties` file on master node,
and then [restart the service](#restart-alluxio).
Note that, this approach introduces downtime of the Alluxio service.

Alternatively, one benefit of running Alluxio in HA mode is to use rolling restarts
to minimize downtime when updating configurations:

1. Update the master configuration on all the master nodes without restarting any master.
1. Restart the leading master (can be determined by running `bin/alluxio leader`).
  A new leading master will be elected to continue servicing requests.
1. Wait for the previous leading master to come up successfully but as a standby master.
1. Update and restart all remaining standby masters.
1. Verify the configuration update

### Update Worker-side Configuration

If you only need to update some local configuration for a worker (e.g., change the mount
of storage capacity allocated to this worker or update the storage directory), the master node does
not need to be stopped and restarted.
Simply stop the desired worker, update the configuration
(e.g., `conf/alluxio-site.properties`) file on that node, and then restart the process.
