---
layout: global
title: Install Alluxio Cluster with HA
---
## Overview

An Alluxio cluster with High Availability (HA) is achieved by running multiple Alluxio master
processes on different nodes in the system.
One master is elected as the **leading master** which serves all workers and clients as the
primary point of contact.
The other masters act as **standby masters** and maintain the same file system state as
the leading master by reading a shared journal.
Standby masters do not serve any client or worker requests; however, if the leading master fails,
one standby master will automatically be elected as the new leading master.
Once the new leading master starts serving, Alluxio clients and workers proceed as usual.
During the failover to a standby master, clients may experience brief delays or transient errors.

The major challenges to achieving high-availability are maintaining a shared file system
state across service restarts and maintaining consensus among masters about the identity of the 
**leading master** after failover. 
[Raft-based Journal](#raft-based-embedded-journal): Uses an internal replicated state
machine based on the [Raft protocol](https://raft.github.io/) to both store the file system journal and run
leader elections.
This approach is introduced in Alluxio 2.0 and requires no dependency on external services.


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

Alluxio admins can create and edit the properties file `conf/alluxio-site.properties` to
configure Alluxio masters or workers.
If this file does not exist, it can be copied from the template file under `${ALLUXIO_HOME}/conf`:

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Make sure that this file is distributed to `${ALLUXIO_HOME}/conf` on every Alluxio master
and worker before starting the cluster.
Restarting Alluxio processes is the safest way to ensure any configuration updates are applied.

### Raft-based Embedded Journal

The minimal configuration to set up a HA cluster is to give the embedded journal addresses to all
nodes inside the cluster.
On each Alluxio node, copy the `conf/alluxio-site.properties` configuration file and add the following properties to the file:

```properties
alluxio.master.hostname=<MASTER_HOSTNAME> # Only needed on master node
alluxio.master.embedded.journal.addresses=<EMBEDDED_JOURNAL_ADDRESS>
```

Explanation:
- The first property `alluxio.master.hostname=<MASTER_HOSTNAME>` is required on each master node
  to be its own externally visible hostname.
  This is required on each individual component of the master quorum to have its own address set.
  On worker nodes, this parameter will be ignored.
  Examples include `alluxio.master.hostname=1.2.3.4`, `alluxio.master.hostname=node1.a.com`.
- The second property `alluxio.master.embedded.journal.addresses` sets the sets of masters to
  participate Alluxio's internal leader election and determine the leading master.
  The default embedded journal port is `19200`.
  An example: `alluxio.master.embedded.journal.addresses=master_hostname_1:19200,master_hostname_2:19200,master_hostname_3:19200`

Note that embedded journal feature relies on [Ratis](https://github.com/apache/ratis) which uses
leader election based on the Raft protocol and has its own format for storing journal entries.
Enabling embedded journal enables Alluxio's internal leader election.
See [embedded journal configuration documentation]({{ '/en/operation/Journal.html' | relativize_url }}#configuring-embedded-journal)
for more details and alternative ways to set up HA cluster with internal leader election.

## Start an Alluxio Cluster with HA

### Format Alluxio

Before Alluxio can be started for the first time, the Alluxio master journal and worker storage must be formatted.

> Formatting the journal will delete all metadata from Alluxio.
> Formatting the worker storage will delete all data from the configured Alluxio storage.
> However, the data in under storage will be untouched.

On all the Alluxio master nodes, list all the worker hostnames in the `conf/workers` file, and list all the masters in the `conf/masters` file. 
This will allow alluxio scripts to run operations on the cluster nodes.
`init format` Alluxio cluster with the following command in one of the master nodes:

```shell
$ ./bin/alluxio init format
```

### Launch Alluxio

In one of the master nodes, start the Alluxio cluster with the following command:

```shell
$ ./bin/alluxio process start all
```

This will start Alluxio masters on all the nodes specified in `conf/masters`, and start the workers
on all the nodes specified in `conf/workers`.

### Verify Alluxio Cluster

To verify that Alluxio is running, you can visit the web UI of the leading master. To determine the
leading master, run:

```shell
$ ./bin/alluxio info report
```

Then, visit `http://<LEADER_HOSTNAME>:19999` to see the status page of the Alluxio leading master.

Alluxio comes with a simple program that writes and reads sample files in Alluxio. Run the sample
program with:

```shell
$ ./bin/alluxio exec basicIOTest
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

```shell
$ hadoop fs -ls alluxio:///directory
```

Depending on the different approaches to achieve HA, different properties are required:

If using embedded journal, set `alluxio.master.rpc.addresses`.

```properties
alluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```

Or specify the properties in Java option. For example, for Spark applications, add the following to 
`spark.executor.extraJavaOptions` and `spark.driver.extraJavaOptions`:

```properties
-Dalluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```

### Specify Alluxio Service with URL Authority {#ha-authority}

Users can also fully specify the HA cluster information in the URI to connect to an Alluxio HA cluster.
Configuration derived from the HA authority takes precedence over all other forms of configuration,
e.g. site properties or environment variables.

- When using embedded journal, use `alluxio://master_hostname_1:19998,
master_hostname_2:19998,master_hostname_3:19998/path`

For many applications (e.g., Hadoop, Hive and Flink), you can use a comma as the
delimiter for multiple addresses in the URI, like
`alluxio://master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998/path`.

For some other applications (e.g., Spark) where comma is not accepted inside a URL authority, you
need to use semicolons as the delimiter for multiple addresses,
like `alluxio://master_hostname_1:19998;master_hostname_2:19998;master_hostname_3:19998`.

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

```properties
alluxio.master.nameservices.my-alluxio-cluster=master1,master2,master3
```

* alluxio.master.rpc.address.[logical name]. [master node ID]  RPC Address for each alluxio master node

For each alluxio master node previously configured, set the full address of each alluxio master node, for example:

```properties
alluxio.master.rpc.address.my-alluxio-cluster.master1=master1:19998
alluxio.master.rpc.address.my-alluxio-cluster.master2=master2:19998
alluxio.master.rpc.address.my-alluxio-cluster.master3=master3:19998
```

## Common Operations

Below are common operations to perform on an Alluxio cluster.

### Stop Alluxio

To stop an Alluxio service, run:

```shell
$ ./bin/alluxio process stop all
```

This will stop all the processes on all nodes listed in `conf/workers` and `conf/masters`.

You can stop just the masters and just the workers with the following commands:

```shell
$ ./bin/alluxio process stop masters # stops all masters in conf/masters
$ ./bin/alluxio process stop workers # stops all workers in conf/workers
```

If you do not want to use `ssh` to login to all the nodes and stop all the processes, you can run
commands on each node individually to stop each component.
For any node, you can stop a master or worker with:

```shell
$ ./bin/alluxio process stop master # stops the local master
$ ./bin/alluxio process stop worker # stops the local worker
```

### Restart Alluxio

Starting Alluxio is similar. If `conf/workers` and `conf/masters` are both populated, you can start
the cluster with:

```shell
$ ./bin/alluxio process start all
```

You can start just the masters and just the workers with the following commands:

```shell
$ ./bin/alluxio process start masters # starts all masters in conf/masters
$ ./bin/alluxio process start workers # starts all workers in conf/workers
```

If you do not want to use `ssh` to login to all the nodes and start all the processes, you can run
commands on each node individually to start each component. For any node, you can start a master or
worker with:

```shell
$ ./bin/alluxio process start master # starts the local master
$ ./bin/alluxio process start worker # starts the local worker
```

### Add/Remove Workers Dynamically

Adding a worker to an Alluxio cluster dynamically is as simple as starting a new Alluxio worker
process, with the appropriate configuration.
In most cases, the new worker's configuration should be the same as all the other workers' configuration.
Run the following command on the new worker to add

```shell
$ ./bin/alluxio process start worker # starts the local worker
```

Once the worker is started, it will register itself with the Alluxio leading master and become part of the Alluxio cluster.

Removing a worker is as simple as stopping the worker process.

```shell
$ ./bin/alluxio process stop worker # stops the local worker
```

Once the worker is stopped, and after
a timeout on the master (configured by master parameter `alluxio.master.worker.timeout`), the master
will consider the worker as "lost", and no longer consider it as part of the cluster.

### Add/Remove Masters

In order to add a master, the Alluxio cluster must operate in HA mode.
If you are running the cluster as a single master cluster, you must configure it to be an HA cluster
before having more than one master.

See the [journal management documentation]({{ '/en/operation/Journal.html#adding-a-new-master' | relativize_url }}) for
more information about adding and removing masters.

### Update Master-side Configuration

In order to update the master-side configuration, you can first [stop the service](#stop-alluxio),
update the `conf/alluxio-site.properties` file on master node,
and then [restart the service](#restart-alluxio).
Note that, this approach introduces downtime of the Alluxio service.

Alternatively, one benefit of running Alluxio in HA mode is to use rolling restarts
to minimize downtime when updating configurations:

1. Update the master configuration on all the master nodes without restarting any master.
2. Restart standby masters one by one (the cluster [cannot survive more than `floor(n/2)` simultaneous restarts]( {{ '/en/operation/Journal.html#embedded-journal-vs-ufs-journal' | relativize_url }})).
3. Elect a standby master as the leading master (tutorial [here]({{ '/en/operation/Journal.html#electing-a-specific-master-as-leader' | relativize_url }})).
4. Restart the old leading master that is now a standby master.
5. Verify the configuration update.

### Update Worker-side Configuration

If you only need to update some local configuration for a worker (e.g., change the mount
of storage capacity allocated to this worker or update the storage directory), the master node does
not need to be stopped and restarted.
Simply stop the desired worker, update the configuration
(e.g., `conf/alluxio-site.properties`) file on that node, and then restart the process.
