---
layout: global
title: Deploy an Alluxio Cluster with a Single Master
nickname: Cluster
group: Deploying Alluxio
priority: 2
---

* Table of Contents
{:toc}

## Overview

This section describes the basic setup to run Alluxio with a single master in a cluster.
This is the simplest way to deploy Alluxio on a cluster, meanwhile this single
master may also become the single point of failure (SPOF) in an Alluxio cluster. If that machine or
process became unavailable, the cluster as a whole would be unavailable.
To deploy Alluxio in production, we highly recommend running Alluxio masters in
[High Availability]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url }}) mode.

## Prerequisites

* To deploy a Alluxio cluster, first [download](https://alluxio.io/download) the pre-compiled Alluxio
  binary file, extract the tarball and copy the extracted directory to all nodes (including nodes
  running masters and workers).
* Enable SSH login without password from master node to worker nodes. You can add a public SSH key for
  the host into `~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html) for more details.
* TCP traffic across all nodes is allowed. For basic functionality make sure RPC port (default :19998) is open
  on all nodes.

## Basic Setup

On the master node, create the `conf/alluxio-site.properties` configuration file from the template.

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Set the following properties in this configuration file (`conf/alluxio-site.properties`):

```
alluxio.master.hostname=<MASTER_HOSTNAME>
alluxio.master.mount.table.root.ufs=<STORAGE_URI>
```

- The first property `alluxio.master.hostname` sets the hostname of the single master node.
Examples include
`alluxio.master.hostname=1.2.3.4` or `alluxio.master.hostname=node1.a.com`.
- The second property
`alluxio.master.mount.table.root.ufs` sets to the URI of the under store to mount to
the Alluxio root. This shared shared storage system must be accessible by the master node and all
worker nodes. Examples include `alluxio.master.mount.table.root.ufs=hdfs://1.2.3.4:9000/alluxio/root/`,
or `alluxio.master.mount.table.root.ufs=s3://bucket/dir/`.

Next, copy configuration files to all the other Alluxio nodes. To use the provided scripts,
add the IP addresses or hostnames of all the worker nodes to the `conf/workers` file. Then run:

```bash
./bin/alluxio copyDir conf/
```

This command will copy the `conf/` directory to all the workers specified in the `conf/workers`
file. Once this command succeeds, all the Alluxio nodes will be correctly configured.

This is the minimal configuration to start Alluxio, but additional configuration may be added.

## Start an Alluxio Cluster

### Format Alluxio

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
nodes specified in the `conf/workers` file.  Argument `SudoMount` indicates to mount the RamFS on each worker
using `sudo` privilege, if it is not already mounted.

### Verify Alluxio Cluster

To verify that Alluxio is running, visit `http://<alluxio_master_hostname>:19999` to see the status
page of the Alluxio master.

Alluxio comes with a simple program that writes and reads sample files in Alluxio. Run the sample program with:

```bash
./bin/alluxio runTests
```

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
./bin/alluxio-start.sh all
```

You can start just the masters and just the workers with the following commands:

```bash
./bin/alluxio-start.sh masters # starts all masters in conf/masters
./bin/alluxio-start.sh workers # starts all workers in conf/workers
```

If you do not want to use `ssh` to login to all the nodes and start all the processes, you can run
commands on each node individually to start each component. For any node, you can start a master or worker with:

```bash
./bin/alluxio-start.sh master # starts the local master
./bin/alluxio-start.sh worker # starts the local worker
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

### Update Master-side Configuration

In order to update the master-side configuration, you must first [stop the service](#stop-alluxio),
update the `conf/alluxio-site.properties` file on master node,
copy the file to all nodes (e.g., using `bin/alluxio copyDir conf/`),
and then [restart the service](#restart-alluxio).

### Update Worker-side Configuration

If you only need to update some local configuration for a worker (e.g., change the mount
of storage capacity allocated to this worker or update the storage directory), the master node does
not need to be stopped and restarted. One can simply stop the local worker, update the configuration
(e.g., `conf/alluxio-site.properties`) file on this worker, and then restart the worker.
