---
layout: global
title: Deploy Alluxio on a Cluster
nickname: Cluster
group: Deploying Alluxio
priority: 2
---

* Table of Contents
{:toc}

## Prerequisites

* To deploy a Alluxio cluster, first [download](https://alluxio.io/download) the pre-compiled Alluxio
  binary file, extract the tarball and copy the extracted directory to all nodes (including nodes
  running masters and workers).
* Enable SSH login without password from master node to worker nodes. You can add a public SSH key for 
  the host into `~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html) for more details.
* TCP traffic across all nodes is allowed. For basic functionality make sure RPC port (default :19998) is open
  on all nodes.

## Basic Setup

The simplest way to deploy Alluxio on a cluster is to use one master. However, this single master becomes
the single point of failure (SPOF) in an Alluxio cluster: if that machine or process became unavailable,
the cluster as a whole would be unavailable. We highly recommend running Alluxio masters in
[High Availability]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url }}) mode in production.
This section describes the basic setup to run Alluxio with a single master in a cluster.
On the master node of the installation, create the `conf/alluxio-site.properties` configuration file from the template.

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

This configuration file (`conf/alluxio-site.properties`) is read by all Alluxio masters and Alluxio
workers. The configuration parameters which must be set are:

```
alluxio.master.hostname=<MASTER_HOSTNAME>
alluxio.master.mount.table.root.ufs=<STORAGE_URI>
```

The first property `alluxio.master.hostname` sets the hostname of the single master node. Examples include
`alluxio.master.hostname=1.2.3.4` or `alluxio.master.hostname=node1.a.com`. The second property
`alluxio.master.mount.table.root.ufs` sets to the URI of the shared storage system to mount to
the Alluxio root. This shared shared storage system must be accessible by the master node and all
worker nodes. Examples include `alluxio.master.mount.table.root.ufs=hdfs://1.2.3.4:9000/alluxio/root/`,
or `alluxio.master.mount.table.root.ufs=s3://bucket/dir/`.

This is the minimal configuration to start Alluxio, but additional configuration may be added.

Since this same configuration file will be the same for the master and all the workers, this configuration
file must be copied to all the other Alluxio nodes. The simplest way to do this is
to use the `copyDir` shell command on the master node. In order to do so, add the IP addresses or
hostnames of all the worker nodes to the `conf/workers` file. Then run:

```bash
./bin/alluxio copyDir conf/
```

This will copy the `conf/` directory to all the workers specified in the `conf/workers` file. Once
this command succeeds, all the Alluxio nodes will be correctly configured.

## Run Alluxio Cluster

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
nodes specified in the `conf/workers` file.

### Verify Alluxio Cluster

To verify that Alluxio is running, visit `http://<alluxio_master_hostname>:19999` to see the status
page of the Alluxio master.

Alluxio comes with a simple program that writes and reads sample files in Alluxio. Run the sample program with:

```bash
./bin/alluxio runTests
```

## Cluster Operations

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
