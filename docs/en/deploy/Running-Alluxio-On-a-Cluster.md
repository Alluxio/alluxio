---
layout: global
title: Deploy an Alluxio Cluster with a Single Master
nickname: Cluster
group: Install Alluxio
priority: 2
---

* Table of Contents
{:toc}

## Overview

This section describes the basic setup to run Alluxio with a single master in a cluster.
This is the simplest way to deploy Alluxio on a cluster.
Deploying with only a single master also allows it to become the single point of failure (SPOF) in
an Alluxio cluster.
If the master machine or process becomes unavailable, the entire cluster would become unavailable.
To deploy Alluxio in production, we highly recommend running Alluxio masters in
[High Availability]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url }}) mode.

## Prerequisites

* To deploy an Alluxio cluster, first [download](https://alluxio.io/download) the pre-compiled
  Alluxio binary file, extract the tarball with the below command, and copy the extracted 
  directory to all nodes (including nodes running masters and workers).
  
  ```console
  $ tar -xvzpf alluxio-{{site.ALLUXIO_VERSION_STRING}}-bin.tar.gz
  ```
  
* Enable SSH login without password from the master node to worker nodes and from the master node to itself.
  You can add a public SSH key for the host into `~/.ssh/authorized_keys`.
  See [this tutorial](http://www.linuxproblem.org/art_9.html) for more details.
* TCP traffic across all nodes is allowed.
  For basic functionality, make sure RPC port (default:19998) is open on all nodes.
* Allow `sudo` privilege for the OS user that Alluxio will be running as.
  This is only needed if you expect Alluxio to mount a RAMFS on the workers automatically.

## Basic Setup

On the master node, create the `conf/alluxio-site.properties` configuration file from the template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Set the following properties in this configuration file (`conf/alluxio-site.properties`):

```
alluxio.master.hostname=<MASTER_HOSTNAME>
alluxio.master.mount.table.root.ufs=<STORAGE_URI>
```

- The first property `alluxio.master.hostname` sets the hostname of the single master node.
  Please ensure this address is reachable by your worker nodes.
  Examples include
  `alluxio.master.hostname=1.2.3.4` or `alluxio.master.hostname=node1.a.com`.
- The second property `alluxio.master.mount.table.root.ufs` sets to the URI of the under store to
  mount to the Alluxio root.
  This shared storage system must be accessible by the master node and all worker nodes.
  
  For example, when [HDFS]({{ '/en/ufs/HDFS.html#basic-setup' | relativize_url }})
  is used as the under storage system, the value of this property can be set to
  `alluxio.master.mount.table.root.ufs=hdfs://1.2.3.4:9000/alluxio/root/`
  
  When [Amazon S3]({{ '/en/ufs/S3.html#basic-setup' | relativize_url }})
  is used as the under storage system, the value can be set to
  `alluxio.master.mount.table.root.ufs=s3://bucket/dir/`

Append the hostname of each node into `conf/masters` and `conf/workers` accordingly.
Append the hostname of each Alluxio master node to a new line into `conf/masters`,
and the hostname of each worker node to a new line into `conf/worers`.
Comment out `localhost` if necessary.
For example, in `conf/masters`, we can add the hostnames of two master nodes in the following format:
```
# The multi-master Zookeeper HA mode requires that all the masters can access
# the same journal through a shared medium (e.g. HDFS or NFS).
# localhost
ec2-1-111-11-111.compute-1.amazonaws.com
ec2-2-222-22-222.compute-2.amazonaws.com
```

Next, copy the configuration file to all the Alluxio worker nodes.
The following built-in utility will copy the configuration files to all master and worker
nodes specified in the `conf/masters` and `conf/workers` files respectively.

```console
$ ./bin/alluxio copyDir conf/
```

Once this command succeeds, all the Alluxio nodes will be correctly configured.

This is the minimal configuration to start Alluxio. Additional configuration properties
may be set as needed. See the [configuration properties reference](https://docs.alluxio.io/os/user/stable/en/reference/Properties-List.html)
for more details.

- You may need to set additional properties to enable Alluxio to access
  the configured under storage (eg., [AWS S3 configuration](https://docs.alluxio.io/os/user/stable/en/overview/Getting-Started.html#bonus-configuration-for-aws))

## Start an Alluxio Cluster

### Format Alluxio

Before Alluxio can be started for the first time, the journal must be formatted.

> Formatting the journal will delete all metadata from Alluxio.
  However, the data in under storage will be untouched.

Format the journal for the Alluxio master node with the following command:

```console
$ ./bin/alluxio formatMasters
```

### Launch Alluxio

To start the Alluxio cluster, on the master node make sure the `conf/masters` and
`conf/workers` files have the correct hostnames set.

On the master node, start the Alluxio cluster with the following command:

```console
$ ./bin/alluxio-start.sh all SudoMount
```

This will start the master on the master node, and start all the workers on all the
worker nodes specified in the `conf/workers` file.
The `SudoMount` argument enables the workers to attempt to mount the RamFS using `sudo` 
privilege, if not already mounted.

### Verify Alluxio Cluster

To verify that Alluxio is running, visit `http://<alluxio_master_hostname>:19999` to see the status
page of the Alluxio master.

Alluxio comes with a simple program that writes and reads sample files in Alluxio.
Run the sample program with:

```console
$ ./bin/alluxio runTests
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

Starting Alluxio is similar.
If `conf/workers` and `conf/masters` are both populated, you can start the cluster with:

```console
$ ./bin/alluxio-start.sh all
```

You can start just the masters and just the workers with the following commands:

```console
$ ./bin/alluxio-start.sh masters # starts all masters in conf/masters
$ ./bin/alluxio-start.sh workers # starts all workers in conf/workers
```

If you do not want to use `ssh` to login to all the nodes and start all the processes, you can run
commands on each node individually to start each component.
For any node, you can start a master or worker with:

```console
$ ./bin/alluxio-start.sh master # starts the local master
$ ./bin/alluxio-start.sh worker # starts the local worker
```

### Format the Journal

On any master node, format the Alluxio journal with the following command:

```console
$ ./bin/alluxio formatMaster
```

Formatting the journal will delete all metadata from Alluxio.
However, the data in under storage will be untouched.

### Add/Remove Workers Dynamically

Adding a worker to an Alluxio cluster dynamically is as simple as starting a new Alluxio worker
process with the appropriate configuration.
In most cases, the new worker's configuration should be the same as all the other workers'
configuration.
Run the following command on the new worker to add it to the cluster.

```console
$ ./bin/alluxio-start.sh worker SudoMount # starts the local worker
```

Once the worker is started, it will register itself with the Alluxio master, and become part of the
Alluxio cluster.

Removing a worker is as simple as stopping the worker process.

```console
$ ./bin/alluxio-stop.sh worker # stops the local worker
```

Once the worker is stopped, the master will flag the worker as lost after a predetermined timeout 
value (configured by master parameter `alluxio.master.worker.timeout`).
The master will consider the worker as "lost", and no longer include it as part of the cluster.

### Update Master-side Configuration

In order to update the master-side configuration, you must first [stop the service](#stop-alluxio),
update the `conf/alluxio-site.properties` file on master node,
copy the file to all nodes (e.g., using `bin/alluxio copyDir conf/`),
and then [restart the service](#restart-alluxio).

### Update Worker-side Configuration

If you only need to update some local configuration for a worker (e.g., change the mount
of storage capacity allocated to this worker or update the storage directory), the master node does
not need to be stopped and restarted.
Simply stop the desired worker, update the configuration
(e.g., `conf/alluxio-site.properties`) file on that node, and then restart the process.

