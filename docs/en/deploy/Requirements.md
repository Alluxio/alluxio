---
layout: global
title: Alluxio Software Requirements
nickname: Software Requirements
group: Install Alluxio
priority: 9
---

* Table of Contents
{:toc}

## General Requirements

Listed below are the generic requirements to run Alluxio locally or as a cluster.

For large scale deployments and tuning suggestions, see
[Scalability Tuning]({{ '/en/operation/Scalability-Tuning.html' | relativize_url }})
and [Performance Tuning]({{ '/en/operation/Performance-Tuning.html' | relativize_url }}).

* Cluster nodes should be running one of the following supported operating systems:
  * MacOS 10.10 or later
  * CentOS - 6.8 or 7
  * RHEL - 7.x
  * Ubuntu - 16.04
* Alluxio requires version 8 or 11 of the JDK. Other versions are not supported:
  * Java JDK 8 or 11 (Oracle or OpenJDK distributions supported)
* Alluxio works on IPv4 networks only.
* Allow the following ports and protocols:
  * Inbound TCP 22 - ssh as a user to install Alluxio components across specified nodes.

### Master Requirements

There are Alluxio-specific requirements for cluster nodes running the master process.

Note that these are bare minimum requirements to run the software.
Running Alluxio at scale and under high load will increase these requirements.

* Minimum 4 GB disk space
* Minimum 4 GB memory (6 GB if using embedded journal)
* Minimum 4 CPU cores
* Allow the following ports and protocols:
  * Inbound TCP 19998 - The Alluxio master's default RPC port
  * Inbound TCP 19999 - The Alluxio master's default web UI port: `http://<master-hostname>:19999`
  * Inbound TCP 20001 - The Alluxio job master's default RPC port
  * Inbound TCP 20002 - The Alluxio job master's default web UI port
  * Embedded Journal Requirements Only
    * Inbound TCP 19200 - The Alluxio master's default port for internal leader election
    * Inbound TCP 20003 - The Alluxio job master's default port for internal leader election

### Worker Requirements

There are Alluxio-specific requirements for cluster nodes running the worker process:

* Minimum 1 GB disk space
* Minimum 1 GB memory
* Minimum 2 CPU cores
* Allow the following ports and protocols:
  * Inbound TCP 29999 - The Alluxio worker's default RPC port
  * Inbound TCP 30000 - The Alluxio worker's default web UI port: `http://<worker-hostname>:30000`
  * Inbound TCP 30001 - The Alluxio job worker's default RPC port
  * Inbound TCP 30002 - The Alluxio job worker's default data port
  * Inbound TCP 30003 - The Alluxio job worker's default web UI
    port: `http://<worker-hostname>:30003`

#### Worker Cache

Alluxio workers need configure a storage volume to use as the caching layer.
By default, workers will set up a
[`ramfs`](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt) but this
can be modified to use a different storage volume by setting a different directory for the
`alluxio.worker.tieredstore.level%d.dirs.path` property in `alluxio-site.properties`.

Note from the `ramfs` documentation:

> "One downside of ramfs is you can keep writing data into it until you fill
> up all memory ... Because of this, only root (or a trusted user) should
> be allowed write access to a ramfs mount."

To get started with the default configuration, set `alluxio.worker.ramdisk.size` in
`alluxio-site.properties`, add worker hostnames in `conf/workers`, then run the following command:

```console
$ ./bin/alluxio-mount.sh SudoMount workers
```

### Proxy Requirements

The proxy process provides a REST based client:

* Minimum 1 GB memory
* Allow the following ports and protocols:
  * Inbound TCP 39999 - Used by clients to access the proxy.

### Fuse Requirements

There are Alluxio-specific requirements for nodes running the fuse process.

Note that these are bare minimum requirements to run the software.
Running Alluxio Fuse under high load will increase these requirements.

* Minimum 1 CPU core
* Minimum 1 GB memory
* FUSE installed
  * libfuse 2.9.3 or newer for Linux
  * osxfuse 3.7.1 or newer for MacOS

## Additional Requirements

Alluxio can aggregate logs and send them to a remote server.
This is useful to be able to view logs for the entire cluster from a single location.
Below are the port and resource requirements for the remote logging server.

### Remote Logging Server Requirements

There are Alluxio-specific requirements for running the remote logging server:

* Minimum 1 GB disk space
* Minimum 1 GB memory
* Minimum 2 CPU cores
* Allow the following ports and protocols:
  * Inbound TCP 45600 - Used by loggers to write logs to the server.
