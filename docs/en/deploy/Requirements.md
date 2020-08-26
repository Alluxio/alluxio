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

Listed below are the generic requirements to run Alluxio in local or cluster mode

For large scale deployments and tuning suggestions, see
[Scalability Tuning]({{ '/en/operation/Scalability-Tuning.html' | relativize_url }})
and [Performance Tuning]({{ '/en/operation/Performance-Tuning.html' | relativize_url }}).

* Cluster nodes should be running one of the following supported operating systems:
  * MacOS 10.10 or later
  * CentOS - 6.8 or 7
  * RHEL - 7.x
  * Ubuntu - 16.04
* Alluxio requires version 8 of the JDK. Higher versions are not supported:
  * Java JDK 8 (Oracle or OpenJDK distributions supported)
* Alluxio works on IPv4 networks only.
* Allow the following ports and protocols:
  * Inbound TCP 22 - ssh as a user to install Alluxio components across specified nodes.

### Master Requirements

There are Alluxio-specific requirements for cluster nodes running the master process.

Note that these are bare minimum requirements to run the software.
Running Alluxio at scale and under high load will increase these requirements.

* Minimum 4 GB disk space
* Minimum 4 GB memory
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

* minimum 1 GB disk space
* minimum 1 GB memory
* minimum 2 CPU cores
* Allow the following ports and protocols:
  * Inbound TCP 29999 - The Alluxio worker's default RPC port
  * Inbound TCP 30000 - The Alluxio worker's default web UI port: `http://<worker-hostname>:30000`
  * Inbound TCP 30001 - The Alluxio job worker's default RPC port
  * Inbound TCP 30002 - The Alluxio job worker's default data port
  * Inbound TCP 30003 - The Alluxio job worker's default web UI
    port: `http://<worker-hostname>:30003`

#### Worker Cache

Alluxio Workers need to be configured with storage to use as the caching layer.
By default, they set up a
[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt) but this
can be modified to use a different storage volume.
By providing a different directory in `alluxio.worker.tieredstore.level%d.dirs.path`, users can
setup Alluxio to use a different directory backed by a different storage medium.
For users looking to get started with the defaults, run the command `./bin/alluxio-mount.sh
SudoMount workers` with any sudo privileged account.
This should be run after setting `alluxio.worker.ramdisk.size` in the `alluxio-site.properties`
file and adding all workers to the `conf/workers` file.

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

Alluxio can also aggregate logs into a remote server to view in a unified place.
Below are the port and resource requirements for the Logging Server.

### Remote Logging Server Requirements

There are Alluxio-specific requirements for running the remote logging server:

* Minimum 1 GB disk space
* Minimum 1 GB memory
* Minimum 2 CPU cores
* Allow the following ports and protocols:
  * Inbound TCP 45600 - Used by loggers to write logs to the server.
