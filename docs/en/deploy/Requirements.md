---
layout: global
title: Alluxio Software Requirements
nickname: Software Requirements
group: Deploying Alluxio
priority: 9
---

{:toc}

## General Requirements

There are base requirements for cluster nodes:

* Cluster nodes should be running one of the following supported operating systems:
  * MacOS 10.10 or later
  * CentOS - 6.8 or 7
  * RHEL - 7.x
  * Ubuntu - 16.04
* Alluxio is a Java application which requires version 8 of the JRE. Higher versions of the Java are not supported:
  * Java JRE 8 (Oracle or OpenJDK distributions supported)
* Alluxio works on IPv4 networks only.
* Allow the following ports and protocols:
  * Inbound TCP 22 - ssh as a user to install Alluxio components across specified nodes.

## Master Requirements

There are Alluxio-specific requirements for cluster nodes running the master process.

Note that these are bare minimums to run the software. Running Alluxio at scale and under high load
will increase these requirements.

* Minimum 4 GB disk space
* Minimum 4 GB memory
* Minimum 4 CPU cores
* Allow the following ports and protocols:
  * Inbound TCP 19200 - The Alluxio master's default port for internal leader election
  * Inbound TCP 19998 - The Alluxio master's default RPC port
  * Inbound TCP 19999 - The Alluxio master's default web UI port. Accessible at `http://<master-hostname>:19999`
  * Inbound TCP 20001 - The Alluxio job master's default RPC port
  * Inbound TCP 20002 - The Alluxio job master's default web UI port
  * Inbound TCP 20003 - The Alluxio job master's default port for internal leader election

## Worker Requirements

There are Alluxio-specific requirements for cluster nodes running the worker process:

* minimum 1 GB disk space
* minimum 1 GB memory
* minimum 2 CPU cores
* Allow the following ports and protocols:
  * Inbound TCP 29999 - The Alluxio worker's default RPC and data transfer port
  * Inbound TCP 30000 - The Alluxio worker's default web UI port. Accessible at `http://<worker-hostname>:30000` in your browser
  * Inbound TCP 30001 - The Alluxio job worker's default RPC port
  * Inbound TCP 30003 - The Alluxio job worker's default web UI port.  Accessible at `http://<worker-hostname>:30003` in your browser

### RAMFS

When Alluxio workers store blocks in memory, they use a [RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt).
If not pre-mounted, sudo privileges are required for mounting a RAMFS on Linux when using the Alluxio startup scripts.
Alternatively, if sudo provilegs are restricted, pre-mount a RAMFS location on Alluxio worker nodes.

```console
$ mkdir -p ${TIER_PATH}
$ mount -t ramfs -o size=${MEM_SIZE} ramfs ${TIER_PATH}
```

## Proxy Requirements

There are Alluxio-specific requirements for cluster nodes running the proxy process:

* minimum 1 GB memory
* Allow the following ports and protocols:
  * Inbound TCP 39999 - Used by clients to access the proxy.

## Remote Logging Server Requirements

There are Alluxio-specific requirements for running the remote logging server:

* minimum 1 GB disk space
* minimum 1 GB memory
* minimum 2 CPU cores
* Allow the following ports and protocols:
  * Inbound TCP 45600 - Used by loggers to write logs to the server.
