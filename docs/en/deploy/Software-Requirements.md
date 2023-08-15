---
layout: global
title: Alluxio Software Requirements
---

## General Requirements

Listed below are the generic requirements to run Alluxio locally or as a cluster.

* Cluster nodes should be running one of the following supported operating systems:
  * MacOS 10.10 or later
  * CentOS - 6.8 or 7
  * RHEL - 7.x
  * Ubuntu - 16.04
* Java JDK 11 (Oracle or OpenJDK distributions supported)
* Alluxio works on IPv4 networks only.
* Allow the following ports and protocols:
  * Inbound TCP 22 - ssh as a user to install Alluxio components across specified nodes.
* It is recommended to use hardware based on the x86 architecture. It is verified that Alluxio can run on the ARM architecture, but it may be possible that certain features may not work.
   * Alluxio's overall performance is similar between the two architectures,
  based on benchmark results when running on [AWS Graviton processors](https://aws.amazon.com/ec2/graviton/) (ex. [r6g instances](https://aws.amazon.com/ec2/instance-types/r6g/))
  versus [AWS 3rd generation AMD EPYC processors](https://aws.amazon.com/ec2/amd/) (ex. [r6a instances](https://aws.amazon.com/ec2/instance-types/r6a/)).

### Master Requirements

There are Alluxio-specific requirements for cluster nodes running the master process.

Note that these are bare minimum requirements to run the software.
Running Alluxio at scale and under high load will increase these requirements.

* Minimum 1 GB disk space
* Minimum 1 GB memory (6 GB if using embedded journal)
* Minimum 2 CPU cores
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

### Fuse Requirements

There are Alluxio-specific requirements for nodes running the fuse process.

Note that these are bare minimum requirements to run the software.
Running Alluxio Fuse under high load will increase these requirements.

* Minimum 1 CPU core
* Minimum 1 GB memory
* FUSE installed
  * libfuse 2.9.3 or newer for Linux, recommend to use libfuse >= 3.0.0
  * osxfuse 3.7.1 or newer for MacOS

