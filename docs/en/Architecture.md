---
layout: global
title: Architecture Overview
nickname: Architecture Overview
group: User Guide
priority: 1
---

* Table of Contents
{:toc}

## Where Alluxio Fits

Because of Alluxio's memory centric design and being the central point of access, Alluxio holds 
a unique place in the big data ecosystem, residing between storage systems such as Amazon S3, 
Apache HDFS or OpenStack Swift and computation frameworks and applications such as Apache Spark 
or Hadoop MapReduce. For user applications and computation frameworks, Alluxio is the underlayer 
that manages data access and fast storage, facilitating data sharing and locality between jobs, 
regardless of whether they are running with the same computation engine. As a result, Alluxio 
can bring an order of magnitude speed up for those big data applications while providing a 
common interface of data access. For under storage systems, Alluxio connects the gap between 
big data applications and traditional storage systems, and redefines the set of workloads 
available to utilize the data. Since Alluxio hides the integration of under storage systems to 
applications, any under storage can back all the applications and frameworks running on top of 
Alluxio. Coupled with the potential to mount multiple under storage systems, Alluxio can serve 
as a unifying layer for any number of varied data sources.

![Stack]({{site.data.img.stack}})

## Alluxio Components

Alluxio's design uses a single master and multiple workers. At a very high level, Alluxio can 
be divided into three components, the [master](#master), [workers](#worker), and 
[clients](#client). The master and workers together make up the Alluxio servers, which are the 
components a system admin would maintain and manage. The clients are generally the 
applications, such as Spark or MapReduce jobs, or Alluxio command-line users. Users of Alluxio 
will usually only need to interact with the client portion of Alluxio.

### Master

Alluxio master can be deployed as primary master or secondary master. 
#### Primary Master
The primary master is primarily responsible for managing the global metadata of the system, 
for example, the file system tree. Clients may interact with the primary master to read or 
modify this metadata. In addition, all workers periodically heartbeat to the primary master to 
maintain their participation in the cluster. The primary master does not initiate communication 
with other components; it only interacts with other components by responding to requests. There 
is one and only one primary master in an Alluxio cluster.

#### Secondary Master
The secondary master replays journals written by the primary master and periodically writes 
checkpoints. It does not process any requests from any Alluxo components.

#### Master Deployment
Alluxio may be deployed in one of two master modes, [simple mode](Running-Alluxio-Locally.html) or 
[high availablity mode](Running-Alluxio-Fault-Tolerant.html). There must be one and only one 
primary master in both of the simple mode and the high availability mode. The simple mode can at 
most have one secondary master which cannot be promoted to the primary master. The high 
availability mode can have zero or more secondary masters, one of which will be promoted to the 
primary master if the primary master fails.

### Worker

Alluxio workers are responsible for [managing local resources](Tiered-Storage-on-Alluxio.html) 
allocated to Alluxio. These resources could be local memory, SSD, or hard disk and are user 
configurable. Alluxio workers store data as blocks and serve requests from clients to read 
or write data by reading or creating new blocks. However, the worker is only responsible for the 
data in these blocks; the actual mapping from file to blocks is only stored in the master.

### Client

The Alluxio client provides users a gateway to interact with the Alluxio servers. It exposes 
a [file system API](File-System-API.html). It initiates communication with master to carry out 
metadata operations and with workers to read and write data that exist in Alluxio. Data that 
exists in the under storage but is not available in Alluxio is accessed directly through an 
under storage client.
