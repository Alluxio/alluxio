---
layout: global
title: Architecture Overview
nickname: Architecture Overview
group: User Guide
priority: 1
---

* Table of Contents
{:toc}

# Where Tachyon Fits

Because of Tachyon's memory centric design and being the central point of access, Tachyon holds a
unique place in the big data ecosystem, residing between traditional storage (e.g ., Amazon S3,
Apache HDFS and OpenStack Swift, etc.) and computation frameworks and applications (e.g., Spark,
Hadoop MapReduce).  For user applications and computation frameworks, Tachyon is the underlayer that
manages data access and fast storage, facilitating data sharing and locality between jobs,
regardless of whether they are running with the same computation engine. As a result, Tachyon can
bring an order of magnitude speed up for those big data applications while providing a  common
interface of data access. For under storage systems, Tachyon connects the gap between big data
applications and traditional storage systems, and redefines the set of workloads available to
utilize the data. Since Tachyon hides the integration of under storage systems to applications, any
under storage can back all the applications and frameworks running on top of Tachyon. Coupled with
the potential to mount multiple under storage systems, Tachyon can serve as a unifying layer for any
number of varied data sources.

![Stack]({{site.data.img.stack}})

# Tachyon's Components

Tachyon's design uses a single master and multiple workers. At a very high level, Tachyon can be
divided into three components, the [master](#master), [workers](#worker), and [clients](#client).
The master and workers together make up the Tachyon servers, which are the components a system admin
would maintain and manage. The clients are generally the applications, such as Spark or MapReduce
jobs, or Tachyon command-line users. Users of Tachyon will usually only need to interact with the
client portion of Tachyon.

### Master

Tachyon may be deployed in one of two master modes, [single master](Running-Tachyon-Locally.html) or
[multiple masters (one primary plus standbys)](Running-Tachyon-Fault-Tolerant-on-EC2.html). The
master  is primarily responsible for managing the global metadata of the system, for example, the
file system  tree. Clients may interact with the master to read or modify this metadata. In
addition, all workers periodically heartbeat to the master to maintain their participation in the
cluster. The master does not initiate communication with other components; it only interacts with
other components by responding to requests.

### Worker

Tachyon workers are responsible for [managing local resources](Tiered-Storage-on-Tachyon.html)
allocated to Tachyon. These resources could be local memory, SSD, or hard disk and are user
configurable. Tachyon workers store data as blocks and serve requests from clients to read or write
data by reading or creating new blocks. However, the worker is only responsible for the data in
these blocks; the actual mapping from file to blocks is only stored in the master.

### Client

The Tachyon client provides users a gateway to interact with the Tachyon servers. It exposes a
[file system API](File-System-API.html). It initiates communication with master to carry out
metadata operations and with workers to read and write data that exist in Tachyon. Data that exists
in the  under storage but is not available in Tachyon is accessed directly through an under storage
client.
