---
layout: global
title: Overview
group: Home
priority: 0
---

* Table of Contents
{:toc}

## What is Alluxio

Alluxio is the world’s first memory-speed virtual distributed storage system.
It bridges the gap between computation frameworks and storage systems, enabling applications
to connect to numerous storage systems through a common interface.
Alluxio’s memory-centric architecture enables data access at speeds orders of magnitude faster than existing solutions.

The Alluxio project is open source under [Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE)
and is deployed at a wide range of companies.
It is one of the fastest growing open source projects.
In a span of three years, Alluxio has attracted more than [800 contributors](https://github.com/alluxio/alluxio/graphs/contributors)
from over 150 institutions including
[Alibaba](http://www.alibaba.com),
[Alluxio](http://www.alluxio.com/),
[Baidu](https://www.baidu.com),
[CMU](https://www.cmu.edu/),
[Google](https://www.google.com),
[IBM](https://www.ibm.com),
[Intel](http://www.intel.com/),
[NJU](http://www.nju.edu.cn/english/),
[Red Hat](https://www.redhat.com/),
[UC Berkeley](https://amplab.cs.berkeley.edu/),
and [Yahoo](https://www.yahoo.com/).
The project is the storage layer of the Berkeley Data Analytics Stack ([BDAS](https://amplab.cs.berkeley.edu/bdas/))
and is also part of the [Fedora distribution](https://fedoraproject.org/wiki/SIGs/bigdata/packaging).
Today, Alluxio is deployed in production by hundreds of organizations and runs on clusters exceeding 1,000 nodes.

<p align="center">
<img src="{{ "/img/stack.png" | relativize_url }}" width="800" alt="Ecosystem"/>
</p>

## Benefits

Alluxio helps overcome the obstacles in extracting insight from data by simplifying how applications
access their data, regardless of format or location. The benefits of Alluxio include:

* **Memory-Speed I/O**: Alluxio can be used as a distributed shared caching service so that compute
applications talking to Alluxio can transparently cache frequently accessed data, especially
from remote locations, to provide in-memory I/O throughput.

* **Simplified Cloud and Object Storage Adoption**: Cloud and object storage systems use different
semantics that have performance implications compared to traditional file systems. Common file
system operations such as directory listing and renaming often incur significant performance
overhead. When accessing data in cloud storage, applications have no node-level locality or
cross-application caching. Deploying Alluxio with cloud or object storage mitigates these problems
because data will be retrieved from Alluxio instead of the underlying cloud or object storage.

* **Simplified Data Management**: Alluxio provides a single point of access to multiple data
sources. In addition to connecting data sources of different types, Alluxio also enables users to
simultaneously connect to different versions of the same storage system, such as multiple versions
of HDFS, without complex system configuration and management.

* **Easy Application Deployment**: Alluxio manages communication between applications and file or
object storages, translating data access requests from applications into underlying
storage interfaces. Alluxio is Hadoop compatible. Existing data analytics applications, such as
Spark and MapReduce programs, can run on top of Alluxio without any code change.

## How Alluxio Works

Alluxio brings three key areas of innovation together to provide a unique set of capabilities.

1. **Intelligent Cache**: Alluxio clusters act as a read and write cache for data in connected storage
systems. Configurable policies automatically optimize data placement for performance and reliability
across both memory and disk (SSD/HDD). Caching is transparent to the user and uses
buffering to maintain consistency with persistent storage. See
[Alluxio Storage Management]({{ '/en/advanced/Alluxio-Storage-Management.html' | relativize_url }}) for more details.

1. **Global Namespace**: Alluxio serves as a single point of access to multiple independent storage
systems regardless of physical location. This provides a unified view of all data sources and a
standard interface for applications. See
[Namespace Management]({{ '/en/advanced/Namespace-Management.html' | relativize_url }}) for more details.

1. **Server-Side API Translation**: Alluxio transparently converts from a standard client-side
interface to any storage interface. Alluxio manages communication between applications and file or
object storage, eliminating the need for complex system configuration and management. File data can
look like object data and vice versa.

To understand more details on Alluxio internals, please read
[Alluxio architecture and data flow]({{ '/en/Architecture-DataFlow.html' | relativize_url }}).

## Getting Started

To quickly get Alluxio up and running, take a look at our
[Getting Started]({{ '/en/Getting-Started.html' | relativize_url }}) page,
which explains how to deploy Alluxio and run examples in a local environment.

## Downloads and More

Released versions of Alluxio are available from the [Project Downloads Page](http://alluxio.org/download).
Each release comes with prebuilt binaries compatible with various Hadoop versions.
[Building From Master Branch Documentation]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }})
explains how to build the project from source
code. Questions can be directed to our
[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users). Users who
can not access the Google Group may use its [mirror](http://alluxio-users.85194.x6.nabble.com/)
Note that the mirror does not have information before May 2016.
