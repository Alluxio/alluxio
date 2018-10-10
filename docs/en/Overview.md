---
layout: global
title: Overview
group: Home
---

* Table of Contents
{:toc}


## What is Alluxio

Alluxio is the world’s first memory-speed virtual distributed storage system. It unifies data access
and bridges computation frameworks and underlying storage systems.  Applications only need to
connect with Alluxio to access data stored in any underlying storage systems. Additionally,
Alluxio’s memory-centric architecture enables data access at speeds that is orders of magnitude
faster than existing solutions.  It runs on commodity hardware, creating a shared data layer
abstracting the files or objects in underlying persistent storage systems. Applications connect to
Alluxio via a standard interface, accessing data from a single unified source.

The Alluxio project is open source under [Apache License
2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE) and is deployed at many companies. It
is one of the fastest growing open source projects. With three years of open source history, Alluxio
has attracted more than [800 contributors](https://github.com/alluxio/alluxio/graphs/contributors)
from over 150 institutions, including [Alibaba](http://www.alibaba.com),
[Alluxio](http://www.alluxio.com/), [Baidu](https://www.baidu.com), [CMU](https://www.cmu.edu/),
[Google](https://www.google.com), [IBM](https://www.ibm.com), [Intel](http://www.intel.com/),
[NJU](http://www.nju.edu.cn/english/), [Red Hat](https://www.redhat.com/), [UC
Berkeley](https://amplab.cs.berkeley.edu/), and [Yahoo](https://www.yahoo.com/).  The project is the
storage layer of the Berkeley Data Analytics Stack ([BDAS](https://amplab.cs.berkeley.edu/bdas/))
and also part of the [Fedora distribution](https://fedoraproject.org/wiki/SIGs/bigdata/packaging).
Today, Alluxio is deployed in production by 100s organizations, and runs on clusters that exceed
1,000 nodes.

<div style="text-align:center">
<span>
<img src="{{site.baseurl}}{% link img/stack.png %}" width="800"/>
</span>
</div>

## Benefits

Alluxio helps overcome the obstacles to extracting value from data by making it simple to give
applications access to whatever data is needed, regardless of format or location. The benefits of
Alluxio Include:

* **Memory-Speed I/O**: Alluxio can be used as a distributed shared caching service so that compute
applications talking to Alluxio can transparently cache frequently accessed data, especially data
from remote locations, to provide in-memory I/O throughput.

* **Simplified Cloud and Object Storage Adoption**: Cloud and object storage systems use different
semantics that have performance implications compared to traditional file systems. For example, when
accessing data in cloud storage there is no node-level locality or cross-application caching. There
are also different performance characteristics in common file system operations like directory
listing (‘ls’) and ‘rename’, which often add significant overhead to analytics. Deploying Alluixo
with cloud or object storage can close the semantics gap and achieve significant performance gains.

* **Simplified Data Management**: Alluxio provides a single point of access to multiple data
sources. For example, If you need to access data stored in multiple versions of HDFS or multiple
cloud storage vendors Alluxio also gives applications the ability to talk to different versions of
the same storage, without complex system configuration and management.

* **Easy Application Deployment**: Alluxio manages communication between applications and file or
object storage, translating data access requests from applications to any persistent underlying
storage interface. Alluxio is Hadoop compatible.  Existing data analytics applications, such as
Spark and MapReduce programs, can run on top of Alluxio without any code change.

## How Alluxio Works

Alluxio is far more than simply a caching solution. A rich set of intelligent data management
capabilities ensure efficient use of memory resources, high performance, and data continuity. Data
is unified with a single point of access, and a standard interface makes data access transparent to
applications without any changes. The solution is based on three key areas of innovation working
together to provide a unique set of capabilities.

1. **Intelligent Cache**: Alluxio clusters act as a read/write cache for data in connected storage
systems. Configurable policies automatically optimize data placement for performance and reliability
across both memory and disk (SSD/HDD). Caching is transparent to the user, and uses read/write
buffering to maintain consistency with persistent storage.  See [Alluxio Storage Management]({{
site.baseurl }}{% link en/advanced/Alluxio-Storage-Management.md %}) for more details.

1. **Global Namespace**: A single point of access to multiple independent storage systems regardless
of physical location. Alluxio provides a unified view of all data sources and a standard interface
for applications.  See [Namespace Management]({{ site.baseurl }}{% link
en/advanced/Namespace-Management.md %}) for more details.

1. **Server-Side API Translation**: Alluxio transparently converts from a standard client-side
interface to any storage interface. Alluxio manages communication between applications and file or
object storage, eliminating the need for complex system configuration and management. File data can
look like object data and vice versa.

To understand more details on Alluxio internals, please read [Alluxio architecture and data flow]({{
site.baseurl }}{% link en/Architecture-DataFlow.md %}).

## Getting Started

To quickly get Alluxio up and running, take a look at our [Getting Started]({{ site.baseurl }}{%
link en/Getting-Started.md %}) page which will go through how to deploy Alluxio and run some basic
examples in a local environment.  See a list of
[companies](https://alluxio.org/community/powered-by-alluxio) that are using Alluxio.

## Downloads and More

You can get the released versions of Alluxio from the [Project Downloads
Page](http://alluxio.org/download). Each release comes with prebuilt binaries compatibile with
various Hadoop versions. If you would like to build the project from the source code, check out the
[Building From Master Branch Documentation]({{ site.baseurl }}{% link
en/contributor/Building-Alluxio-From-Source.md %}). If you have any questions, please feel free to
ask at our [User Mailing
List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users). For users can not access
Google Group, please use its [mirror](http://alluxio-users.85194.x6.nabble.com/) (notes: the mirror
does not have information before May 2016).
