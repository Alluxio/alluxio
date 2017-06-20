---
layout: global
title: Overview
group: Home
---

* Table of Contents
{:toc}

Alluxio, formerly known as Tachyon, is the world’s first memory speed virtual distributed storage
system. It unifies data access and bridges computation frameworks and underlying storage systems.
Applications only need to connect with Alluxio to access data stored in any underlying storage
systems. Additionally, Alluxio’s memory-centric architecture enables data access orders of magnitude
faster than existing solutions.

In the big data ecosystem, Alluxio lies between computation frameworks or jobs, such as Apache
Spark, Apache MapReduce, Apache HBase, Apache Hive, or Apache Flink, and various kinds of storage
systems, such as Amazon S3, Google Cloud Storage, OpenStack Swift, GlusterFS, HDFS, MaprFS, Ceph,
NFS, and Alibaba OSS. Alluxio brings significant performance improvement to the ecosystem; for
example, [Baidu](https://www.baidu.com)
uses Alluxio to improve speedup the throughput of their data analytics pipeline
[30 times](http://www.alluxio.com/assets/uploads/2016/02/Baidu-Case-Study.pdf). Barclays makes the
impossible possible with Alluxio to accelerate jobs from
[hours to seconds](https://dzone.com/articles/Accelerate-In-Memory-Processing-with-Spark-from-Hours-to-Seconds-With-Tachyon).
Qunar performs
[real-time data analytics](http://www.alluxio.com/2016/07/qunar-performs-real-time-data-analytics-up-to-300x-faster-with-alluxio/)
on top of Alluxio. Beyond performance, Alluxio bridges new workloads with data stored in traditional
storage systems. Users can run Alluxio using its
[standalone cluster mode](Running-Alluxio-on-a-Cluster.html), for example on
[Amazon EC2](Running-Alluxio-on-EC2.html),
[Google Compute Engine](Running-Alluxio-on-GCE.html), or launch Alluxio with
[Apache Mesos](Running-Alluxio-on-Mesos.html) or
[Apache Yarn](Running-Alluxio-on-EC2-Yarn.html).

Alluxio is Hadoop compatible. Existing data analytics applications, such as Spark and MapReduce
programs, can run on top of Alluxio without any code change. The project is open source under
[Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE) and is deployed at
many companies. It is one of the fastest growing open source projects. With three
years of open source history, Alluxio has attracted more than
[600 contributors](https://github.com/alluxio/alluxio/graphs/contributors) from over 150
institutions, including [Alibaba](http://www.alibaba.com), [Alluxio](http://www.alluxio.com/),
[Baidu](https://www.baidu.com), [CMU](https://www.cmu.edu/), [Google](https://www.google.com),
[IBM](https://www.ibm.com), [Intel](http://www.intel.com/), [NJU](http://www.nju.edu.cn/english/),
[Red Hat](https://www.redhat.com/), [UC Berkeley](https://amplab.cs.berkeley.edu/), and
[Yahoo](https://www.yahoo.com/).
The project is the storage layer of the Berkeley Data Analytics Stack
([BDAS](https://amplab.cs.berkeley.edu/bdas/)) and also part of the
[Fedora distribution](https://fedoraproject.org/wiki/SIGs/bigdata/packaging).
Today, Alluxio is deployed in production by 100s organizations, and runs on clusters that exceed
1,000 nodes.

[Downloads](http://alluxio.org/download/) |
[User Guide](Getting-Started.html) |
[Developer Guide](Contributing-to-Alluxio.html) |
[Meetup Group](https://www.meetup.com/Alluxio/) |
[Issue Tracking](https://alluxio.atlassian.net/browse/ALLUXIO) |
[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users) |
[Videos](https://www.youtube.com/channel/UCpibQsajhwqYPLYhke4RigA) |
[Powered By](Powered-By-Alluxio.html) |
[Github](https://github.com/alluxio/alluxio/) |
[Releases](http://alluxio.org/releases/)

## Current Features

* **[Flexible File API](File-System-API.html)** Alluxio's native API is similar to that of the
``java.io.File`` class, providing InputStream and OutputStream interfaces and efficient support for
memory-mapped I/O. We recommend using this API to get the best performance from Alluxio.
Alternatively, Alluxio provides a Hadoop compatible FileSystem interface, allowing Hadoop MapReduce
and Spark to use Alluxio in place of HDFS.

* **Pluggable Under Storage** To provide fault-tolerance, Alluxio checkpoints in-memory data to the
underlying storage system. It has a generic interface to make plugging different underlayer storage
systems easy. We currently support Amazon S3, Google Cloud Storage, OpenStack Swift, GlusterFS,
HDFS, MaprFS, Ceph, NFS, Alibaba OSS, and single-node local file systems, and support for many other
file systems is coming.

* **[Tiered Storage](Tiered-Storage-on-Alluxio.html)** With Tiered Storage, Alluxio can manage SSDs
and HDDs in addition to memory, allowing for larger datasets to be stored in Alluxio. Data will
automatically be managed between the different tiers, keeping hot data in faster tiers. Custom
policies are easily pluggable, and a pin concept allows for direct user control.

* **[Unified Namespace](Unified-and-Transparent-Namespace.html)** Alluxio enables effective
data management across different storage systems through the mount feature. Furthermore,
transparent naming ensures that file names and directory hierarchy for objects created in Alluxio
is preserved when persisting these objects to the underlying storage system.

* **[Lineage](Lineage-API.html)** Alluxio can achieve high throughput writes without compromising
fault-tolerance by using lineage, where lost output is recovered by re-executing the jobs that
created the output. With lineage, applications write output into memory, and Alluxio periodically
checkpoints the output into the under file system in an asynchronous fashion. In case of failures,
Alluxio launches job recomputation to restore the lost files.

* **[Web UI](Web-Interface.html) & [Command Line](Command-Line-Interface.html)** Users can browse
the file system easily through the web UI. Under debug mode, administrators can view detailed
information of each file, including locations, checkpoint path, etc. Users can also use
``./bin/alluxio fs`` to interact with Alluxio, e.g. copy data in and out of the file system.

## Getting Started

To quickly get Alluxio up and running, take a look at our [Getting Started](Getting-Started.html)
page which will go through how to deploy Alluxio and run some basic examples in a local environment.
See a list of [companies](Powered-By-Alluxio.html) that are using Alluxio.

## Downloads and More

You can get the released versions of Alluxio from the
[Project Downloads Page](http://alluxio.org/download). Each release comes with prebuilt
binaries compatibile with various Hadoop versions. If you would like to build the project from the
source code, check out the
[Building From Master Branch Documentation](Building-Alluxio-Master-Branch.html). If you have any
questions, please feel free to ask at our
[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users). For users
can not access Google Group, please use its
[mirror](http://alluxio-users.85194.x6.nabble.com/) (notes: the mirror does not have infomation
before May 2016).
