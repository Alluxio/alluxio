---
layout: global
title: Overview
group: Home
---

Alluxio, formerly known as Tachyon, the world’s first memory-centric virtual distributed storage system, unifies data access
and bridges computation frameworks and underlying storage systems. Applications only need to connect
with Alluxio to access data stored in any underlying storage systems. Additionally, Alluxio’s
memory-centric architecture enables data access orders of magnitude faster than existing solutions.

In the big data ecosystem, Alluxio lies between computation frameworks or jobs, such as Apache
Spark, Apache MapReduce, or Apache Flink, and various kinds of storage systems, such as Amazon S3,
OpenStack Swift, GlusterFS, HDFS, Ceph, or OSS. Alluxio brings significant performance improvement
to the ecosystem; for example, [Baidu](https://www.baidu.com) uses Alluxio to improve speedup the
throughput of their data analytics pipeline
[30 times](http://www.alluxio.com/assets/uploads/2016/02/Baidu-Case-Study.pdf).
Barclays makes the impossible possible with Alluxio to accelerate jobs from
[hours to seconds](https://dzone.com/articles/Accelerate-In-Memory-Processing-with-Spark-from-Hours-to-Seconds-With-Tachyon).
Beyond performance, Alluxio bridges new workloads with data stored in traditional storage systems.
Users can run Alluxio using its standalone cluster mode, for example on Amazon EC2, or launch
Alluxio with Apache Mesos or Apache Yarn.

Alluxio is Hadoop compatible. This means that existing Spark and MapReduce programs can run on top
of Alluxio without any code changes. The project is open source
([Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE)) and is deployed at
multiple companies. It is one of the fastest growing open source projects. With less than three
years of open source history, Alluxio has attracted more than
[190 contributors](https://github.com/alluxio/alluxio/graphs/contributors) from over 50
institutions, including [Alibaba](http://www.alibaba.com), [Alluxio](http://www.alluxio.com/),
[Baidu](https://www.baidu.com), [CMU](https://www.cmu.edu/), [IBM](https://www.ibm.com),
[Intel](http://www.intel.com/), [NJU](http://www.nju.edu.cn/english/), [Red Hat](https://www.redhat.com/),
[UC Berkeley](https://amplab.cs.berkeley.edu/), and [Yahoo](https://www.yahoo.com/).
The project is the storage layer of the Berkeley Data Analytics Stack
([BDAS](https://amplab.cs.berkeley.edu/bdas/)) and also part of the
[Fedora distribution](https://fedoraproject.org/wiki/SIGs/bigdata/packaging).

[Github](https://github.com/alluxio/alluxio/) |
[Releases](http://alluxio.org/releases/) |
[Downloads](http://alluxio.org/download/) |
[User Document](Getting-Started.html) |
[Developer Document](Contributing-to-Alluxio.html) |
[Meetup Group](https://www.meetup.com/Alluxio/) |
[JIRA](https://alluxio.atlassian.net/browse/ALLUXIO) |
[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users) |
[Powered By](Powered-By-Alluxio.html)

# Current Features

* **[Flexible File API](File-System-API.html)** Alluxio's native API is similar to that of the
``java.io.File`` class, providing InputStream and OutputStream interfaces and efficient support for
memory-mapped I/O. We recommend using this API to get the best performance from Alluxio.
Alternatively, Alluxio provides a Hadoop compatible FileSystem interface, allowing Hadoop MapReduce
and Spark to use Alluxio in place of HDFS.

* **Pluggable Under Storage** To provide fault-tolerance, Alluxio checkpoints in-memory data to the
underlying storage system. It has a generic interface to make plugging different underlayer storage
systems easy. We currently support Amazon S3, OpenStack Swift, Apache HDFS, GlusterFS, and
single-node local file systems, and support for many other file systems is coming.

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

# Getting Started

To quickly get Alluxio up and running, take a look at our [Getting Started](Getting-Started.html)
page which will go through how to deploy Alluxio and run some basic examples in a local environment.

# Downloads

You can get the released versions of Alluxio from the
[Project Downloads Page](http://alluxio.org/download). Each release comes with prebuilt
binaries compatibile with various Hadoop versions. If you would like to build the project from the
source code, check out the
[Building From Master Branch Documentation](Building-Alluxio-Master-Branch.html).
