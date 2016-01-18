---
layout: global
title: Overview
group: Home
---

Tachyon is an open source memory-centric distributed storage system enabling reliable data sharing
at memory-speed across cluster jobs, possibly written in different computation frameworks, such as
Apache Spark, Apache MapReduce, and Apache Flink. In the big data ecosystem, Tachyon lies between
computation frameworks or jobs, such as Apache Spark, Apache MapReduce, or Apache Flink, and various
kinds of storage systems, such as Amazon S3, OpenStack Swift, GlusterFS, HDFS, Ceph, or OSS. Tachyon
brings significant performance improvement to the stack; for example, [Baidu](https://www.baidu.com)
uses Tachyon to improve their data analytics performance by 30 times. Beyond performance, Tachyon
bridges new workloads with data stored in traditional storage systems. Users can run Tachyon using
its standalone cluster mode, for example on Amazon EC2, or launch Tachyon with Apache Mesos or
Apache Yarn.

Tachyon is Hadoop compatible. This means that existing Spark and MapReduce programs can run on top
of Tachyon without any code changes. The project is open source
([Apache License 2.0](https://github.com/amplab/tachyon/blob/master/LICENSE)) and is deployed at
multiple companies. It is one of the fastest growing open source projects. With less than three
years open source history, Tachyon has attracted more than
[160 contributors](https://github.com/amplab/tachyon/graphs/contributors) from over 50 institutions,
including [Alibaba](http://www.alibaba.com), [Baidu](https://www.baidu.com),
[CMU](https://www.cmu.edu/), [IBM](https://www.ibm.com), [Intel](http://www.intel.com/),
[Red Hat](https://www.redhat.com/), [Tachyon Nexus](http://www.tachyonnexus.com/),
[UC Berkeley](https://amplab.cs.berkeley.edu/), and [Yahoo](https://www.yahoo.com/).
The project is the storage layer of the Berkeley Data Analytics Stack
([BDAS](https://amplab.cs.berkeley.edu/bdas/)) and also part of the
[Fedora distribution](https://fedoraproject.org/wiki/SIGs/bigdata/packaging).

[Github](https://github.com/amplab/tachyon/) |
[Releases](http://tachyon-project.org/releases/) |
[Downloads](http://tachyon-project.org/downloads/) |
[User Document](Getting-Started.html) |
[Developer Document](Contributing-to-Tachyon.html) |
[Meetup Group](https://www.meetup.com/Tachyon/) |
[JIRA](https://tachyon.atlassian.net/browse/TACHYON) |
[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/tachyon-users)

# Current Features

* **[Flexible File API](File-System-API.html)** Tachyon's native API is similar to that of the
``java.io.File`` class, providing InputStream and OutputStream interfaces and efficient support for
memory-mapped I/O. We recommend using this API to get the best performance from Tachyon.
Alternatively, Tachyon provides a Hadoop compatible FileSystem interface, allowing Hadoop MapReduce
and Spark to use Tachyon in place of HDFS.

* **Pluggable Under Storage** To provide fault-tolerance, Tachyon checkpoints in-memory data to the
underlying storage system. It has a generic interface to make plugging different underlayer storage
systems easy. We currently support Amazon S3, OpenStack Swift, Apache HDFS, GlusterFS, and
single-node local file systems, and support for many other file systems is coming.

* **[Tiered Storage](Tiered-Storage-on-Tachyon.html)** With Tiered Storage, Tachyon can manage SSDs
and HDDs in addition to memory, allowing for larger datasets to be stored in Tachyon. Data will
automatically be managed between the different tiers, keeping hot data in faster tiers. Custom
policies are easily pluggable, and a pin concept allows for direct user control.

* **[Unified Namespace](Unified-and-Transparent-Namespace.html)** Tachyon enables effective
data management across different storage systems through the mount feature. Furthermore, 
transparent naming ensures that file names and directory hierarchy for objects created in Tachyon 
is preserved when persisting these objects to the underlying storage system.

* **[Lineage](Lineage-API.html)** Tachyon can achieve high throughput writes without compromising
fault-tolerance by using lineage, where lost output is recovered by re-executing the jobs that
created the output. With lineage, applications write output into memory, and Tachyon periodically
checkpoints the output into the under file system in an asynchronous fashion. In case of failures,
Tachyon launches job recomputation to restore the lost files.

* **[Web UI](Web-Interface.html) & [Command Line](Command-Line-Interface.html)** Users can browse
the file system easily through the web UI. Under debug mode, administrators can view detailed
information of each file, including locations, checkpoint path, etc. Users can also use
``./bin/tachyon tfs`` to interact with Tachyon, e.g. copy data in and out of the file system.

# Getting Started

To quickly get Tachyon up and running, take a look at our [Getting Started](Getting-Started.html)
page which will go through how to deploy Tachyon and run some basic examples in a local enviornment.

# Downloads

You can get the released versions of Tachyon from the
[Project Downloads Page](http://tachyon-project.org/downloads). Each release comes with prebuilt
binaries compatibile with various Hadoop versions. If you would like to build the project from the
source code, check out the
[Building From Master Branch Documentation](Building-Tachyon-Master-Branch.html).
