---
layout: global
title: Tachyon Overview
---

Tachyon is a memory-centric distributed storage system enabling reliable data sharing at memory-speed
across cluster frameworks, such as Spark and MapReduce. It achieves high performance by leveraging
lineage information and using memory aggressively. Tachyon caches working set files in memory,
thereby avoiding going to disk to load datasets that are frequently read. This enables different
jobs/queries and frameworks to access cached files at memory speed.

Tachyon is Hadoop compatible. Existing Spark and MapReduce programs can run on top of it without
any code change. The project is open source
([Apache License 2.0](https://github.com/amplab/tachyon/blob/master/LICENSE)) and is deployed at
multiple companies. It has more than
[80 contributors](https://github.com/amplab/tachyon/graphs/contributors) from over 30 institutions,
including [Yahoo](https://www.yahoo.com/), [Intel](http://www.intel.com/),
[Red Hat](http://www.redhat.com/), and [Tachyon Nexus](http://www.tachyonnexus.com/).
The project is the storage layer of the Berkeley Data Analytics
Stack ([BDAS](https://amplab.cs.berkeley.edu/bdas/)) and also part of the
[Fedora distribution](https://fedoraproject.org/wiki/SIGs/bigdata/packaging).

[Github Repository](https://github.com/amplab/tachyon/) |
[Releases and Downloads](https://github.com/amplab/tachyon/releases) |
[User Documentation](#user-documentation) |
[Developer Documentation](#developer-documentation) |
[Meetup Group](http://www.meetup.com/Tachyon/) |
[JIRA](https://tachyon.atlassian.net/browse/TACHYON) |
[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/tachyon-users)

# Current Features

* **Java-like File API**: Tachyon's native API is similar to that of the ``java.io.File`` class,
providing InputStream and OutputStream interfaces and efficient support for memory-mapped I/O. We
recommend using this API to get the best performance from Tachyon.

* **Compatibility**: Tachyon implements the Hadoop FileSystem interface. Therefore, Hadoop MapReduce
and Spark can run with Tachyon without modification. However, close integration is required to fully
take advantage of Tachyon, and we are working towards that. End-to-end latency speedup depends on
the workload and the framework, since various frameworks have different execution overhead.

* **Pluggable underlayer file system**: To provide fault-tolerance, Tachyon checkpoints in-memory
data to the underlayer file system. It has a generic interface to make plugging different underlayer
file systems easy. We currently support HDFS, S3, GlusterFS, and single-node local file systems, and
support for many other file systems is coming.

* **Native support for raw tables**: Table data with over hundreds of columns is common in data
warehouses. Tachyon provides native support for multi-columned data, with the option to put only hot
columns in memory to save space.

* **[Web UI](Web-Interface.html)**: Users can browse the file system easily through the web UI.
Under debug mode, administrators can view detailed information of each file, including locations,
checkpoint path, etc.

* **[Command line interaction](Command-Line-Interface.html)**: Users can use ``./bin/tachyon tfs``
to interact with Tachyon, e.g. copy data in and out of the file system.

# User Documentation

**Deployment Guide:**

* [Single Node](Running-Tachyon-Locally.html)
* [Cluster](Running-Tachyon-on-a-Cluster.html)
* [Master Fault Tolerant Cluster](Fault-Tolerant-Tachyon-Cluster.html)
* [Tachyon Deploy Module (Virtualbox and AWS EC2)](Deploy-Module.html)
* [Amazon AWS Through mesos/spark-ec2](Running-Tachyon-on-EC2.html)

**Configuration:**

* [Configure Underlayer Storage System](Setup-UFS.html): Learn how to configure underlayer storage
system or to create a new one.
* [Configuration Settings](Configuration-Settings.html): How to configure Tachyon.

**Frameworks on Tachyon:**

* [Running Apache Spark on Tachyon](Running-Spark-on-Tachyon.html): Get Apache Spark running on
Tachyon
* [Running Shark on Tachyon](Running-Shark-on-Tachyon.html): Get Shark running on Tachyon
* [Running Apache Hadoop MapReduce on Tachyon](Running-Hadoop-MapReduce-on-Tachyon.html): Get Apache
Hadoop MapReduce running on Tachyon
* [Running Apache Flink on Tachyon](Running-Flink-on-Tachyon.html): Get Apache Flink running on
Tachyon

**Others:**

* [Command-Line Interface](Command-Line-Interface.html): Interact with Tachyon through the command
line.
* [Syncing the Underlayer Storage System](Syncing-the-Underlying-Filesystem.html): Make Tachyon
understand an existing underlayer storage system.
* [FAQ](FAQ.html)
* [Tachyon Java API (Javadoc)](api/java/index.html)
* [Tiered Storage (Beta)](Tiered-Storage-on-Tachyon.html)

**Tachyon Presentations:**

* Strata and Hadoop World 2014 (October, 2014) [pdf](http://goo.gl/fP9qkF) [pptx](http://goo.gl/GOyUbI)
* Spark Summit 2014 (July, 2014) [pdf](http://goo.gl/DKrE4M)
* Strata and Hadoop World 2013 (October, 2013) [pdf](http://goo.gl/AHgz0E)

# Developer Documentation

[Contributing to Tachyon](Contributing-to-Tachyon.html)

[Building Tachyon Master Branch](Building-Tachyon-Master-Branch.html)

# External resources

Tachyon Mini Courses:

* [AMPCamp 5](http://ampcamp.berkeley.edu/5/exercises/)
* [Strata 2014](http://ampcamp.berkeley.edu/big-data-mini-course/)

[Hot Rod Hadoop With Tachyon on Fedora 21](http://timothysc.github.io/blog/2014/02/17/bdas-tachyon/)

# Support or Contact

You are welcome to join our
[mailing list](https://groups.google.com/forum/?fromgroups#!forum/tachyon-users) to discuss
questions and make suggestions. We use [JIRA](https://tachyon.atlassian.net/browse/TACHYON) to
track development and issues. If you are interested in trying out Tachyon in your cluster, please
contact [Haoyuan](mailto:haoyuan@cs.berkeley.edu).

# Acknowledgement

Tachyon is an open source project started in the
[UC Berkeley AMP Lab](http://amplab.cs.berkeley.edu).
This research is supported in part by NSF CISE Expeditions Award CCF-1139158, LBNL Award 7076018,
and DARPA XData Award FA8750-12-2-0331, and gifts from Amazon Web Services, Google, SAP, The Thomas
and Stacey Siebel Foundation, Adatao, Adobe, Apple, Inc., Blue Goji, Bosch, C3Energy, Cisco, Cray,
Cloudera, EMC, Ericsson, Facebook, Guavus, Huawei, Informatica, Intel, Microsoft, NetApp, Pivotal,
Samsung, Splunk, Virdata, VMware, and Yahoo!.

We would also like to thank to our project
[contributors](https://github.com/amplab/tachyon/graphs/contributors).

# Related Projects

[Berkeley Data Analysis Stack (BDAS)](https://amplab.cs.berkeley.edu/bdas/) from AMPLab at Berkeley
