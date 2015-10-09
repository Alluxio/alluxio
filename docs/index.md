---
layout: global
title: Overview
group: Home
---

Tachyon is an open source memory-centric distributed storage system enabling reliable data sharing
at memory-speed across cluster frameworks, such as Spark and MapReduce. It achieves high performance
by leveraging lineage information and using memory aggressively. Tachyon caches working set files in
memory, thereby avoiding going to disk to load datasets that are frequently read. This enables
different jobs/queries and frameworks to access cached files at memory speed.

Tachyon is Hadoop compatible. Existing Spark and MapReduce programs can run on top of it without
any code change. The project is open source
([Apache License 2.0](https://github.com/amplab/tachyon/blob/master/LICENSE)) and is deployed at
multiple companies. It has more than
[150 contributors](https://github.com/amplab/tachyon/graphs/contributors) from over 50 institutions,
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

* **[Flexible File API](File-System-API.md)** Tachyon's native API is similar to that of the
``java.io.File`` class, providing InputStream and OutputStream interfaces and efficient support for
memory-mapped I/O. We recommend using this API to get the best performance from Tachyon.
Alternatively, Tachyon provides a Hadoop compatible FileSystem interface, allowing Hadoop MapReduce
and Spark can run with Tachyon without modification.

* **Pluggable Under Storage** To provide fault-tolerance, Tachyon checkpoints in-memory data to the
underlayer storage system. It has a generic interface to make plugging different underlayer storage
systems easy. We currently support HDFS, S3, Swift, GlusterFS, and single-node local file systems,
and support for many other file systems is coming.

* **[Tiered Storage](Tiered-Storage-on-Tachyon.html)** With Tiered Storage, Tachyon can manage SSDs
and HDDs in addition to memory, allowing for larger datasets to be stored in Tachyon. Data will
automatically be managed between the different tiers, keeping hot data in faster tiers. Custom
policies are easily pluggable, and a pin concept allows for direct user control.

* **[Lineage](Lineage-API.md)**

* **[Web UI](Web-Interface.html) & [Command Line](Command-Line-Interface.html)** Users can browse
the file system easily through the web UI. Under debug mode, administrators can view detailed
information of each file, including locations, checkpoint path, etc. Users can also use
``./bin/tachyon tfs`` to interact with Tachyon, e.g. copy data in and out of the file system.

* **[Mounting & Transparency](Mounting-and-Transparent-Naming.md)**

# Getting Started

To quickly get Tachyon up and running, take a look at our [Getting Started](Getting-Started.html)
page which will go through how to deploy Tachyon and run some basic examples in a local enviornment.

# Downloads

You can get the released versions of Tachyon from the
[Project Downloads Page](http://tachyon-project.org/downloads). Each release comes with prebuilt
binaries compatibile with various Hadoop versions. If you would like to build the project from the
source code, check out the
[Building From Master Branch Documentation](Building-Tachyon-Master-Branch.html).