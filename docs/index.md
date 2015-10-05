---
layout: global
title: Overview
group: Home
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
[100 contributors](https://github.com/amplab/tachyon/graphs/contributors) from over 30 institutions,
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

* **Java-like File API** Tachyon's native API is similar to that of the ``java.io.File`` class,
providing InputStream and OutputStream interfaces and efficient support for memory-mapped I/O. We
recommend using this API to get the best performance from Tachyon.

* **Compatibility** Tachyon implements the Hadoop FileSystem interface. Therefore, Hadoop MapReduce
and Spark can run with Tachyon without modification. However, close integration is required to fully
take advantage of Tachyon, and we are working towards that. End-to-end latency speedup depends on
the workload and the framework, since various frameworks have different execution overhead.

* **Pluggable underlayer file system** To provide fault-tolerance, Tachyon checkpoints in-memory
data to the underlayer file system. It has a generic interface to make plugging different underlayer
file systems easy. We currently support HDFS, S3, GlusterFS, and single-node local file systems, and
support for many other file systems is coming.

* **Native support for raw tables** Table data with over hundreds of columns is common in data
warehouses. Tachyon provides native support for multi-columned data, with the option to put only hot
columns in memory to save space.

* **[Web UI](Web-Interface.html)** Users can browse the file system easily through the web UI.
Under debug mode, administrators can view detailed information of each file, including locations,
checkpoint path, etc.

* **[Command line interaction](Command-Line-Interface.html)** Users can use ``./bin/tachyon tfs``
to interact with Tachyon, e.g. copy data in and out of the file system.

# Getting Started
TODO(calvin): Add link to user guide

# Downloads
TODO(calvin): Add link to downloads