---
layout: global
title: Tachyon Overview
---

Tachyon is a fault tolerant distributed file system enabling reliable file sharing at memory-speed
across cluster frameworks, such as Spark and MapReduce. It achieves high performance by leveraging
lineage information and using memory aggressively. Tachyon caches working set files in memory, and
enables different jobs/queries and frameworks to access cached files at memory speed. Thus, Tachyon
avoids going to disk to load datasets that are frequently read.

[Releases and Downloads](https://github.com/amplab/tachyon/releases) | [User Documentation](#user-
documentation) | [Developer Documentation](#developer-documentation) |
[Acknowledgement](#acknowledgement)

# Current Features

* **Java-like File API**: Tachyon's native API is similar to JAVA file, providing InputStream,
OutputStream interface, and efficient support for memory mapped I/O. Using this API would get
best performance from Tachyon

* **Compatibility**: Tachyon implements Hadoop FileSystem interface, therefore, Hadoop MapReduce and
Spark can run with Tachyon without modification. However, close integration is required to fully
take advantage of Tachyon and we are working towards that. End-to-end latency speedup depends on the
workload and the framework, since various frameworks have different execution overhead.


* **Native support for raw tables**: Table data with hundreds or more columns is common in data
warehouses. Tachyon provides native support for multi-columned data. The user can choose to only put
hot columns in memory.

* **Pluggable underlayer file system**: Tachyon checkpoints in memory data into the underlayer file
system. Tachyon has a generic interface to make plugging an underlayer file system easy. It
currently supports HDFS, and single node local file system.

* **Web UI**: Users can browse the file system easily through web UI. Under debug mode,
administrators can view detailed information of each file, including locations, checkpoint path,
etc.

* **Command line interaction**: Users can use ./bin/tachyon tfs to interact with Tachyon, e.g. copy
data in and out of the file system.

# User Documentation

[Running Tachyon Locally](Running-Tachyon-Locally.html): Get Tachyon up and running on a single node
for a quick spin in ~ 5 mins.

[Running Tachyon on a Cluster](Running-Tachyon-on-a-Cluster.html): Get Tachyon up and running on
your own cluster.

[Fault Tolerant Tachyon Cluster](Fault-Tolerant-Tachyon-Cluster.html): Make your cluster fault
tolerant.

[Running Spark on Tachyon](Running-Spark-on-Tachyon.html): Get Spark running on Tachyon

[Running Shark on Tachyon](Running-Shark-on-Tachyon.html): Get Shark running on Tachyon

[Running Hadoop MapReduce on Tachyon](Running-Hadoop-MapReduce-on-Tachyon.html): Get Hadoop
MapReduce running on Tachyon

[Configuration-Settings](Configuration-Settings.html): How to configure Tachyon.

[Command-Line-Interface](Command-Line-Interface.html): Interact with Tachyon through command line.

[Syncing the Underlying Filesystem](Syncing-the-Underlying-Filesystem.html): Make Tachyon understand
an existing underlayer filesystem.

[Tachyon Presentation](http://goo.gl/nhmcWA) at AMPCamp 2013 (August, 2013)

# Developer Documentation

[Startup Tasks for New Contributors](Startup-Tasks-for-New-Contributors.html): For people who are
interested in contributing.

[Building Tachyon Master Branch](Building-Tachyon-Master-Branch.html)

# Support or Contact

You are welcome to join our [mailing list](https://groups.google.com/forum/?fromgroups#!forum
/tachyon-users) to discuss questions and make suggestions. We use [JIRA](https://spark-
project.atlassian.net/browse/TACHYON) to track development / issues. If you are interested in trying
out Tachyon in your cluster, please contact [Haoyuan](mailto:haoyuan@cs.berkeley.edu).

# Acknowledgement

Tachyon is an open source project started in the [UC Berkeley AMP
Lab](http://amplab.cs.berkeley.edu). This research and development is supported in part by NSF CISE
Expeditions award CCF-1139158 and DARPA XData Award FA8750-12-2-0331, and gifts from Amazon Web
Services, Google, SAP, Blue Goji, Cisco, Clearstory Data, Cloudera, Ericsson, Facebook, General
Electric, Hortonworks, Huawei, Intel, Microsoft, NetApp, Oracle, Quanta, Samsung, Splunk, VMware and
Yahoo!.

# Related Projects

[Berkeley Data Analysis Stack (BDAS)](https://amplab.cs.berkeley.edu/bdas/) from AMPLab at Berkeley

