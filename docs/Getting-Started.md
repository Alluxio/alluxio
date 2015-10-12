---
layout: global
title: Getting Started
group: User Guide
priority: 0
---

* Table of Contents
{:toc}

### Setting up Tachyon

Tachyon can be configured in a variety of modes. The simplest setup for new users is to
[run Tachyon locally](Running-Tachyon-Locally.html). To experiment with a cluster setup, see the
[virtual box](Running-Tachyon-on-Virtual-Box.html) or [AWS](Running-Tachyon-on-EC2.html) tutorials.

### Configuring the Under Storage

Tachyon can be seen as a data exchange layer and benefits from having reliable persistant storage
backing it. Depending on the production environment, different under storage will be preferred.
Tachyon can be integrated with any under storage, provided an under storage connector is written.
Currently, [S3](Configuring-Tachyon-with-S3.html), [HDFS](Configuring-Tachyon-with-HDFS.html),
[Swift](Configuring-Tachyon-with-Swift.html), and
[GlusterFS](Configuring-Tachyon-with-GlusterFS.html) are supported.

### Configuring an Application

Tachyon provides a [file system interface](File-System-API.html) to applications in order to
interact with data in Tachyon. If you want to directly write an application on top of Tachyon,
simply add the Tachyon-Client dependency to the program. For example, if the application is built
using Maven:

    <dependency>
        <groupId>org.tachyon-project</groupId>
        <artifactId>tachyon-client</artifactId>
        ...
    </dependency>

A special set of applications leveraging Tachyon are computation frameworks. Transitioning these
frameworks to use Tachyon is almost effortless, especially if the framework is already integrated
through the HDFS FileSystem interface. Since Tachyon also provides an implementation of the
interface, the only modification required is to change the data path scheme from
`hdfs://master-hostname:port` to `tachyon://master-hostname:port`. See the tutorials for
[Spark](Running-Spark-on-Tachyon.html),
[Hadoop Map-Reduce](Running-Hadoop-MapReduce-on-Tachyon.html), or
[Flink](Running-Flink-on-Tachyon.html) for examples.

### Configuring the System

Tachyon has various knobs to tune the system to perform best for separate use cases. For an
application, Tachyon reads custom configurations from the specified tachyon-site.properties file or
from java options passed through the command line. See
[configuration settings](Configuration-Settings.html) for more information about the specific
adjustable values.

### Advanced Features

While Tachyon can provide significant performance gains simply through accelerating data
input/output, Tachyon also provides advanced features tailored to specific use cases.

* [Tiered storage](Tiered-Storage-on-Tachyon.html) provides additional resource for Tachyon to
manage, allowing for workloads larger than memory to be optimized by Tachyon storage.
* [Lineage](Lineage.html) provides an alternative to costly disk replication for fault tolerance and
data durability, greatly improving write performance.
* [Mounting and transparenting naming](Mounting-and-Transparent-Naming.html) provides the ability
for users to import data from existing storage and easily handle deployments where not all systems
are Tachyon-aware.