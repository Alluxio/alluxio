---
layout: global
title: Getting Started
group: User Guide
priority: 0
---

* Table of Contents
{:toc}

### Setting up Alluxio

Alluxio can be configured in a variety of modes. The simplest setup for new users is to
[run Alluxio locally](Running-Alluxio-Locally.html). To experiment with a cluster setup, see the
[virtual box](Running-Alluxio-on-Virtual-Box.html) or [Amazon AWS](Running-Alluxio-on-EC2.html)
tutorials.

### Configuring the Under Storage

Alluxio can be seen as a data exchange layer and benefits from having reliable persistent storage
backing it. Depending on the production environment, different under storage will be preferred.
Alluxio can be integrated with any under storage, provided an under storage connector is implemented.
Currently, [Amazon S3](Configuring-Alluxio-with-S3.html),
[OpenStack Swift](Configuring-Alluxio-with-Swift.html),
[GlusterFS](Configuring-Alluxio-with-GlusterFS.html), and
[Apache HDFS](Configuring-Alluxio-with-HDFS.html), are supported.

### Configuring an Application

Alluxio provides a [file system interface](File-System-API.html) to applications to let them
interact with data stored in Alluxio. If you want to directly write an application on top of
Alluxio, simply add the `alluxio-core-client` dependency to your program. For example, if the
application is built using Maven:

{% include Getting-Started/config-application.md %}

A special set of applications leveraging Alluxio are computation frameworks. Transitioning these
frameworks to use Alluxio is almost effortless, especially if the framework is already integrated
with the Hadoop FileSystem interface. Since Alluxio also provides an implementation of the
interface, the only modification required is to change the data path scheme from
`hdfs://master-hostname:port` to `alluxio://master-hostname:port`. See the tutorials for
[Apache Spark](Running-Spark-on-Alluxio.html),
[Apache Hadoop MapReduce](Running-Hadoop-MapReduce-on-Alluxio.html), or
[Apache Flink](Running-Flink-on-Alluxio.html) for examples.

### Configuring the System

Alluxio has various knobs to tune the system to perform best for separate use cases. For an
application, Alluxio reads custom configurations from the specified `alluxio-site.properties` file
or from Java options passed through the command line. See
[configuration settings](Configuration-Settings.html) for more information about the specific
adjustable values.

### Additional Features

Beyond providing a data sharing layer powered by fast storage, Alluxio also comes with many useful
features for developers and admins.

* [Command Line Interface](Command-Line-Interface.html) allows users to access and manipulate data
in Alluxio through a light-weight shell provided in the codebase.
* [Metrics Collection](Metrics-System.html) allows admins to easily monitor the state of the system
and discover any bottlenecks or inefficiencies.
* [Web Interface](Web-Interface.html) gives a visually rich representation of the data in Alluxio,
but provides a read-only view.

### Advanced Features

Beyond providing significant performance gains simply through accelerating data input/output,
Alluxio also provides following advanced features.

* [Tiered storage](Tiered-Storage-on-Alluxio.html) provides additional resources for Alluxio to
manage (such as SSD or HHD), allowing for data sets that cannot fit into memory to still take
advantage of the Alluxio architecture.
* [Unified and Transparent Namespace](Unified-and-Transparent-Namespace.html) provides the ability
for users to manage data from existing storage systems and easily handle deployments where not all
systems are Alluxio-aware.
* [Lineage](Lineage-API.html) provides an alternative to costly disk replication for fault tolerance
and data durability, greatly improving write performance.
