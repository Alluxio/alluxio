---
layout: global
title: Running Apache Kylin on Alluxio
nickname: Apache Kylin
group: Frameworks
priority: 2
---

This guide describes how to run [Apache Kylin](http://kylin.apache.org/) on Alluxio, so
that you can speed up the cube build phase in Kylin.

# Prerequisites

The prerequisite for this part is that you have
[Java](Java-Setup.html). Alluxio cluster should also be
set up in accordance to these guides for either [Local Mode](Running-Alluxio-Locally.html) or
[Cluster Mode](Running-Alluxio-on-a-Cluster.html).

Please [Download Kylin](http://kylin.apache.org/download/).

# Configuration

Kylin requires a setup environment including [Hive](http://hive.apache.org/),
[Hadoop](http://hadoop.apache.org/) and [HBase](http://hbase.apache.org/). You
need to refer the following documentations for how to run these frameworks on Alluxio:

 [Running Hadoop MapReduce on Alluxio](Running-Hadoop-MapReduce-on-Alluxio.html)
 
 [Running Apache HBase on Alluxio](Running-HBase-on-Alluxio.html)
 
 [Running Apache Hive with Alluxio](Running-Hive-with-Alluxio.html)

#### Set property in Hadoop `core-site.xml`

You need to change the following property in `core-site.xml` in your Hadoop installation
`etc/hadoop` directory, which ensures that Kylin's Hadoop engine uses Alluxio as its default
filesystem.

```xml
<property>
   <name>fs.defaultFS</name>
   <value>alluxio://<master_hostname>:19998</value>
</property>
```

#### Add additional Alluxio site properties to Kylin

If there are any Alluxio site properties you want to specify for Kylin, add those to the
corresponding configuration file in Kylin's `conf` directory. For example,
change `alluxio.user.file.writetype.default` from default `MUST_CACHE` to `CACHE_THROUGH` in
`kylin_hive_conf.xml`:

```xml
<property>
<name>alluxio.user.file.writetype.default</name>
<value>CACHE_THROUGH</value>
</property>
```

Then you can follow the [Kylin documentation](http://kylin.apache.org/docs16/) to use Kylin.

# Kylin cube build example

Follow [Quick Start with Sample Cube](http://kylin.apache.org/docs16/tutorial/kylin_sample.html)
in Kylin's website, you can verify if Alluxio can accelerate the cube build phase in Kylin.
