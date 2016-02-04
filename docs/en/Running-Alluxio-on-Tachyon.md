---
layout: global
title: Running Apache Flink on Alluxio
nickname: Apache Flink
group: Frameworks
priority: 2
---

This guide describes how to get Alluxio running with [Apache Flink](http://flink.apache.org/), so
that you can easily work with files stored in Alluxio.

# Prerequisites

The prerequisite for this part is that you have
[Java](Java-Setup.html). We also assume that you have set up
Alluxio in accordance to these guides [Local Mode](Running-Alluxio-Locally.html) or
[Cluster Mode](Running-Alluxio-on-a-Cluster.html).

Please find the guides for setting up Flink on the Apache Flink [website](http://flink.apache.org/).

# Configuration

Apache Flink allows to use Alluxio through a generic file system wrapper for the Hadoop file system.
Therefore, the configuration of Alluxio is done mostly in Hadoop configuration files.

#### Set property in `core-site.xml`

If you have a Hadoop setup next to the Flink installation, add the following property to the
`core-site.xml` configuration file:

{% include Running-Flink-on-Alluxio/core-site-configuration.md %}

In case you don't have a Hadoop setup, you have to create a file called `core-site.xml` with the
following contents:

{% include Running-Flink-on-Alluxio/create-core-site.md %}

#### Specify path to `core-site.xml` in `conf/flink-config.yaml`

Next, you have to specify the path to the Hadoop configuration in Flink. Open the 
`conf/flink-config.yaml` file in the Flink root directory and set the `fs.hdfs.hadoopconf`
configuration value to the **directory** containing the `core-site.xml`. (For newer Hadoop versions,
the directory usually ends with `etc/hadoop`.)

#### Make the Alluxio Client jar available to Flink

In the last step, we need to make the Alluxio `jar` file available to Flink, because it contains the
configured `tachyon.hadoop.TFS` class.

There are different ways to achieve that:

- Put the `tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar` file into the
`lib` directory of Flink (for local and standalone cluster setups)
- Put the `tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar` file into the
`ship` directory for Flink on YARN.
- Specify the location of the jar file in the `HADOOP_CLASSPATH` environment variable (make sure its
available on all cluster nodes as well). For example like this:

```bash
export HADOOP_CLASSPATH=/pathToAlluxio/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar
```

# Using Alluxio with Flink

To use Alluxio with Flink, just specify paths with the `tachyon://` scheme.

If Alluxio is installed locally, a valid path would look like this
`tachyon://localhost:19998/user/hduser/gutenberg`.
