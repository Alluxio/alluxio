---
layout: global
title: Running Apache Flink on Alluxio
nickname: Apache Flink
group: Compute Integrations
priority: 2
---

* Table of Contents
{:toc}

This guide describes how to get Alluxio running with [Apache Flink](http://flink.apache.org/), so
that you can easily work with files stored in Alluxio.

## Prerequisites

* Setup Java for Java 8 Update 161 or higher (8u161+), 64-bit.
* Alluxio has been set up and is running.
* Flink has been set up and is running.

## Configuration

Apache Flink allows to use Alluxio through a generic file system wrapper for the Hadoop file system.
Therefore, the configuration of Alluxio is done mostly in Hadoop configuration files.

### Set property in `core-site.xml`

If you have a Hadoop setup next to the Flink installation, add the following property to the
`core-site.xml` configuration file:

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
```

In case you don't have a Hadoop setup, you have to create a file called `core-site.xml` with the
following contents:

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```

### Specify path to `core-site.xml` in `conf/flink-conf.yaml`

Next, you have to specify the path to the Hadoop configuration in Flink. Open the
`conf/flink-conf.yaml` file in the Flink root directory and set the `fs.hdfs.hadoopconf`
configuration value to the **directory** containing the `core-site.xml`. (For newer Hadoop versions,
the directory usually ends with `etc/hadoop`.)

### Distribute the Alluxio Client Jar

In order to communicate with Alluxio, we need to provide Flink programs with the Alluxio Core Client
jar. We recommend you to download the tarball from
Alluxio [download page](https://www.alluxio.io/download/).
Alternatively, advanced users can choose to compile this client jar from the source code
by following the instructions [here]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}).
The Alluxio client jar can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}`.

We need to make the Alluxio `jar` file available to Flink, because it contains the configured
`alluxio.hadoop.FileSystem` class.

There are different ways to achieve that:

- Put the `{{site.ALLUXIO_CLIENT_JAR_PATH}}` file into the `lib` directory of Flink (for local and
standalone cluster setups)
- Put the `{{site.ALLUXIO_CLIENT_JAR_PATH}}` file into the `ship` directory for Flink on YARN.
- Specify the location of the jar file in the `HADOOP_CLASSPATH` environment variable (make sure its
available on all cluster nodes as well). For example like this:

```console
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}
```

### Translate additional Alluxio site properties to Flink

In addition, if there are any client-related properties specified in `conf/alluxio-site.properties`,
translate those to `env.java.opts` in `{FLINK_HOME}/conf/flink-conf.yaml` for Flink to pick up
Alluxio configuration. For example, if you want to configure Alluxio client to use CACHE_THROUGH as
the write type, you should add the following to `{FLINK_HOME}/conf/flink-conf.yaml`.

```yaml
env.java.opts: -Dalluxio.user.file.writetype.default=CACHE_THROUGH
```

## Using Alluxio with Flink

To use Alluxio with Flink, just specify paths with the `alluxio://` scheme.

If Alluxio is installed locally, a valid path would look like this
`alluxio://localhost:19998/user/hduser/gutenberg`.

### Wordcount Example

This example assumes you have set up Alluxio and Flink as previously described.

Put the file `LICENSE` into Alluxio, assuming you are in the top level Alluxio project directory:

```console
$ bin/alluxio fs copyFromLocal LICENSE alluxio://localhost:19998/LICENSE
```

Run the following command from the top level Flink project directory:

```console
$ bin/flink run examples/batch/WordCount.jar \
  --input alluxio://localhost:19998/LICENSE \
  --output alluxio://localhost:19998/output
```

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There should be an output file `output` which contains the word counts of the file `LICENSE`.
