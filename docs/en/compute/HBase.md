---
layout: global
title: Running Apache HBase on Alluxio
nickname: Apache HBase
group: Compute
priority: 2
---

This guide describes how to run [Apache HBase](http://hbase.apache.org/), so
that you can easily store HBase tables into Alluxio at various storage levels.

* Table of Contents
{:toc}

## Prerequisites

* Alluxio has been set up and is running.
* Make sure that the Alluxio client jar is available.
This Alluxio client jar file can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the tarball
downloaded from Alluxio [download page](http://www.alluxio.org/download).
Alternatively, advanced users can compile this client jar from the source code
by following the [instructions]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}).
* [Deploy HBase](https://hbase.apache.org/book.html#configuration)
Please follow this guides for setting up HBase.

## Basic Setup

Apache HBase allows you to use Alluxio through a generic file system wrapper for the Hadoop file system.
Therefore, the configuration of Alluxio is done mostly in HBase configuration files.

### Set property in `hbase-site.xml`

Change the `hbase.rootdir` property in `conf/hbase-site.xml`:
> You do not need to create the `/hbase` directory in Alluxio, HBase will do this for you.

```xml
<property>
  <name>hbase.rootdir</name>
  <value>alluxio://master_hostname:port/hbase</value>
</property>
```

Add the following property to the same file `hbase-site.xml`.
(make sure it is configured in all HBase cluster nodes):

```xml
<property>
  <name>hbase.regionserver.hlog.syncer.count</name>
  <value>1</value>
</property>
```

This property is required to prevent HBase from flushing Alluxio file stream in a thread unsafe
way.

### Distribute the Alluxio Client jar

We need to make the Alluxio client jar file available to HBase, because it contains the configured
`alluxio.hadoop.FileSystem` class.

Specify the location of the jar file in the `$HBASE_CLASSPATH` environment variable (make sure it's available
on all cluster nodes). For example:

```bash
export HBASE_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HBASE_CLASSPATH}
```

Alternative ways are described in the [Advanced Setup]({{ '/en/compute/HBase.html' | relativize_url }}#advanced-setup)

## Example

Start HBase:

```bash
$ ${HBASE_HOME}/bin/start-hbase.sh
```

Visit HBase Web UI at `http://<HBASE_MASTER_HOSTNAME>:16010` to confirm that HBase is running on Alluxio
(check the `HBase Root Directory` attribute):

![HBaseRootDirectory]({{ site.baseurl }}{% link img/screenshot_start_hbase_webui.png %})

And visit Alluxio Web UI at `http://<ALLUXIO_MASTER_HOSTNAME>:19999`, click `Browse` and you can see the files HBase stores
on Alluxio, including data and WALs:

![HBaseRootDirectoryOnAlluxio]({{ site.baseurl }}{% link img/screenshot_start_hbase_alluxio_webui.png %})

Create a text file `simple_test.txt` and write these commands into it:

```
create 'test', 'cf'
for i in Array(0..9999)
 put 'test', 'row'+i.to_s , 'cf:a', 'value'+i.to_s
end
list 'test'
scan 'test', {LIMIT => 10, STARTROW => 'row1'}
get 'test', 'row1'
```

Run the following command from the top level HBase project directory:

```bash
bin/hbase shell simple_test.txt
```

You should see some output like this:

![HBaseShellOutput]({{ site.baseurl }}{% link img/screenshot_hbase_shell_output.png %})

If you have Hadoop installed, you can run a Hadoop-utility program in HBase shell to
count the rows of the newly created table:

```bash
bin/hbase org.apache.hadoop.hbase.mapreduce.RowCounter test
```

After this mapreduce job finishes, you can see a result like this:

![HBaseHadoopOutput]({{ site.baseurl }}{% link img/screenshot_hbase_hadoop_output.png %})

## Advanced Setup

### Alluxio in HA mode

When Alluxio is running in fault tolerant mode, change the `hbase.rootdir` property in `conf/hbase-site.xml`
to include Zookeeper information.

```xml
<property>
  <name>hbase.rootdir</name>
  <value>alluxio://zk@zookeeper_hostname1:2181,zookeeper_hostname2:2181,zookeeper_hostname3:2181/hbase</value>
</property>
```

### Add additional Alluxio site properties to HBase

If there are any Alluxio site properties you want to specify for HBase, add those to `hbase-site.xml`. For example,
change `alluxio.user.file.writetype.default` from default `MUST_CACHE` to `CACHE_THROUGH`:

```xml
<property>
  <name>alluxio.user.file.writetype.default</name>
  <value>CACHE_THROUGH</value>
</property>
```

### Alternative way to distribute the Alluxio Client jar

Instead of specifying the location of the jar file in the `$HBASE_CLASSPATH` environment variable,
users could copy the `{{site.ALLUXIO_CLIENT_JAR_PATH}}` file into the `lib` directory of HBase
(make sure it's available on all cluster nodes).

## Troubleshooting

### Logging Configuration

In order to change the logging configuration for HBase, you can [modify your installation's
`log4j.properties` file.](http://hbase.apache.org/0.94/book/trouble.client.html#trouble.client.scarylogs)

