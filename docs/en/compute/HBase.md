---
layout: global
title: Running Apache HBase on Alluxio
nickname: Apache HBase
group: Compute Integrations
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
downloaded from Alluxio [download page](https://www.alluxio.io/download).
Alternatively, advanced users can compile this client jar from the source code
by following the [instructions]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}).
* [Deploy HBase](https://hbase.apache.org/book.html#configuration)
Please follow this guides for setting up HBase.

## Basic Setup

Apache HBase allows you to use Alluxio through a generic file system wrapper for the Hadoop file system.
Therefore, the configuration of Alluxio is done mostly in HBase configuration files.

### Set property in `hbase-site.xml`

Set the following properties in `conf/hbase-site.xml` and make sure all HBase cluster nodes
have the configuration.

Set the `hbase.rootdir` property as follows: 
```xml
<property>
  <name>hbase.rootdir</name>
  <value>alluxio://master_hostname:port/hbase</value>
</property>
```

> You do not need to create the `/hbase` directory in Alluxio, HBase will do this for you.

You also need to add the FS implementation classes to HBase configuration. These classes are provided in Alluxio Client jar.

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
<property>
  <name>fs.AbstractFileSystem.alluxio.impl</name>
  <value>alluxio.hadoop.AlluxioFileSystem</value>
</property>
```

Also add the following property to the same file `hbase-site.xml`:
```xml
<property>
  <name>hbase.regionserver.hlog.syncer.count</name>
  <value>1</value>
</property>
```

> This property is required to prevent HBase from flushing Alluxio file stream in a thread unsafe
way.

If you are running HBase version greater than 2.0, add the following property:

```xml
<property>
  <name>hbase.unsafe.stream.capability.enforce</name>
  <value>false</value>
</property>
```

> This will disable HBase new stream capabilities (hflush/hsync) used for WAL.

### Distribute the Alluxio Client jar

We need to make the Alluxio client jar file available to HBase, because it contains the configured
`alluxio.hadoop.FileSystem` class.

Specify the location of the jar file in the `$HBASE_CLASSPATH` environment variable (make sure it's available
on all cluster nodes). For example:

```console
$ export HBASE_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HBASE_CLASSPATH}
```

Alternative ways are described in the [Advanced Setup]({{ '/en/compute/HBase.html' | relativize_url }}#advanced-setup)

## Example

Ensure alluxio scheme is recognized before starting HBase:

```console
$ ${HBASE_HOME}/bin/start-hbase.sh
```

If not, follow the [Usage FAQs]({{ '/en/operation/Troubleshooting.html' | relativize_url }}#usage-faq)
 as needed.

Visit HBase Web UI at `http://<HBASE_MASTER_HOSTNAME>:16010` to confirm that HBase is running on Alluxio
(check the `HBase Root Directory` attribute):

![HBaseRootDirectory]({{ '/img/screenshot_start_hbase_webui.png' | relativize_url }})

And visit Alluxio Web UI at `http://<ALLUXIO_MASTER_HOSTNAME>:19999`, click `Browse` and you can see the files HBase stores
on Alluxio, including data and WALs:

![HBaseRootDirectoryOnAlluxio]({{ '/img/screenshot_start_hbase_alluxio_webui.png' | relativize_url }})

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

```console
$ bin/hbase shell simple_test.txt
```

You should see some output like this:

![HBaseShellOutput]({{ '/img/screenshot_hbase_shell_output.png' | relativize_url }})

If you have Hadoop installed, you can run a Hadoop-utility program in HBase shell to
count the rows of the newly created table:

```console
$ bin/hbase org.apache.hadoop.hbase.mapreduce.RowCounter test
```

After this mapreduce job finishes, you can see a result like this:

![HBaseHadoopOutput]({{ '/img/screenshot_hbase_hadoop_output.png' | relativize_url }})

## Advanced Setup

### Alluxio in HA mode

When Alluxio is running in HA mode, change the `hbase.rootdir` property in `conf/hbase-site.xml`
to use a HA-style Alluxio authority like `host1:19998,host2:19998,host3:19998`
or `zk@host1:2181,host2:2181,host3:2181`.

```xml
<property>
  <name>hbase.rootdir</name>
  <value>alluxio://master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998/hbase</value>
</property>
```

See [HA authority]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url }}#ha-authority)
for more details.

### Add additional Alluxio site properties to HBase

If there are any Alluxio site properties you want to specify for HBase, add those to `hbase-site.xml`. For example,
change `alluxio.user.file.writetype.default` from default `ASYNC_THROUGH` to `CACHE_THROUGH`:

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

```console
$ cp `{{site.ALLUXIO_CLIENT_JAR_PATH}}` /path/to/hbase-master/lib/
$ cp `{{site.ALLUXIO_CLIENT_JAR_PATH}}` /path/to/current/hbase-client/lib/
$ cp `{{site.ALLUXIO_CLIENT_JAR_PATH}}` /path/to/hbase-regionserver/lib/
```

## Troubleshooting

### Logging Configuration

In order to change the logging configuration for HBase, you can [modify your installation's
`log4j.properties` file.](http://hbase.apache.org/0.94/book/trouble.client.html#trouble.client.scarylogs)

