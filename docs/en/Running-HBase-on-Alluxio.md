---
layout: global
title: Running Apache HBase on Alluxio
nickname: Apache HBase
group: Frameworks
priority: 2
---

* Table of Contents
{:toc}

This guide describes how to run [Apache HBase](http://hbase.apache.org/), so
that you can easily store HBase tables into Alluxio at various storage levels.

## Prerequisites

The prerequisite for this part is that you have
[Java](Java-Setup.html). Alluxio cluster should also be
set up in accordance to these guides for either [Local Mode](Running-Alluxio-Locally.html) or
[Cluster Mode](Running-Alluxio-on-a-Cluster.html).

Please follow the guides for setting up HBase on
[Apache HBase Configuration](https://hbase.apache.org/book.html#configuration).

## Configuration

Apache HBase allows you to use Alluxio through a generic file system wrapper for the Hadoop file system.
Therefore, the configuration of Alluxio is done mostly in HBase configuration files.

### Set property in `hbase-site.xml`

You need to add the following three properties to `hbase-site.xml` in your HBase installation `conf` directory
(make sure these properties are configured in all HBase cluster nodes):

Tips:You do not need to create the /hbase directory in Alluxio, HBase will do this for you.

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
<property>
  <name>fs.AbstractFileSystem.alluxio.impl</name>
  <value>alluxio.hadoop.AlluxioFileSystem</value>
</property>
<property>
  <name>hbase.rootdir</name>
  <value>alluxio://<hostname>:<port>/hbase</value>
</property>
```

## Distribute the Alluxio Client jar

We need to make the Alluxio client `jar` file available to HBase, because it contains the configured
`alluxio.hadoop.FileSystem` class.

There are two ways to achieve that:

- Put the `alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` file into the
  `lib` directory of HBase.
- Specify the location of the jar file in the `$HBASE_CLASSPATH` environment variable (make sure it's available
on all cluster nodes). For example:

```bash
export HBASE_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HBASE_CLASSPATH}
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

## Using Alluxio with HBase

Start HBase:

```bash
$ ${HBASE_HOME}/bin/start-hbase.sh
```

Visit HBase Web UI at `http://<hostname>:16010` to confirm that HBase is running on Alluxio
(check the `HBase Root Directory` attribute):

![HBaseRootDirectory]({{site.data.img.screenshot_start_hbase_webui}})

And visit Alluxio Web UI at `http://<hostname>:19999`, click `Browse` and you can see the files HBase stores
on Alluxio, including data and WALs:

![HBaseRootDirectoryOnAlluxio]({{site.data.img.screenshot_start_hbase_alluxio_webui}})

## HBase shell examples

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

![HBaseShellOutput]({{site.data.img.screenshot_hbase_shell_output}})

If you have Hadoop installed, you can run a Hadoop-utility program in HBase shell to
count the rows of the newly created table:

```bash
bin/hbase org.apache.hadoop.hbase.mapreduce.RowCounter test
```

After this mapreduce job finishes, you can see a result like this:

![HBaseHadoopOutput]({{site.data.img.screenshot_hbase_hadoop_output}})
