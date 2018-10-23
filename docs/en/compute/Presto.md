---
layout: global
title: Running Presto with Alluxio
nickname: Presto
group: Compute
priority: 2
---

[Presto](https://prestodb.io/)
is an open source distributed SQL query engine for running interactive analytic queries
on data at a large scale.
This guide describes how to run Presto to query Alluxio as a distributed cache layer,
where the data sources can be AWS S3, Azure blob store, HDFS and many others.
With this setup, Alluxio will help Presto access data regardless of the data source and
transparently cache the data frequently accessed (e.g., tables commonly used) into Alluxio
distributed storage.
Co-locating Alluxio workers with Presto workers can benefit data locality and reduce the I/O access
latency especially when data is remote or network is slow or congested.

* Table of Contents
{:toc}

## Prerequisites

* Setup Java for Java 8 Update 60 or higher (8u60+), 64-bit.
* [Deploy Presto](https://prestodb.io/docs/current/installation/deployment.html).
This guide is tested with `presto-0.208`.
* Alluxio has been set up and is running.
* Make sure that the Alluxio client jar is available.
  This Alluxio client jar file can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the tarball
  downloaded from Alluxio [download page](http://www.alluxio.org/download).
* Make sure that Hive metastore is running to serve metadata information of Hive tables.

## Basic Setup

### Configure Presto to connect to Hive Metastore

Presto gets the database and table metadata information, as well as
the file system location of table data from Hive Metastore.
Edit the Presto configuration `${PRESTO_HOME}/etc/catalog/hive.properties`:

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
```

### Distribute the Alluxio client jar to all Presto servers

Put Alluxio client jar `{{site.ALLUXIO_CLIENT_JAR_PATH}}` into directory
`${PRESTO_HOME}/plugin/hive-hadoop2/`
(this directory may differ across versions) on all Presto servers. Restart Presto service:

```bash
$ ${PRESTO_HOME}/bin/launcher restart
```

After completing the basic configuration,
Presto should be able to access data in Alluxio.
To configure more advanced features for Presto (e.g., connect to Alluxio with HA), please
follow the instructions at [Advanced Setup](#advanced-setup).

## Examples: Use Presto to Query Tables on Alluxio

### Create a Hive table on Alluxio

Here is an example to create an internal table in Hive backed by files in Alluxio.
You can download a data file (e.g., `ml-100k.zip`) from
[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/).
Unzip this file and upload the file `u.user` into `/ml-100k/` on Alluxio:

```bash
$ bin/alluxio fs mkdir /ml-100k
$ bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio:///ml-100k
```

Create an external Hive table from existing files in Alluxio.

```
hive> CREATE TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION 'alluxio://master_hostname:port/ml-100k';
```

View Alluxio WebUI at `http://master_hostname:19999` and you can see the directory and file Hive creates:

![HiveTableInAlluxio]({{ site.baseurl }}{% link img/screenshot_presto_table_in_alluxio.png %})

### Start Hive metastore

Next, ensure your Hive metastore service is running. Hive metastore listens on port `9083` by
default. If it is not running,

```bash
$ ${HIVE_HOME}/bin/hive --service metastore
```

### Start Presto server

Start your Presto server. Presto server runs on port `8080` by default (set by
`http-server.http.port` in `${PRESTO_HOME}/etc/config.properties` ):

```bash
$ ${PRESTO_HOME}/bin/launcher run
```

### Query tables using Presto

Follow [Presto CLI guidence](https://prestodb.io/docs/current/installation/cli.html) to download the `presto-cli-<PRESTO_VERSION>-executable.jar`,
rename it to `presto`, and make it executable with `chmod +x`
(sometimes the executable `presto` exists in `${PRESTO_HOME}/bin/presto` and you can use it
directly).

Run a single query (replace `localhost:8080` with your actual Presto server hostname and port):

```bash
$ ./presto --server localhost:8080 --execute "use default;select * from u_user limit 10;" --catalog hive --debug
```

And you can see the query results from console:

![PrestoQueryResult]({{ site.baseurl }}{% link img/screenshot_presto_query_result.png %})

Presto Server log:

![PrestoQueryLog]({{ site.baseurl }}{% link img/screenshot_presto_query_log.png %})

## Advanced Setup

### Customize Alluxio User Properties

To configure additional Alluxio properties, you can append the conf path (i.e.
`${ALLUXIO_HOME}/conf`) containing [`alluxio-site.properties`]({{ '/en/basic/Configuration-Settings.html' | relativize_url }})
to Presto's JVM config at `etc/jvm.config` under Presto folder. The advantage of this approach is to
have all the Alluxio properties set within the same file of `alluxio-site.properties`.

```bash
...
-Xbootclasspath/p:<path-to-alluxio-conf>
```

Alternatively, one can add them to the Hadoop conf files
(`core-site.xml`, `hdfs-site.xml`), and use
Presto property `hive.config.resources` in
file `${PRESTO_HOME}/etc/catalog/hive.properties` to point to the file's location for every Presto
worker.

```
hive.config.resources=/<PATH_TO_CONF>/core-site.xml,/<PATH_TO_CONF>/hdfs-site.xml
```

#### Example: connect to Alluxio with HA

To use Alluxio in fault tolerant mode, set the Alluxio cluster properties appropriately in an
`alluxio-site.properties` file which is on the classpath.

```properties
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=zkHost1:2181,zkHost2:2181,zkHost3:2181
```

Alternatively you can add the properties to the Hadoop `core-site.xml` configuration
which is contained by `hive.config.resources`.

```xml
<configuration>
  <property>
    <name>alluxio.zookeeper.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>alluxio.zookeeper.address</name>
    <value>zkHost1:2181,zkHost2:2181,zkHost3:2181</value>
  </property>
</configuration>
```

#### Example: change default Alluxio write type

For example, change
`alluxio.user.file.writetype.default` from default `MUST_CACHE` to `CACHE_THROUGH`.

One can specify the property in `alluxio-site.properties` and distribute this file to the classpath
of each Hive node:

```properties
alluxio.user.file.writetype.default=CACHE_THROUGH
```

Alternatively, modify `conf/hive-site.xml` to have:

```xml
<property>
  <name>alluxio.user.file.writetype.default</name>
  <value>CACHE_THROUGH</value>
</property>
```

### Enable data locality

It is recommended to co-locate Presto workers with Alluxio workers so that Presto workers can read data locally. An important option to enable in Presto is `hive.force-local-scheduling`, which forces splits to be
scheduled on the same node as the Alluxio worker serving the split data. By default, `hive.force-local-scheduling` in Presto is set to `false`, and Presto will not attempt to schedule the work on the same machine as the Alluxio worker node.

### Increase parallelism

Presto's Hive integration uses the config [`hive.max-split-size`](https://teradata.github.io/presto/docs/141t/connector/hive.html) to control the parallelism of the query.
For Alluxio 1.6 or earlier,
it is recommended to set this size no less than Alluxio's block size to avoid the read contention within the same block. For later Alluxio versions, this is no more an issue due to
async cache on Alluxio workers.

### Avoid Presto timeout reading large files

It is recommended to increase `alluxio.user.network.netty.timeout` to a bigger value (e.g.
`10min`) to avoid the timeout
 failure when reading large files from remote worker.

## Troubleshooting

### Error message "No FileSystem for scheme: alluxio" on queries

When you see error messages like the following, it is likely that Alluxio client jar is not put
into the classpath of Presto worker. Please follow [instructions](#distribute-the-alluxio-client-jar-to-all-presto-servers)
to fix this issue.

```
Query 20180907_063430_00001_cm7xe failed: No FileSystem for scheme: alluxio
com.facebook.presto.spi.PrestoException: No FileSystem for scheme: alluxio
	at com.facebook.presto.hive.BackgroundHiveSplitLoader$HiveSplitLoaderTask.process(BackgroundHiveSplitLoader.java:189)
	at com.facebook.presto.hive.util.ResumableTasks.safeProcessTask(ResumableTasks.java:47)
	at com.facebook.presto.hive.util.ResumableTasks.access$000(ResumableTasks.java:20)
	at com.facebook.presto.hive.util.ResumableTasks$1.run(ResumableTasks.java:35)
	at io.airlift.concurrent.BoundedExecutor.drainQueue(BoundedExecutor.java:78)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
```
