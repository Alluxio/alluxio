---
layout: global
title: Running Presto with Alluxio
nickname: Presto
group: Compute Integrations
priority: 2
---

[Presto](https://prestodb.io/)
is an open source distributed SQL query engine for running interactive analytic queries
on data at a large scale.
This guide describes how to run queries against Presto with Alluxio as a distributed caching layer,
for any data storage systems that Alluxio supports (AWS S3, HDFS, Azure Blob Store, NFS, and more).
Alluxio allows Presto access data regardless of the data source and transparently cache frequently
accessed data (e.g., tables commonly used) into Alluxio distributed storage.
Co-locating Alluxio workers with Presto workers improves data locality and reduces the I/O access
latency when other storage systems are remote or the network is slow or congested.

* Table of Contents
{:toc}

## Using Presto with the Alluxio Catalog Service

Currently, there are 2 ways to enable Presto to interact with Alluxio:
* Presto interacts with the [Alluxio Catalog Service]({{ '/en/core-services/Catalog.html' | relativize_url }})
* Presto interacts directly with the Hive Metastore (with table definitions updated to use Alluxio paths)

The primary benefits for using Presto with the Alluxio Catalog Service are
- Simpler deployments of Alluxio with Presto (no modifications to the Hive Metastore)
- Enabling schema-aware optimizations (transformations like coalescing and file conversions).

Currently, the catalog service is limited to read-only workloads.

For more details and instructions on how to use the Alluxio Catalog Service with Presto, please
visit the [Alluxio Catalog Service documentation]({{ '/en/core-services/Catalog.html' | relativize_url }}).

The rest of this page discusses the alternative approach of Presto directly interacting with the
Hive Metastore, while IO access is performed through Alluxio.

## Prerequisites

* Setup Java for Java 8 Update 161 or higher (8u161+), 64-bit.
* [Deploy Presto](https://prestodb.io/docs/current/installation/deployment.html).
This guide is tested with PrestoDB 0.247.
* Alluxio has been set up and is running.
* Make sure that the Alluxio client jar is available.
  This Alluxio client jar file can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the tarball
  downloaded from Alluxio [download page](https://www.alluxio.io/download).
* Make sure that Hive Metastore is running to serve metadata information of Hive tables.

## Basic Setup

### Configure Presto to connect to Hive Metastore

Presto gets the database and table metadata information (including file system locations) from
the Hive Metastore, via Presto's Hive connector.
Here is a example Presto configuration file `${PRESTO_HOME}/etc/catalog/hive.properties`,
for a catalog using the Hive connector, where the metastore is located on `localhost`.

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
```

### Distribute the Alluxio client jar to all Presto servers

In order for Presto to be able to communicate with the Alluxio servers, the Alluxio client
jar must be in the classpath of Presto servers.
Put the Alluxio client jar `{{site.ALLUXIO_CLIENT_JAR_PATH}}` into the directory
`${PRESTO_HOME}/plugin/hive-hadoop2/`
(this directory may differ across versions) on all Presto servers. Restart the Presto workers and
coordinator:

```console
$ ${PRESTO_HOME}/bin/launcher restart
```

After completing the basic configuration,
Presto should be able to access data in Alluxio.
To configure more advanced features for Presto (e.g., connect to Alluxio with HA), please
follow the instructions at [Advanced Setup](#advanced-setup).

## Examples: Use Presto to Query Tables on Alluxio

### Start Hive Metastore

Ensure your Hive Metastore service is running. Hive Metastore listens on port `9083` by
default. If it is not running, execute the following command to start the metastore:

```console
$ ${HIVE_HOME}/bin/hive --service metastore
```

### Create a Hive table on Alluxio

Here is an example to create an internal table in Hive backed by files in Alluxio.
You can download a data file (e.g., `ml-100k.zip`) from
[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/).
Unzip this file and upload the file `u.user` into `/ml-100k/` in Alluxio:

```console
$ ./bin/alluxio fs mkdir /ml-100k
$ ./bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio:///ml-100k
```

Create an external Hive table pointing to the Alluxio file location.

```
hive> CREATE TABLE u_user (
  userid INT,
  age INT,
  gender CHAR(1),
  occupation STRING,
  zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'alluxio://master_hostname:port/ml-100k';
```

View the Alluxio WebUI at `http://master_hostname:19999` and you can see the directory and files
that Hive creates:

![HiveTableInAlluxio]({{ '/img/screenshot_presto_table_in_alluxio.png' | relativize_url }})

### Start Presto server

Start your Presto server. Presto server runs on port `8080` by default (configurable with
`http-server.http.port` in `${PRESTO_HOME}/etc/config.properties` ):

```console
$ ${PRESTO_HOME}/bin/launcher run
```

### Query tables using Presto

Follow [Presto CLI instructions](https://prestodb.io/docs/current/installation/cli.html)
to download the `presto-cli-<PRESTO_VERSION>-executable.jar`,
rename it to `presto`, and make it executable with `chmod +x`
(sometimes the executable `presto` exists in `${PRESTO_HOME}/bin/presto` and you can use it
directly).

Run a single query (replace `localhost:8080` with your actual Presto server hostname and port):

```console
$ ./presto --server localhost:8080 --execute "use default; select * from u_user limit 10;" \
  --catalog hive --debug
```

And you can see the query results from console:

![PrestoQueryResult]({{ '/img/screenshot_presto_query_result.png' | relativize_url }})

You can also find some of the Alluxio client log messages in the Presto Server log:

![PrestoQueryLog]({{ '/img/screenshot_presto_query_log.png' | relativize_url }})

## Advanced Setup

### Customize Alluxio User Properties

To configure additional Alluxio properties, you can append the conf path (i.e.
`${ALLUXIO_HOME}/conf`) containing [`alluxio-site.properties`]({{ '/en/operation/Configuration.html' | relativize_url }})
to Presto's JVM config at `etc/jvm.config` under Presto folder. The advantage of this approach is to
have all the Alluxio properties set within the same file of `alluxio-site.properties`.

```bash
...
-Xbootclasspath/a:<path-to-alluxio-conf>
```

Alternatively, add Alluxio properties to the Hadoop configuration files
(`core-site.xml`, `hdfs-site.xml`), and use the Presto property `hive.config.resources` in the
file `${PRESTO_HOME}/etc/catalog/hive.properties` to point to the Hadoop resource locations for
every Presto worker. 

```
hive.config.resources=/<PATH_TO_CONF>/core-site.xml,/<PATH_TO_CONF>/hdfs-site.xml
```

#### Example: connect to Alluxio with HA

If the Alluxio HA cluster uses internal leader election,
set the Alluxio cluster property appropriately in the
`alluxio-site.properties` file which is on the classpath.

```properties
alluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```

Alternatively you can add the property to the Hadoop `core-site.xml` configuration
which is contained by `hive.config.resources`.

```xml
<configuration>
  <property>
    <name>alluxio.master.rpc.addresses</name>
    <value>master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998</value>
  </property>
</configuration>
```

For information about how to connect to Alluxio HA cluster using Zookeeper-based leader election,
please refer to [HA mode client configuration parameters]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url }}#specify-alluxio-service-in-configuration-parameters).

#### Example: change default Alluxio write type

For example, change
`alluxio.user.file.writetype.default` from default `ASYNC_THROUGH` to `CACHE_THROUGH`.

Specify the property in `alluxio-site.properties` and distribute this file to the classpath
of each Presto node:

```properties
alluxio.user.file.writetype.default=CACHE_THROUGH
```

Alternatively, modify `conf/hive-site.xml` to include:

```xml
<property>
  <name>alluxio.user.file.writetype.default</name>
  <value>CACHE_THROUGH</value>
</property>
```

### Increase parallelism

Presto's Hive connector uses the config `hive.max-split-size` to control the parallelism of the
query.
For Alluxio 1.6 or earlier, it is recommended to set this size no less than Alluxio's block
size to avoid the read contention within the same block.
For later Alluxio versions, this is no longer an issue because of Alluxio's async caching abilities.

### Avoid Presto timeouts when reading large files

It is recommended to increase `alluxio.user.streaming.data.timeout` to a bigger value (e.g
`10min`) to avoid a timeout failure when reading large files from remote workers.

## Troubleshooting

### Error message "No FileSystem for scheme: alluxio" on queries

When you see error messages like the following, it is likely that Alluxio client jar is not
on the classpath of the Presto worker. Please follow [instructions](#distribute-the-alluxio-client-jar-to-all-presto-servers)
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
