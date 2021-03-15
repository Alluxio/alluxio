---
layout: global
title: Running Trino with Alluxio
nickname: Trino
group: Compute Integrations
priority: 2
---

[Trino](https://trino.io/)
is an open source distributed SQL query engine for running interactive analytic queries
on data at a large scale.
This guide describes how to run queries against Trino with Alluxio as a distributed caching layer,
for any data storage systems that Alluxio supports (AWS S3, HDFS, Azure Blob Store, NFS, and more).
Alluxio allows Trino to access data regardless of the data source and transparently cache frequently
accessed data (e.g., tables commonly used) into Alluxio distributed storage.
Co-locating Alluxio workers with Trino workers improves data locality and reduces the I/O access
latency when other storage systems are remote or the network is slow or congested.

* Table of Contents
{:toc}

## Using Trino with the Alluxio Catalog Service

Currently, there are 2 ways to enable Trino to interact with Alluxio:
* Trino interacts with the [Alluxio Catalog Service]({{ '/en/core-services/Catalog.html' | relativize_url }})
* Trino interacts directly with the Hive Metastore (with table definitions updated to use Alluxio paths)

The primary benefits for using Trino with the Alluxio Catalog Service are
- Simpler deployments of Alluxio with Trino (no modifications to the Hive Metastore)
- Enabling schema-aware optimizations (transformations like coalescing and file conversions).

Currently, the catalog service is limited to read-only workloads.

For more details and instructions on how to use the Alluxio Catalog Service with Trino, please
visit the [Alluxio Catalog Service documentation]({{ '/en/core-services/Catalog.html' | relativize_url }}).

The rest of this page discusses the alternative approach of Trino directly interacting with the
Hive Metastore, while IO access is performed through Alluxio.

## Prerequisites

* Setup Java for Java 11, at least version 11.0.7, 64-bit，as required by Trino
* Setup Python version 2.6.x, 2.7.x, or 3.x, as required by Trino
* [Deploy Trino](https://trino.io/docs/current/installation/deployment.html).
This guide is tested with `Trino-352`.
* Alluxio has been set up and is running.
* Make sure that the Alluxio client jar is available.
  This Alluxio client jar file can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the tarball
  downloaded from Alluxio [download page](https://www.alluxio.io/download).
* Make sure that Hive Metastore is running to serve metadata information of Hive tables.

## Basic Setup

### Configure Trino to connect to Hive Metastore

Trino gets the database and table metadata information (including file system locations) from
the Hive Metastore, via Trino's Hive connector.
Here is a example Trino configuration file `${Trino_HOME}/etc/catalog/hive.properties`,
for a catalog using the Hive connector, where the metastore is located on `localhost`.

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
```

### Distribute the Alluxio client jar to all Trino servers

In order for Trino to be able to communicate with the Alluxio servers, the Alluxio client
jar must be in the classpath of Trino servers.
Put the Alluxio client jar `{{site.ALLUXIO_CLIENT_JAR_PATH}}` into the directory
`${Trino_HOME}/plugin/hive-hadoop2/`
(this directory may differ across versions) on all Trino servers. Restart the Trino workers and
coordinator:

```console
$ ${Trino_HOME}/bin/launcher restart
```

After completing the basic configuration,
Trino should be able to access data in Alluxio.
To configure more advanced features for Trino (e.g., connect to Alluxio with HA), please
follow the instructions at [Advanced Setup](#advanced-setup).

## Examples: Use Trino to Query Tables on Alluxio

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

You can see the directory and files that Hive creates by viewing the Alluxio WebUI at `http://master_hostname:19999` 

### Start Hive Metastore

Ensure your Hive Metastore service is running. Hive Metastore listens on port `9083` by
default. If it is not running, execute the following command to start the metastore:

```console
$ ${HIVE_HOME}/bin/hive --service metastore
```

### Start Trino server

Start your Trino server. Trino server runs on port `8080` by default (configurable with
`http-server.http.port` in `${Trino_HOME}/etc/config.properties` ):

```console
$ ${Trino_HOME}/bin/launcher run
```

### Query tables using Trino

Follow [Trino CLI instructions](https://trino.io/docs/current/installation/cli.html)
to download the `trino-cli-<Trino_VERSION>-executable.jar`,
rename it to `trino`, and make it executable with `chmod +x`
(sometimes the executable `trino` exists in `${trino_HOME}/bin/trino` and you can use it
directly).

Run a single query (replace `localhost:8080` with your actual Trino server hostname and port):

```console
$ ./trino --server localhost:8080 --execute "use default; select * from u_user limit 10;" \
  --catalog hive --debug
```

## Advanced Setup

### Customize Alluxio User Properties

To configure additional Alluxio properties, you can append the conf path (i.e.
`${ALLUXIO_HOME}/conf`) containing [`alluxio-site.properties`]({{ '/en/operation/Configuration.html' | relativize_url }})
to Trino's JVM config at `etc/jvm.config` under Trino folder. The advantage of this approach is to
have all the Alluxio properties set within the same file of `alluxio-site.properties`.

```bash
...
-Xbootclasspath/a:<path-to-alluxio-conf>
```

Alternatively, add Alluxio properties to the Hadoop configuration files
(`core-site.xml`, `hdfs-site.xml`), and use the Trino property `hive.config.resources` in the
file `${Trino_HOME}/etc/catalog/hive.properties` to point to the Hadoop resource locations for
every Trino worker. 

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

One can specify the property in `alluxio-site.properties` and distribute this file to the classpath
of each Trino node:

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

Trino's Hive connector uses the config `hive.max-split-size` to control the parallelism of the
query.
For Alluxio 1.6 or earlier, it is recommended to set this size no less than Alluxio's block
size to avoid the read contention within the same block.
For later Alluxio versions, this is no longer an issue because of Alluxio's async caching abilities.

### Avoid Trino timeouts when reading large files

It is recommended to increase `alluxio.user.streaming.data.timeout` to a bigger value (e.g
`10min`) to avoid a timeout failure when reading large files from remote workers.

