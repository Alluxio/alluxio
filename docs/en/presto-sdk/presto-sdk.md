---
layout: global
title: Presto SDK with Local Cache
---

Presto provides an SDK way to combined with Alluxio.
With the SDK, hot data that need to be scanned frequently
can be cached locally on Presto Workers that execute the TableScan operator.

## Prerequisites
- Setup Java for Java 8 Update 161 or higher (8u161+), 64-bit.
- [Deploy Presto](https://prestodb.io/docs/current/installation/deployment.html).
- Alluxio has been set up and is running following [the deployment guide here](https://github.com/Alluxio/alluxio/blob/dora/docs/en/Deploy-Alluxio-Cluster.md).
- Make sure that the Alluxio client jar that provides the SDK is available. This Alluxio client jar file can be found at `/<PATH_TO_ALLUXIO>/client/alluxio-${VERSION}-client.jar` in the tarball downloaded from Alluxio download page.
- Make sure that Hive Metastore is running to serve metadata information of Hive tables. The default port of Hive Metastore is `9083`. Executing `lsof -i:9083` can check whether the Hive Metastore process exists or not.

## Basic Setup
### Configure Presto to connect to Hive Metastore
Presto gets the database and table metadata information (including file system locations) from the Hive Metastore, via Presto's Hive Connector.
Here is an example Presto configuration file `${PRESTO_HOME}/etc/hive.properties`, for a catalog using the Hive connector,
where the metastore is located on `localhost`.
```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
```

### Enable local caching for Presto
To enable local caching, add the following configurations in `${PRESTO_HOME}/etc/hive.properties`:
```properties
hive.node-selection-strategy=SOFT_AFFINITY
cache.enabled=true
cache.type=ALLUXIO
cache.base-directory=file:///tmp/alluxio
cache.alluxio.max-cache-size=100MB
```
Here `cache.enabled=true` and `cache.type=ALLUXIO` are to enable the local caching feature in Presto.
`cache.base-directory` is used for specifying the path for local caching.
`cache.alluxio.max-cache-size` is to allocate the space for local caching.

### Distribute the Alluxio client jar to all Presto servers
As Presto communicates with Alluxio servers by the SDK provided in the Alluxio client jar, the Alluxio client jar must be
in the classpath of Presto servers. Put the Alluxio client jar `/<PATH_TO_ALLUXIO>/client/alluxio-2.9.1-client.jar`
into the directory `${PRESTO_HOME}/plugin/hive-hadoop2/` (this directory may differ across versions) on all Presto servers.
Restart the Presto workers and coordinator
```shell
$ ${PRESTO_HOME}/bin/launcher restart
```
After completing the basic configuration, Presto should be able to access data in Alluxio.

## Example
### Create a Hive
Create a Hive table by hive client specifying its `LOCATION` to Alluxio.
```sql
hive> CREATE TABlE employee_parquet_alluxio (name string, salary int)
PARTITIONED BY (doj string)
STORED AS PARQUET
LOCATION 'alluxio://Master01:19998/alluxio/employee_parquet_alluxio';
```
Replace `Master01:19998` to your Alluxio Master address. Note that we set `STORED AS PARQUET` here since currently only parquet and orc format is supported in Presto Local Caching.

### Insert data
Insert some data into the created Hive table for testing.
```sql
INSERT INTO employee_parquet_alluxio select 'jack', 15000, '2023-02-26';
INSERT INTO employee_parquet_alluxio select 'make', 25000, '2023-02-25';
INSERT INTO employee_parquet_alluxio select 'amy', 20000, '2023-02-26';
```

### Query table using Presto
Follow [Presto CLI instructions](https://prestodb.io/docs/current/installation/cli.html) to download the` presto-cli-<PRESTO_VERSION>-executable.jar`,  
rename it to `presto-cli`, and make it executable with `chmod + x`. Run a single query with `presto-cli` to select the data from the table.
```sql
presto> SELECT * FROM employee_parquet_alluxio;
```
You can see that data are cached in the directory specified in `/etc/catalog/hive.properties`. In our example, we should see the files are cached in `/tmp/alluxio/LOCAL`.

## Advanced Setup
### Monitor metrics about local caching
In order to expose the metrics of local caching, follow the steps below:
- **Step 1**: Add `-Dalluxio.metrics.conf.file=<ALLUXIO_HOME>/conf/metrics.properties` to specify the metrics configuration for the SDK used by Presto.
- **Step 2**: Add `sink.jmx.class=alluxio.metrics.sink.JmxSink` to `<ALLUXIO_HOME>/conf/metrics.properties` to expose the metrics.
- **Step 3**: Add `cache.alluxio.metrics-enabled=true` in `<PRESTO_HOME>/etc/catalog.hive.properties` to enable metric collection.
- **Step 4**: Restart the Presto process by executing `<PRESTO_HOME>/bin/laucher restart`.
- **Step 5**: Metrics about local caching should be seen in JMX if we access Presto's JMX RESTful API `<PRESTO_NODE_HOST_NAME>:<PRESTO_PORT>/v1/jmx`.

The following metrics would be useful for tracking local caching:
<table class="table table-striped">
    <tr>
        <th>Metric Name</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheBytesReadCache`</td>
        <td>METER</td>
        <td>Bytes read from client.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when putting cached data in the client cache.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutInsufficientSpaceErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when putting cached data in the client cache due to insufficient space made after eviction.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutNotReadyErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when cache is not ready to add pages.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutBenignRacingErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when adding pages due to racing eviction. This error is benign.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutStoreWriteErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when putting cached data in the client cache due to failed writes to page store.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutEvictionErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when putting cached data in the client cache due to failed eviction.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePutStoreDeleteErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when putting cached data in the client cache due to failed deletes in page store.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheGetErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when getting cached data in the client cache.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheGetNotReadyErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when cache is not ready to get pages.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheGetStoreReadErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when getting cached data in the client cache due to failed read from page stores.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheDeleteNonExistingPageErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when deleting pages due to absence.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheDeleteNotReadyErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when cache is not ready to delete pages.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheDeleteFromStoreErrors`</td>
        <td>COUNTER</td>
        <td>Number of failures when deleting pages from page stores.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheHitRate`</td>
        <td>GAUGE</td>
        <td>Cache hit rate: (# bytes read from cache) / (# bytes requested).</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CachePagesEvicted`</td>
        <td>METER</td>
        <td>Total number of pages evicted from the client cache.</td>
    </tr>
    <tr>
        <td markdown="span">`Client.CacheBytesEvicted`</td>
        <td>METER</td>
        <td>Total number of bytes evicted from the client cache.</td>
    </tr>
</table>
