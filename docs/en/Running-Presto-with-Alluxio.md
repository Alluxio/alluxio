---
layout: global
title: Running Presto with Alluxio
nickname: Presto
group: Frameworks
priority: 2
---

This guide describes how to run [Presto](https://prestodb.io/) with Alluxio, so
that you can easily use Presto to query Hive tables stored in Alluxio's tiered storage.

# Prerequisites

The prerequisite for this part is that you have [Java](Java-Setup.html). And the Java version must use Java 8 Update 60 or higher (8u60+), 64-bit.
Alluxio cluster should also be set up in accordance to these guides for either
[Local Mode](Running-Alluxio-Locally.html) or [Cluster Mode](Running-Alluxio-on-a-Cluster.html).

Please [Download Presto](https://repo1.maven.org/maven2/com/facebook/presto/presto-server/)(This doc uses presto-0.191). Also, please complete Hive setup using
[Hive On Alluxio](Running-Hive-with-Alluxio.html)

# Configuration

Presto gets the database and table metadata information from Hive Metastore. At the same time,
the file system location of table data is obtained from the table's metadata entries. So you need to configure
[Presto on HDFS](https://prestodb.io/docs/current/installation/deployment.html). In order to access HDFS,
you need to add the Hadoop conf files (core-site.xml,hdfs-site.xml), and use `hive.config.resources` in
file `/<PATH_TO_PRESTO>/etc/catalog/hive.properties` to point to the file's location for every Presto worker.

#### Configure `core-site.xml`

You need to add the following configuration items to the `core-site.xml` configured in `hive.properties`:

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
<property>
  <name>fs.AbstractFileSystem.alluxio.impl</name>
  <value>alluxio.hadoop.AlluxioFileSystem</value>
  <description>The Alluxio AbstractFileSystem (Hadoop 2.x)</description>
</property>
```

To use fault tolerant mode, set the Alluxio cluster properties appropriately in an
`alluxio-site.properties` file which is on the classpath.

```properties
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=[zookeeper_hostname]:2181
```

Alternatively you can add the properties to the Hadoop `core-site.xml` configuration which is then
propagated to Alluxio.

```xml
<configuration>
  <property>
    <name>alluxio.zookeeper.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>alluxio.zookeeper.address</name>
    <value>[zookeeper_hostname]:2181</value>
  </property>
</configuration>
```

#### Configure additional Alluxio properties

Similar to above, add additional Alluxio properties to `core-site.xml` of Hadoop configuration in Hadoop directory on each node.
 For example, change `alluxio.user.file.writetype.default` from default `MUST_CACHE` to `CACHE_THROUGH`:

```xml
<property>
  <name>alluxio.user.file.writetype.default</name>
  <value>CACHE_THROUGH</value>
</property>
```

Alternatively, you can also append the conf path (i.e. `/<PATH_TO_ALLUXIO>/conf`) containing [`alluxio-site.properties`](Configuration-Settings.html) to Presto's JVM config at `etc/jvm.config` under Presto folder. The advantage of this approach is to have all the Alluxio properties set within the same file of `alluxio-site.properties`.

```bash
...
-Xbootclasspath/p:<path-to-alluxio-conf>
```

Also, it's recommended to increase `alluxio.user.network.netty.timeout` to a bigger value (e.g. `10min`) to avoid the timeout
 failure when reading large files from remote worker.

#### Enable `hive.force-local-scheduling`

It is recommended to collocate Presto with Alluxio so that Presto workers can read data locally. An important option to enable in Presto is `hive.force-local-scheduling`, which forces splits to be 
scheduled on the same node as the Alluxio worker serving the split data. By default, `hive.force-local-scheduling` in Presto is set to false, and Presto will not attempt to schedule the work on the same machine as the Alluxio worker node.

#### Increase `hive.max-split-size`

Presto's Hive integration uses the config [`hive.max-split-size`](https://teradata.github.io/presto/docs/141t/connector/hive.html) to control the parallelism of the query. It's recommended to set this size no less than Alluxio's block size to avoid the read contention within the same block.

# Distribute the Alluxio Client Jar

We recommend you to download the tarball from
Alluxio [download page](http://www.alluxio.org/download).
Alternatively, advanced users can choose to compile this client jar from the source code
by following the instructs [here](Building-Alluxio-From-Source.html#compute-framework-support).
The Alluxio client jar can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}`.

Distribute the Alluxio client jar to all worker nodes in Presto:
- You must put Alluxio client jar `{{site.ALLUXIO_CLIENT_JAR_PATH_PRESTO}}` into Presto cluster's worker directory
`$PRESTO_HOME/plugin/hive-hadoop2/`
(For different versions of Hadoop, put the appropriate folder), And restart the process of coordinator and worker.

# Presto cli examples

Configure Alluxio as the default filesystem of Hive by following the instructions [here](Running-Hive-with-Alluxio.html#2-use-alluxio-as-the-default-filesystem).

Create a table in Hive and load a file in local path into Hive:

You can download the data file from  [http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/)

```
hive> CREATE TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

hive> LOAD DATA LOCAL INPATH '<path_to_ml-100k>/u.user'
OVERWRITE INTO TABLE u_user;
```

View Alluxio WebUI at `http://master_hostname:19999` and you can see the directory and file Hive creates:

![HiveTableInAlluxio]({{site.data.img.screenshot_presto_table_in_alluxio}})

Alternatively, you can follow the [instructions](Running-Hive-with-Alluxio.html#create-new-tables-from-files-in-alluxio) to create the tables from existing files in Alluxio.

Next, start your Hive metastore service. Hive metastore listens on port 9083 by default.

```bash
$ /<PATH_TO_HIVE>/bin/hive --service metastore
```

The following is an example of the Presto configuration `/<PATH_TO_PRESTO>/etc/catalog/hive.properties` :

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
hive.config.resources=/<PATH_TO_HADOOP>/etc/hadoop/core-site.xml,/<PATH_TO_HADOOP>/etc/hadoop/hdfs-site.xml
```

Start your Presto server. Presto server runs on port 8080 by default:

```bash
$ /<PATH_TO_PRESTO>/bin/launcher run
```

Download the `presto-cli-<YOUR_PRESTO_VERSION>-executable.jar` from [Presto CLI guidence](https://prestodb.io/docs/current/installation/cli.html) 
and rename it to presto.

Run a single query similar to:

```bash
$ /<PATH_TO_PRESTO>/bin/presto --server localhost:8080 --execute "use default;select * from u_user limit 10;" --catalog hive --debug
```

And you can see the query results from console:

![PrestoQueryResult]({{site.data.img.screenshot_presto_query_result}})

Presto Server log:

![PrestoQueryLog]({{site.data.img.screenshot_presto_query_log}})
