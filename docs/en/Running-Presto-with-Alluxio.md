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

Alluxio client will need to be compiled with the Presto specific profile. Build the entire project
from the top level `alluxio` directory with the following command:

```bash
mvn clean package -Ppresto -DskipTests
```

Please [Download Presto](https://repo1.maven.org/maven2/com/facebook/presto/presto-server/)(This doc uses presto-0.170). Also, please complete Hive setup using
[Hive On Alluxio](http://www.alluxio.org/docs/master/en/Running-Hive-with-Alluxio.html)

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

Alternatively, you can also append the path to [`alluxio-site.properties`](Configuration-Settings.html) to Presto's JVM config at `etc/jvm.config` under Presto folder. The advantage of this approach is to have all the Alluxio properties set within the same file of `alluxio-site.properties`.

```bash
...
-Xbootclasspath/p:<path-to-alluxio-site-properties>
```

Also, it's recommended to increase `alluxio.user.network.netty.timeout.ms` to a bigger value (e.g. 10 mins) to avoid the timeout
 failure when reading large files from remote worker.

#### Increase `hive.max-split-size`

Presto's Hive integration uses the config [`hive.max-split-size`](https://teradata.github.io/presto/docs/141t/connector/hive.html) to control the parallelism of the query. It's recommended to set this size no less than Alluxio's block size to avoid the read contention within the same block.

# Distribute the Alluxio Client Jar

Distribute the Alluxio client jar to all worker nodes in Presto:
- You must put Alluxio client jar `{{site.ALLUXIO_CLIENT_JAR_PATH}}` into Presto cluster's worker directory
`$PRESTO_HOME/plugin/hive-hadoop2/`
(For different versions of Hadoop, put the appropriate folder), And restart the process of coordinator and worker.

Alternatively, advanced users can choose to compile this client jar from the source code. Follow the instructs [here](Building-Alluxio-Master-Branch.html#compute-framework-support) and use the generated jar at `{{site.ALLUXIO_CLIENT_JAR_PATH_BUILD}}` for the rest of this guide.

# Presto cli examples

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

Using a single query:
```
/home/path/presto/presto-cli-0.170-executable.jar --server masterIp:prestoPort --execute "use default;select * from u_user limit 10;" --user username --debug
```

And you can see the query results from console:

![PrestoQueryResult]({{site.data.img.screenshot_presto_query_result}})

Presto Server log:

![PrestoQueryLog]({{site.data.img.screenshot_presto_query_log}})
