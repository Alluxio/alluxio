---
layout: global
title: Running Facebook Presto with Alluxio
nickname: Presto
group: Frameworks
priority: 2
---

This guide describes how to run [Facebook Presto](https://prestodb.io/) with Alluxio, so
that you can easily use presto to query Hive tables in Alluxio's tiered storage.

# Prerequisites

The prerequisite for this part is that you have [Java](Java-Setup.html). And the Java version must greater then
1.8. Alluxio cluster should also be set up in accordance to these guides for either
[Local Mode](Running-Alluxio-Locally.html) or [Cluster Mode](Running-Alluxio-on-a-Cluster.html).

Please[Download Presto](https://repo1.maven.org/maven2/com/facebook/presto/presto-server/)。And have finished
[Hive On Alluxio](http://www.alluxio.org/docs/master/cn/Running-Hive-with-Alluxio.html)

# Configuration

Presto get the database and table information by connecting Hive Metastore，At the same time,
the HDFS location of table is obtained by the table's metadata. So you need to configure
[Presto on Hdfs](https://prestodb.io/docs/current/installation/deployment.html), In order to access hdfs，
you need to add the hadoop conf file(core-site.xml、hdfs-site.xml) Add to Presto，And use the `hive.config.resources`
point to the file's location.

#### Configure `core-site.xml`

You need to add the following configuration items to the `core-site.xml` in your Presto directory:

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
If your Alluxio is HA, another configuration need to added:
```xml
<property>
  <name>fs.alluxio-ft.impl</name>
  <value>alluxio.hadoop.FaultTolerantFileSystem</value>
  <description>The Alluxio FileSystem (Hadoop 1.x and 2.x) with fault tolerant support</description>
</property>
```

# Alluxio client jar issue

Distribute the Alluxio client Jar packet to all worker nodes in Presto:

- You must put Alluxio client jar package `alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`
(in `/<PATH_TO_ALLUXIO>/core/client/target/` directory) into Presto cluster's worker directory `$PRESTO_HOME/plugin/hadoop/`
(For different versions of Hadoop, put the appropriate folder), And restart the process of coordinator and worker.




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

```
/home/path/presto/presto-cli-0.159-executable.jar --server masterIp:prestoPort --execute "use default;select * from u_user limit 10;" --user username --debug
```

Using a single query:

```
hive> select * from u_user;
```

And you can see the query results from console:

![PrestoQueryResult]({{site.data.img.screenshot_presto_query_result}})