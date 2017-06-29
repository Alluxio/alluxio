---
layout: global
title: Running Apache Hive with Alluxio
nickname: Apache Hive
group: Frameworks
priority: 2
---

* Table of Contents
{:toc}

This guide describes how to run [Apache Hive](http://hive.apache.org/) with Alluxio, so
that you can easily store Hive tables in Alluxio's tiered storage.

## Prerequisites

The prerequisite for this part is that you have
[Java](Java-Setup.html). Alluxio cluster should also be
set up in accordance to these guides for either [Local Mode](Running-Alluxio-Locally.html) or
[Cluster Mode](Running-Alluxio-on-a-Cluster.html).

Please [Download Hive](http://hive.apache.org/downloads.html).

To run Hive on Hadoop MapReduce, please also follow the instructions in
[running MapReduce on Alluxio](Running-Hadoop-MapReduce-on-Alluxio.html) to make sure Hadoop
MapReduce can run with Alluxio.

## Configure Hive

First of all, set `HIVE_AUX_JARS_PATH` either in shell or `conf/hive-env.sh`:

```bash
export HIVE_AUX_JARS_PATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HIVE_AUX_JARS_PATH}
```
Alluxio can be used as storage for both
[external tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-ExternalTables)
and internal tables. These tables can be new tables that are being created or existing tables that
are stored in HDFS. Alluxio can also be used as the default file system for Hive. In the following
sections, we will describe how to use Hive with Alluxio for these use cases. Hive is running on Hadoop
MapReduce in this documentation.

## Create External Table from Files Located in Alluxio

Hive can create external tables from files stored on Alluxio. The setup is fairly straightforward
and the change is also isolated from other Hive tables. An example use case is to store frequently
used Hive tables in Alluxio for high throughput and low latency by serving these files from memory
storage.

### Hive CLI examples

Here is an example to create an external table in Hive backed by files in Alluxio.
You can download a data file (e.g., `ml-100k.zip`) from
[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/).
Unzip this file and upload the file `u.user` into `ml-100k/` on Alluxio:

```bash
$ bin/alluxio fs mkdir /ml-100k
$ bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio://master_hostname:port/ml-100k
```

Then create an external table:

```
hive> CREATE EXTERNAL TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION 'alluxio://master_hostname:port/ml-100k';
```

## Create Internal Table from Files Located in Alluxio

Hive can create internal tables from files stored on Alluxio and Hive will manage the lifecycle of internal tables.
When you drop an internal table, Hive drops the table metadata and data both.

### Hive CLI examples

Use the data file in `ml-100k.zip` from
[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/) as an example.

```bash
$ bin/alluxio fs mkdir /ml-100k
$ bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio://master_hostname:port/ml-100k
```

Then create an internal table:

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

## Use Alluxio as the FileSystem for External tables currently in HDFS

### Hive CLI examples

Create an external table in HDFS:

```
hive> CREATE EXTERNAL TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION 'hdfs://namenode_hostname:port/ml-100k';
```

We assume that you have set `alluxio.underfs.address=hdfs://namenode:port/` in `alluxio-site.properties`.

Use the following HiveQL to change the table data location：

```
hive> alter table TABLE_NAME set location "alluxio://master_hostname:port/ml-100k";
```

Use the following HiveQL and check the "Location" attribute to verify whether the table location is set correctly:

```
hive> desc formatted u_user;
```

## Use Alluxio as the FileSystem for Internal tables currently in HDFS

### Hive CLI examples

We assume that the **"hive.metastore.warehouse.dir"** property is set as following: 
```xml
<property>
  <name>hive.metastore.warehouse.dir</name>
  <value>/user/hive/warehouse</value>
</property>
```

Create an internal table in HDFS and load data into it:

```
hive> CREATE TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

hive> LOAD DATA LOCAL INPATH '/path/to/ml-100k/u.user'
OVERWRITE INTO TABLE u_user;
```

We assume that you have set `alluxio.underfs.address=hdfs://namenode:port/` in `alluxio-site.properties`.

Use the following HiveQL to change the table data location：

```
hive> alter table TABLE_NAME set location "alluxio://master_hostname:port/user/hive/warehouse/u_user";
```

## Change the table metadata to point back to HDFS

In both cases above about changing table data location to Alluxio, you can also change the table
location back to HDFS:

```
hive> alter table TABLE_NAME set location "hdfs://namenode:port/table/path/in/HDFS";
```

## Use Alluxio as Default Filesystem

Apache Hive can also use Alluxio through a generic file system interface to replace the
Hadoop file system. In this way, the Hive uses Alluxio as the default file system and its internal
metadata and intermediate results will be stored in Alluxio by default.

### Configure Hive

Add the following property to `hive-site.xml` in your Hive installation `conf` directory

```xml
<property>
   <name>fs.defaultFS</name>
   <value>alluxio://master_hostname:port</value>
</property>
```

To use fault tolerant mode, set Alluxio scheme to be `alluxio-ft`:

```xml
<property>
   <name>fs.defaultFS</name>
   <value>alluxio-ft:///</value>
</property>
```


### Add additional Alluxio site properties to Hive

If there are any Alluxio site properties you want to specify for Hive, add those to `core-site.xml`
to Hadoop configuration directory on each node. For example, change
`alluxio.user.file.writetype.default` from default `MUST_CACHE` to `CACHE_THROUGH`:

```xml
<property>
<name>alluxio.user.file.writetype.default</name>
<value>CACHE_THROUGH</value>
</property>
```

### Using Alluxio with Hive

Create Directories in Alluxio for Hive:

```bash
$ ./bin/alluxio fs mkdir /tmp
$ ./bin/alluxio fs mkdir /user/hive/warehouse
$ ./bin/alluxio fs chmod 775 /tmp
$ ./bin/alluxio fs chmod 775 /user/hive/warehouse
```

Then you can follow the
[Hive documentation](https://cwiki.apache.org/confluence/display/Hive/GettingStarted) to use Hive.

### Hive CLI examples

Create a table in Hive and load a file in local path into Hive:

Again use the data file in `ml-100k.zip` from
[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/) as an example.

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

hive> LOAD DATA LOCAL INPATH '/path/to/ml-100k/u.user'
OVERWRITE INTO TABLE u_user;
```

View Alluxio WebUI at `http://master_hostname:port` and you can see the directory and file Hive
creates:

![HiveTableInAlluxio]({{site.data.img.screenshot_hive_table_in_alluxio}})

Using a single query:

```
hive> select * from u_user;
```

And you can see the query results from console:

![HiveQueryResult]({{site.data.img.screenshot_hive_query_result}})
