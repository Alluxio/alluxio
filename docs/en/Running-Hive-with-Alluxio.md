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

## Create Hive Tables on Alluxio

There are different ways to integrate Hive with Alluxio, as storage for
[internal (managed) or external tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-ManagedandExternalTables),
newly created or existing tables. Alluxio can also be used as the default file system
for Hive. In the following sections, we will describe how to use Hive with Alluxio for these use
cases. Hive is running on Hadoop MapReduce in this documentation.
*Tips：All the following Hive CLI examples are also applicable to Hive Beeline. You can try these commands out in Beeline shell.*

### Create New Tables from Files in Alluxio

Hive can create new tables from files stored on Alluxio. The setup is fairly straightforward
and the change is also isolated from other Hive tables. An example use case is to store frequently
used Hive tables in Alluxio for high throughput and low latency by serving these files from memory
storage.

#### Hive CLI examples for New Internal Table

Here is an example to create an internal table in Hive backed by files in Alluxio.
You can download a data file (e.g., `ml-100k.zip`) from
[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/).
Unzip this file and upload the file `u.user` into `ml-100k/` on Alluxio:

```bash
$ bin/alluxio fs mkdir /ml-100k
$ bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio://master_hostname:port/ml-100k
```

Then create a new internal table:

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

#### Hive CLI examples for New External Table

Make the same setup as the previous example, and create a new external table:

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

The difference is that Hive will manage the lifecycle of internal tables.
When you drop an internal table, Hive deletes both the table metadata and the data file from
Alluxio.

### Use Alluxio for Existing Tables Stored in HDFS

When Hive is already serving and managing the tables stored in HDFS,
Alluxio can also serve them for Hive if HDFS is mounted as the under storage of Alluxio.
In this example, we assume a HDFS cluster is mounted as the under storage of
Alluxio root directory (i.e., property `alluxio.underfs.address=hdfs://namenode:port/`
is set in `conf/alluxio-site.properties`). Please refer to
[unified namespace](Unified-and-Transparent-Namespace.html) for more details about mount operation.

#### Hive CLI examples for Existing Internal Table

We assume that the `hive.metastore.warehouse.dir` property is set to `/user/hive/warehouse` which
is the default value, and the internal table is already created like this:

```
hive> CREATE TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

hive> LOAD DATA LOCAL INPATH '/path/to/ml-100k/u.user' OVERWRITE INTO TABLE u_user;
```

The following HiveQL statement will change the table data location from HDFS to Alluxio：

```
hive> alter table u_user set location "alluxio://master_hostname:port/user/hive/warehouse/u_user";
```

Verify whether the table location is set correctly:

```
hive> desc formatted u_user;
```

Note that, accessing files in `alluxio://master_hostname:port/user/hive/warehouse/u_user` for the
first time will be translated to access corresponding files in
`hdfs://namenode:port/user/hive/warehouse/u_user` (the default Hive internal data storage); once
the data is cached in Alluxio, Alluxio will serve them for follow-up queries without loading data
again from HDFS. The entire process is transparent to Hive and users.

#### Hive CLI examples for Existing External Table

Assume there is an existing external table `u_user` in Hive with location set to
`hdfs://namenode_hostname:port/ml-100k`.
You can use the following HiveQL statement to check its "Location" attribute:

```
hive> desc formatted u_user;
```

Then use the following HiveQL statement to change the table data location from HDFS to Alluxio：

```
hive> alter table u_user set location "alluxio://master_hostname:port/ml-100k";
```

### Change Back the Table Metadata to HDFS

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
