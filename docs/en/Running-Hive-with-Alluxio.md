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

Hive users can either create
[external tables](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-ExternalTables)
that point to specified locations in Alluxio while keeping the storage of other tables unchanged,
or use Alluxio as the default filesystem to operate on. In the following, we will introduce these
two approaches to use Hive with Alluxio.

## Create External Table Loacted in Alluxio

Hive can create external tables from files stored on Alluxio. The setup is fairly straightforward
and the change is also isolated from other Hive tables. An example use case is to store frequently
used Hive tables in Alluxio for high throughput and low latency by serving these files from memory
storage.

### Configure Hive

Set `HIVE_AUX_JARS_PATH` either in shell or `conf/hive-env.sh`:

```bash
export HIVE_AUX_JARS_PATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HIVE_AUX_JARS_PATH}
```

### Hive cli examples

Here is an example to create an external table in Hive backed by files in Alluxio.
You can download a data file (e.g., `ml-100k.zip`) from
[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/).
Unzip this file and upload the file `u.user` into `ml-100k/` on Alluxio:

```bash
$ bin/alluxio fs mkdir /ml-100k
$ bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio://master_hostname:port//ml-100k
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

## Use Alluxio as Default Filesystem

Apache Hive also allows to use Alluxio through a generic file system interface to replace the
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

### Hive cli examples

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
