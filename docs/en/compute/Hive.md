---
layout: global
title: Running Apache Hive with Alluxio
nickname: Apache Hive
group: Compute
priority: 2
---

This guide describes how to run [Apache Hive](http://hive.apache.org/) with Alluxio, so
that you can easily store Hive tables in Alluxio's tiered storage.

* Table of Contents
{:toc}

## Prerequisites

* Setup Java for Java 8 Update 60 or higher (8u60+), 64-bit.
* [Download and setup Hive](https://cwiki.apache.org/confluence/display/Hive/GettingStarted).
* Alluxio has been set up and is running.
* Make sure that the Alluxio client jar is available.
  This Alluxio client jar file can be found at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` in the tarball
  downloaded from Alluxio [download page](http://www.alluxio.org/download).
  Alternatively, advanced users can compile this client jar from the source code
  by following the [instructions]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}).
* To run Hive on Hadoop MapReduce, please also follow the instructions in
  [running MapReduce on Alluxio]({{ '/en/compute/Hadoop-MapReduce.html' | relativize_url }})
  to make sure Hadoop MapReduce can work with Alluxio.
  In the following sections of this documentation, Hive is running on Hadoop MapReduce.

## Basic Setup

Distribute Alluxio client jar on all Hive nodes and include the Alluxio client jar to Hive
classpath so Hive can query and access data on Alluxio.
Set `HIVE_AUX_JARS_PATH` in `conf/hive-env.sh`:

```bash
export HIVE_AUX_JARS_PATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HIVE_AUX_JARS_PATH}
```

## Example: Create New Hive Tables in Alluxio

This section talks about how to use
Hive to create new either [internal (managed) or external](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-ManagedandExternalTables)
tables from files stored on Alluxio. In this way,
Alluxio is used as one of the filesystems to store Hive tables similar to HDFS.

The advantage of this setup is that it is fairly straightforward
and each Hive table is isolated from other tables. One typical use case is to store frequently
used Hive tables in Alluxio for high throughput and low latency by serving these files from memory
storage.

> Tips：All the following Hive CLI examples are also applicable to Hive Beeline. You can try these commands out in Beeline shell.

### Prepare Data in Alluxio

Here is an example to create a table in Hive backed by files in Alluxio.
You can download a data file (e.g., `ml-100k.zip`) from
[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/).
Unzip this file and upload the file `u.user` into `ml-100k/` on Alluxio:

```bash
$ bin/alluxio fs mkdir /ml-100k
$ bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio://master_hostname:port/ml-100k
```

View Alluxio WebUI at `http://master_hostname:port` and you can see the directory and file Hive
creates:

![HiveTableInAlluxio]({{ '/img/screenshot_hive_table_in_alluxio.png' | relativize_url }})

### Create a New Internal Table

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

### Create a New External Table

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

### Query the Table

Now you can query the created table. For example:

```
hive> select * from u_user;
```

And you can see the query results from console:

![HiveQueryResult]({{ '/img/screenshot_hive_query_result.png' | relativize_url }})

## Example: Serve Existing Tables Stored in HDFS from Alluxio

When Hive is already serving and managing the tables stored in HDFS,
Alluxio can also serve them for Hive if HDFS is mounted as the under storage of Alluxio.
In this example, we assume a HDFS cluster is mounted as the under storage of
Alluxio root directory (i.e., property `alluxio.underfs.address=hdfs://namenode:port/`
is set in `conf/alluxio-site.properties`). Please refer to
[unified namespace]({{ '/en/advanced/Namespace-Management.html' | relativize_url }})
for more details about Alluxio `mount` operation.

### Move an Internal Table from HDFS to Alluxio

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

### Move an External Table from HDFS to Alluxio

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

### Move an Alluxio Table Back to HDFS

In both cases above about changing table data location to Alluxio, you can also change the table
location back to HDFS:

```
hive> alter table TABLE_NAME set location "hdfs://namenode:port/table/path/in/HDFS";
```

Instructions and examples till here illustrate how to use Alluxio as one of the filesystems to store
tables in Hive, together with other filesystems like HDFS. They do not require to change the global
setting in Hive such as the default filesystem which is covered in the next section.

## Advanced Setup

### Customize Alluxio User Properties

There are two ways to specify any Alluxio client properties for Hive queries when
connecting to Alluxio service:

- Specify the Alluxio client properties in `alluxio-site.properties` and
ensure that this file is on the classpath of Hive service on each node.
- Add the Alluxio site properties to `conf/hive-site.xml` configuration file on each node.

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

### Connect to Alluxio with HA

> Tips：after Alluxio 1.8 (exclusive), there is an easier way to configure Hive to connect to Alluxio
 in fault tolerant mode with Zookeeper. Please follow the instructions in [HDFS API to connect to
 Alluxio with high availability]({{ '/en/deploy/Running-Alluxio-On-a-Cluster.html' | relativize_url }}#Configure-Alluxio-Clients-for-HA).

If you are running Alluxio in fault tolerant mode with a Zookeeper service running at
`zkHost1:2181`, `zkHost2:2181` and `zkHost3:2181`, it requires setting
set the Alluxio properties `alluxio.zookeeper.enabled` and `alluxio.zookeeper.address`

One approach is to set in `alluxio-site.properties`.
Ensure that this file is on the classpath of Hive.

```properties
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=zkHost1:2181,zkHost2:2181,zkHost3:2181
```

Alternatively one can add the properties to the Hive `conf/hive-site.xml`:

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

### Experimental: Use Alluxio as the Default Filesystem

This section talks about how to use Alluxio as the default file system for Hive.
Apache Hive can also use Alluxio through a generic file system interface to replace the
Hadoop file system. In this way, the Hive uses Alluxio as the default file system and its internal
metadata and intermediate results will be stored in Alluxio by default.

#### Configure Hive

Add the following property to `hive-site.xml` in your Hive installation `conf` directory

```xml
<property>
   <name>fs.defaultFS</name>
   <value>alluxio://master_hostname:port</value>
</property>
```

#### Using Alluxio with Hive

Create Directories in Alluxio for Hive:

```bash
$ ./bin/alluxio fs mkdir /tmp
$ ./bin/alluxio fs mkdir /user/hive/warehouse
$ ./bin/alluxio fs chmod 775 /tmp
$ ./bin/alluxio fs chmod 775 /user/hive/warehouse
```

Then you can follow the
[Hive documentation](https://cwiki.apache.org/confluence/display/Hive/GettingStarted) to use Hive.

#### Example: Create a Table

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

![HiveTableInAlluxio]({{ '/img/screenshot_hive_table_in_alluxio.png' | relativize_url }})

Using a single query:

```
hive> select * from u_user;
```

And you can see the query results from console:

![HiveQueryResult]({{ '/img/screenshot_hive_query_result.png' | relativize_url }})

## Troubleshooting

### Check Hive is Configured Correctly

Before running Hive on Alluxio, you might want to make sure that your configuration has been
setup correctly set up with Alluxio. The Hive integration checker can help you achieve this (Hive
 2.x required).

You can run the following command in the Alluxio project directory:

```bash
$ integration/checker/bin/alluxio-checker.sh hive -hiveurl [HIVE_URL]
```

You can use `-h` to display helpful information about the command.
This command will report potential problems that might prevent you from running Hive on Alluxio.

### Logging Configuration

If you wish to modify how your Hive client logs information, see the detailed page within the Hive
documentation that
[explains how to configure logging for your Hive application][https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-HiveLoggingErrorLogsHiveLogs]
