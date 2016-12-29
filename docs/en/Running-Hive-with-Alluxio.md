---
layout: global
title: Running Apache Hive with Alluxio
nickname: Apache Hive
group: Frameworks
priority: 2
---

This guide describes how to run [Apache Hive](http://hive.apache.org/) with Alluxio, so
that you can easily store Hive tables in Alluxio's tiered storage.

# Prerequisites

The prerequisite for this part is that you have
[Java](Java-Setup.html). Alluxio cluster should also be
set up in accordance to these guides for either [Local Mode](Running-Alluxio-Locally.html) or
[Cluster Mode](Running-Alluxio-on-a-Cluster.html).

Please [Download Hive](http://hive.apache.org/downloads.html).

# Configuration

Apache Hive allows you to use Alluxio through a generic file system wrapper for the Hadoop file system.
Therefore, the configuration of Alluxio is done mostly in Hive and its under computing frameworks.

#### Configure `hive-site.xml`

You need to add the following property to `hive-site.xml` in your Hive installation `conf` directory

```xml
<property>
   <name>fs.defaultFS</name>
   <value>alluxio://<master_hostname>:19998</value>
</property>
```

#### Configure Hadoop MapReduce

If you run Hive on Hadoop MapReduce, Hive can read configirations from Hadoop configuration files. In addition,
Hive's Hadoop jobs will store its intermediate results in Alluxio.

You need to add the following properties to `core-site.xml` file in Hadoop:

```xml
<property>
   <name>fs.alluxio.impl</name>
   <value>alluxio.hadoop.FileSystem</value>
</property>
<property>
   <name>fs.AbstractFileSystem.alluxio.impl</name>
   <value>alluxio.hadoop.AlluxioFileSystem</value>
</property>
```

modify `$HADOOP_CLASSPATH` by changing `hadoop-env.sh` to:

```bash
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

Distributing the Alluxio Client Jar in Hadoop MapReduce:

- You must place the client jar
`alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`
(located in the `/<PATH_TO_ALLUXIO>/core/client/target/` directory), in the `$HADOOP_HOME/share/hadoop/common/lib`
(may be `$HADOOP_HOME/lib` for different versions of Hadoop) directory of every
MapReduce node, and restart all of the NodeManagers.

# Distribute the Alluxio Client jar

We need to make the Alluxio client `jar` file available to Hive, because it contains the configured
`alluxio.hadoop.FileSystem` class.

You can achieve this by:

- Put the `alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` file into the
  `lib` directory of Hive.

#### Add additional Alluxio site properties to Hive

If there are any Alluxio site properties you want to specify for Hive, add those to `hive-site.xml`. For example,
change `alluxio.user.file.writetype.default` from default `MUST_CACHE` to `CACHE_THROUGH`:

```xml
<property>
<name>alluxio.user.file.writetype.default</name>
<value>CACHE_THROUGH</value>
</property>
```

# Using Alluxio with Hive

Create Directories in Alluxio for Hive:

```bash
$ ./bin/alluxio fs mkdir /tmp
$ ./bin/alluxio fs mkdir /user/hive/warehouse
$ ./bin/alluxio fs chmod 775 /tmp
$ ./bin/alluxio fs chmod 775 /user/hive/warehouse
```

Then you can follow the [Hive documentation](https://cwiki.apache.org/confluence/display/Hive/GettingStarted) to use Hive

# Hive cli examples

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

![HiveTableInAlluxio]({{site.data.img.screenshot_hive_table_in_alluxio}})

Using a single query:

```
hive> select * from u_user;
```

And you can see the query results from console:

![HiveQueryResult]({{site.data.img.screenshot_hive_query_result}})
