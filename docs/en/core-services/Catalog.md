---
layout: global
title: Catalog
nickname: Catalog
group: Core Services
priority: 2
---

* Table of Contents
{:toc}

## Overview

Alluxio 2.1.0 introduced a new service within Alluxio called the Alluxio Catalog Service.
The Alluxio Catalog Service is a service for managing access to structured data, which serves a
purpose similar to the
[Apache Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-Metastore)

SQL engines like Presto, SparkSQL, and Hive, leverage these metastore-like
services to determine which, and how much data to read when executing queries.
They store information about different database catalogs, tables, storage formats, data
location, and more.
The Alluxio Catalog Service is designed to make it simple and straightforward to retrieve and
serve structured table metadata to Presto query engines, e.g.[PrestoDB](https://prestodb.io/), 
[Trino](https://trino.io/) and [Starburst Presto](https://starburstdata.com).


## Architecture

The Alluxio Catalog Service is designed in a way very similar to the normal Alluxio filesystem.
The service itself is not responsible for retaining all data, but is rather a caching service for
metadata that originates in another location (i.e. MySQL, Hive).
The locations where the metadata originates from are called **UDBs** (**U**nder **D**ata**B**ase).
UDBs are responsible for the management and storage of the metadata.
Currently, Hive is the only supported UDB.
The Alluxio Catalog Service caches and makes the metadata available universally through the Alluxio
filesystem namespace.

```
Query Engine     Metadata service                   Under meta service
+--------+       +--------------------------+       +----------------+
| Presto | <---> | Alluxio Catalog Service  | <---> | Hive Metastore |
+--------+       +--------------------------+       +----------------+
```

Users that have tables which span multiple storage services (i.e. AWS S3, HDFS, GCS) - would
typically need to configure their SQL engines to connect to each one of these services individually
in order to make requests.
Using the Alluxio catalog service, all the user needs to do is configure a single Alluxio client,
and data any supported under storage systems locations can be served and read through Alluxio.

## Using The Alluxio Catalog Service

Here are the basic configuration parameters and ways to interact with the Alluxio Catalog Service. More details
can be found in the [command line interface documentation]({{ '/en/operation/User-CLI.html' | relativize_url }}#table-operations).

### Alluxio Server Configuration

By default, the catalog service is enabled. To explicitly disable it, add the following
line to your `alluxio-site.properties`

```properties
alluxio.table.enabled=false
```

By default, mounted databases and tables will exist underneath `/catalog` directory in Alluxio.
Configure the root directory for the catalog service by configuring

```properties
alluxio.table.catalog.path=</desired/alluxio/path>
```

### Attaching Databases

In order for Alluxio to serve information about structured table data, Alluxio must be informed
about where databases are located.
The `attachdb` command is the first command that should be used to inform the Catalog service about
table locations.
The Table CLI can be used to perform any actions regarding attaching, browsing, or detaching
databases from Alluxio.

```console
$ ${ALLUXIO_HOME}/bin/alluxio table
Usage: alluxio table [generic options]
	 [attachdb [-o|--option <key=value>] [--db <alluxio db name>] [--ignore-sync-errors] <udb type> <udb connection uri> <udb db name>]
	 [detachdb <db name>]
	 [ls [<db name> [<table name>]]]
	 [sync <db name>]
	 [transform <db name> <table name>]
	 [transformStatus [<job ID>]]
```

To attach a database use the `attachdb` command. Currently, `hive` and `glue` are supported as the
`<udb type>`.
See the [attachdb command documentation]({{ '/en/operation/User-CLI.html' | relativize_url }}#attachdb)
for more details.
The following command maps the hive database `default` into a database in Alluxio called
`alluxio_db` from the metastore located at `thrift://metastore_host:9083`

```console
$ ${ALLUXIO_HOME}/bin/alluxio table attachdb --db alluxio_db hive \
    thrift://metastore_host:9083 default
```

> **Note:** When databases are attached, all tables are synced from the configured UDB.
If out-of-band updates occur to the database or table and the user wants query results to reflect
the updates, the database must be synced. See [Syncing Databases](#syncing-databases) for more
information.

#### UDB Configuration File

To specify a configuration file for the UDB, append an option `-o catalog.db.config.file` to
`attachdb` command.
Each time the configuration file is changed, you can use `alluxio table sync` to apply the changes.

The configuration file is in JSON format, and can contain these configurations:

1. Bypassing tables and partitions:

   You can specify some tables and partitions to be bypassed from Alluxio, so that data will not be
   cached in the Alluxio file system, instead computing frameworks will be directed to access data directly from the UFS.
   This can be helpful when some tables and partitions are large, and accommodating them in the cache
   is undesirable. An example configuration is like the following:

   ```json
   {
     "bypass": {
       "tables": [
         "table1",
         {"regex": "table[23]"},
         {
           "table": "table4", 
           "partitions": [
             "table4_part1", 
             {"regex": "table4_part[23]"}
           ]
         }
       ]
     }
   }
   ```

   You can specify which tables and partitions within these tables should be bypassed from Alluxio.
   There are 2 ways to specify tables and partitions:
    
   1. Plain names: Use the exact name of a table or partition;
   2. Regular expressions: Use a pattern to capture a set of table or partition names. The pattern
      is matched as a whole on the names, i.e., it is as if the patterns were enclosed in a pair 
      of `^` and `$`.
       
   All partitions, if any, of a table specified using exact name or regular expression, will be 
   bypassed. In other words, the table is "fully" bypassed. 
    
   Additionally, you can specify some partitions of a table to be bypassed, like
   `table4` in the above example. In this case, only listed partitions will be bypassed, the rest 
   will be mounted normally.
    
   In the above example, table 1 is specified by its exact name, and will be fully bypassed. 
   Table 2 and 3 are matched by the regular expression, and will be fully bypassed.
   Partition 1, 2 and 3 of table 4 are bypassed, and any other partitions, if any, are not.
   
2. Ignoring tables:

   You can specify some tables to be ignored when attaching the database to Alluxio. Compared to 
   bypassed tables, Alluxio will not try to mount the ignored tables, therefore they will be 
   invisible to the Alluxio client. 
   
   The syntax for configuring ignored tables is similar to that of bypassing:

   ```json
   {
     "ignore": {
       "tables": [
         "table1",
         {"regex": "table[a-z]"}
       ]
     }
   }
   ```
    
   Tables are ignored as a whole. Ignoring individual partitions is not currently supported.

   > **Note:** if the same table is configured to be ignored and bypassed at the same time, it will 
   be ignored.

3. Excluding tables and partitions from bypassing or ignoring:

   Sometimes it is useful to exclude some tables and partitions from the bypassed or ignored list.
   To do so, the syntax supports an exclusion mode in tables and partitions specifications:
   
   ```json
   {
     "bypass": {
       "tables": [
         {
           "table": "partially_bypassed_table", 
           "partitions": {
             "exclude": ["normally_mounted_part"]
           }
         }
       ]
     },
     "ignore": {
       "tables": {
         "exclude": [
           "partially_bypassed_table",
           "not_ignored_table1",
           {"regex":  "not_ignored_table[23]"}
         ]
       }
     }
   }
   ```
   
   You can use names and regular expressions inside the exclusion list, just like in inclusion mode.
   In exclusion mode, the excluded tables and partitions will be mounted normally, but any other
   tables of the parent database, and any other partitions of the parent table, will be bypassed or 
   ignored.
   
   Note that to exclude a partition from a table, you need to specify the table in inclusion mode, 
   and specify that partition inside the exclusion list of the table's partition specification.
   It is an error to specify partitions of a table inside an exclusion list:
   
   ```json
   {
     "tables": {
       "exclude": [
         {"table": "table1", "partitions": ["part1"]}
       ]
     }
   }
   ```
   
   In the previous example, 
   partition `normally_mounted_part` of table `partially_bypassed_table` is excluded from 
   bypassing, meaning that the partition will be mounted normally, and any other partitions of
   `partially_bypassed_table` are bypassed. Likewise, all tables except for 
   `not_ignored_table1|2|3`, and `partially_bypassed_table` are ignored.
   
   > **Note:** since ignoring takes precedence over bypassing, when both bypassing and ignoring 
   > are enabled, and exclusion mode is used for ignoring, make sure to add the bypassed tables to 
   > the exclusion list. Otherwise, the bypassing configuration will be overridden and have no 
   > effect.
   
   > **Note:** An empty exclusion list **does not** cause all tables or partitions to be 
   > bypassed or ignored. Instead, it has no effect: no tables or partitions will be bypassed or 
   > ignored. To bypass or ignore all tables or partitions, use an inclusion list with the 
   > catch-all regular expression: `.*`.
   
Examples for common use cases:

1. Bypassing all tables of a database:

   ```json
   {
     "bypass": {
       "tables": [
         {"regex": ".*"}
       ]
     }
   }
   ```

2. Bypassing all tables except for a specific one `not_bypassed_table`:

   ```json
   {
     "bypass": {
       "tables": {
         "exclude": [ "not_bypassed_table" ]
       }
     }
   }
   ```
   
3. Bypassing all partitions of some tables, and some partitions of some other table:

   ```json
   {
     "bypass": {
       "tables": [  
         {"regex": "fully_bypassed_table\\d"},
         {
           "table": "partially_bypassed_table1", 
           "partitions": [
             {"regex": "bypassed_part_[a-z]"}
           ]
         }
       ]
     }
   }
   ```
   
4. Bypassing all partitions matching a pattern, of a group of tables matching another pattern:

   ```json
   {
     "bypass": {
       "tables": [
         {
           "table": "partially_bypassed_table1", 
           "partitions": [
             {"regex": "table1_part_[a-z]"}
           ]
         },
         {
           "table": "partially_bypassed_table2",
           "partitions": [
             {"regex": "table2_part_[a-z]"}
           ]
         }
       ]
     }
   }
   ```

   You have to specify every individual table, even though they can be captured by a pattern. It 
   might be tempting to write something like:

   ```json
   {
     "bypass": {
       "tables": [
         {
           "regex": "partially_bypassed_table(\\d)", 
           "partitions": [
             {"regex": "table$1_part_[a-z]"}
           ]
         }
       ]
     }
   }
   ```
   
   However, this is not supported.

### Exploring Attached Databases

Once attached, check that the database has been mounted with `alluxio table ls`

```console
$ ${ALLUXIO_HOME}/bin/alluxio table ls
alluxio_db
```

List the tables underneath the database with `alluxio tables ls <db_name>`.
If any tables exist underneath the corresponding database in hive,
they will appear when executing this command.

```console
$ ${ALLUXIO_HOME}/bin/alluxio table ls alluxio_db
test
```

In this case there is a table `test` in the `alluxio_db`.
To get more information about a table in a database, run
```console
$ alluxio table ls <db_name> <table_name>
```
This command will dump information about the table to the console.
An example of the output is below:

```console
$ ${ALLUXIO_HOME}/bin/alluxio table ls alluxio_db test
db_name: "alluxio_db"
table_name: "test"
owner: "alluxio"
schema {
  cols {
    name: "c1"
    type: "int"
    comment: ""
  }
}
layout {
  layoutType: "hive"
  layoutSpec {
    spec: "test"
  }
  layoutData: "\022\004test\032\004test\"\004test*\345\001\n\232\001\n2org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\022(org.apache.hadoop.mapred.TextInputFormat\032:org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\0227alluxio://localhost:19998/catalog/test/tables/test/hive\032\v\020\377\377\377\377\377\377\377\377\377\001 \0002\v\022\002c1\032\003int*\000"
}
parameters {
  key: "totalSize"
  value: "6"
}
parameters {
  key: "numRows"
  value: "3"
}
parameters {
  key: "rawDataSize"
  value: "3"
}
parameters {
  key: "COLUMN_STATS_ACCURATE"
  value: "{\"BASIC_STATS\":\"true\"}"
}
parameters {
  key: "numFiles"
  value: "3"
}
parameters {
  key: "transient_lastDdlTime"
  value: "1571191595"
}
```

### Detaching databases

If desired, databases can be detached from the Alluxio namespace by using

```console
$ alluxio table detach <database name>
```

For the previous examples, to detach we would run:

```console
$ ${ALLUXIO_HOME}/bin/alluxio table detachdb alluxio_db
```

Running `alluxio table ls` afterwards will not display the database any more.

### Syncing databases

When the underlying database and tables are updated, users can invoke the sync command
to refresh the information stored in the Alluxio catalog metadata.
See the [sync command documentation]({{ '/en/operation/User-CLI.html' | relativize_url }}#sync)
for more details.

```console
$ alluxio table sync <database name>
```

For the previous examples, to sync we would run:

```console
$ ${ALLUXIO_HOME}/bin/alluxio table sync alluxio_db
```

This sync command will update the Alluxio catalog metadata according to the updates, deletions,
and additions in the UDB tables, it will update db config as well.

## Using Alluxio Structured Data with Presto

PrestoDB version 0.232 or above and Trino version 332 or above has built-in support for the
Alluxio Catalog Service in their hive-hadoop2 connector.
For instructions to setup Alluxio Catalog Service with those versions of PrestoDB or Trino,
please consult PrestoDB's [documentation](https://prestodb.io/docs/current/connector/hive.html#alluxio-configuration)
Trino's [documentation](https://trino.io/docs/current/connector/hive-alluxio.html).

If you are using PrestoDB and Trino's earlier versions, you can use the `hive-alluxio` connector
included in the Alluxio distribution.
The latest Alluxio distribution contains a presto connector jar which can be dropped into the
`${PRESTO_HOME}/plugins` directory to enable connectivity to the catalog service via Presto.

### Enabling the Alluxio Catalog Service with Presto

Assuming you have Alluxio and Presto installation on your local machine at `${ALLUXIO_HOME}` and
`${PRESTO_HOME}` respectively, you need to copy the Alluxio connector files into the
Presto installation as a new plugin. This must be done on all Presto nodes. 

For PrestoDB installations, run the following.

```console
$ cp -R ${ALLUXIO_HOME}/client/presto/plugins/prestodb-hive-alluxio-227/ ${PRESTO_HOME}/plugin/hive-alluxio/
```

For Trino installations, run the following. 

```console
$ cp -R ${ALLUXIO_HOME}/client/presto/plugins/presto-hive-alluxio-319/ ${PRESTO_HOME}/plugin/hive-alluxio/
```

Additionally, you'll need to create a new catalog to use the Alluxio connector and
Alluxio Catalog Service:

`/etc/catalog/catalog_alluxio.properties`
```properties
connector.name=hive-alluxio
hive.metastore=alluxio
hive.metastore.alluxio.master.address=HOSTNAME:PORT
```

Creating the `catalog_alluxio.properties` file means a new catalog named `catalog_alluxio` is added
to Presto.
Setting `connector.name=hive-alluxio` sets the connector type to the name of the
new Alluxio connector for Presto, which is `hive-alluxio`.
If you are using PrestoDB version 0.232 or above and Trino version 332 or above, support for Alluxio Catalog Service is built into
the `hive-hadoop2` connector, so you should set `connector.name=hive-hadoop2` here.
The `hive.metastore=alluxio` means Hive metastore connection will use the `alluxio` type, in order
to communicate with the Alluxio Catalog service.
The setting `hive.metastore.alluxio.master.address=HOSTNAME:PORT` defines the host and port of the
Alluxio catalog service, which is the same host and port as the Alluxio master.
Once configured on each node, restart all presto coordinators and workers.

### JAVA 11 Support
Trino 330 or above will only run with Java 11. Starting with the 2.4 version, Alluxio also supports Java 11.
If you have both Java 8 and 11 installed on the same node, you will need to modify JAVA_HOME environment variable to point to the correct Java installation directory.
The following is an example of launching presto 

```console
$ JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64" ${PRESTO_HOME}/bin/launcher start
```

### Update Alluxio client library to the lastest version
Both PrestoDb and Trino now includes Alluxio's client library in their release. 
However, the version included in those releases may not be the latest version.
To update the client library to the latest version, first remove the old alluxio client library from `${PRESTO_HOME}/plugin/hive-hadoop2/` directory.
Then you can follow 
[instructions]({{ '/en/compute/Presto.html' | relativize_url }}#distribute-the-alluxio-client-jar-to-all-presto-servers) to copy the latest client to presto plugin directory.
This client update needs to be performed on all Presto nodes (coordinator and workers). 
After the files are in place, be sure to restart the presto cluster so the new library is loaded.

### Using the Alluxio Catalog Service with Presto

In order to utilize the Alluxio Presto plugin start the presto CLI with the following (assuming
the `/etc/catalog/catalog_alluxio.properties` file has been created)

```console
$ presto --catalog catalog_alluxio
```

By default, presto will now retrieve database and table information from Alluxio's catalog service
when executing any queries.

Confirm that configuration is correct by running some of the following queries:

- List out the attached databases:

```sql
SHOW SCHEMAS;
```

- List tables from one of the schemas:

```sql
SHOW TABLES FROM <schema name>;
```

- Run a simple query which will read data from the metastore and load data from a table:

```sql
DESCRIBE <schema name>.<table name>;
SELECT count(*) FROM <schema name>.<table name>;
```
