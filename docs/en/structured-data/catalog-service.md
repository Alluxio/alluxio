---
layout: global
title: Alluxio Catalog Service
nickname: Catalog Service
group: Structured Data
priority: 0
---

{% assign feature_name="the Alluxio Catalog Service" %}

* Table of Contents
{:toc}

## Overview

Alluxio 2.1.0 introduces a new service within Alluxio called {{feature_name}}.
{{feature_name}} is a service for managing access to structured data, which serves a purpose similar to the
[Apache Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-Metastore)

SQL engines, i.e. [Starburst Presto](https://starburstdata.com), leverage these metastore-like
services to determine which, and how much data to read when executing queries.
They store information about different database catalogs, tables, storage formats, data
location, and more.
{{feature_name}} is designed to make it simple and straightforward to retrieve and serve structured table
metadata to these SQL engines.


## Architecture

{{feature_name}} is designed in a way very similar to the normal Alluxio filesystem.
The service itself is not responsible for retaining all data, but is rather a caching service for
metadata that originates in another location (i.e. MySQL, Hive).
These are called **UDBs** (**U**nder **D**ata**B**ase).
UDBs are responsible for the management and storage of the metadata.
Currently, Hive is the only supported UDB.
{{feature_name}} caches and makes the metadata available universally through the Alluxio filesystem
namespace.

```
Query Engine     Metadata service            Under meta service
+--------+       +-------------------+       +----------------+
| Presto | <---> | {{feature_name}}  | <---> | Hive Metastore |
+--------+       +-------------------+       +----------------+
```

Users that have tables which span multiple storage services (i.e. AWS S3, HDFS, GCS) - would
typically need to configure their SQL engines to connect to each one of these services individually
in order to make requests.
Using {{feature_name}}, all the user needs to do is configure a single Alluxio client, and data any
supported under storage systems locations can be served and read through Alluxio.

## Using {{feature_name}}

### Alluxio configuration

To enable {{feature_name}}, add the following line to your `alluxio-site.properties`

```properties
alluxio.table.enabled=true
```

By default, mounted databases and tables will exist underneath `/catalog` directory in Alluxio.
Configure the root directory for structured data {{feature_name}} by configuring

```properties
alluxio.table.catalog.path=</desired/alluxio/path>
```

### Attaching Databases

In order for Alluxio to serve information about structured table data, Alluxio must be informed
about where databases are located.
Use the Table CLI to perform any actions regarding attaching, browsing, or detaching
databases from Alluxio.

```console
$ ${ALLUXIO_HOME}/bin/alluxio table
Usage: alluxio table [generic options]
	 [attachdb [-o|--option <key=value>] [--db <alluxio db name>] <udb type> <udb connection uri> <udb db name>]
	 [detachdb <db name>]
	 [ls [<db name> [<table name>]]]
	 [sync <db name>]
	 [transform <db name> <table name>]
	 [transformStatus [<job ID>]]
```

To attach a database use the `attachdb` command. Currently, only `hive` is supported as the
`dbType`.
This command maps the hive database `default` into a database in Alluxio called `alluxio_db` from
the metastore located at `thrift://metastore_host:9083`

```console
$ ${ALLUXIO_HOME}/bin/alluxio table attachdb --db alluxio_db hive \
    thrift://metastore_host:9083 default
```

> **Note:** When databases are attached, all tables will be synced from the configured UDB.
If out-of-band updates occur to the database or table and the user wants query results to reflect
the updates, the user must detach ([See Detaching Databases](#detaching-databases)) and then
re-attach the database to reflect up any updates.
Alternatively, if the only changes to the database are additional tables and partitions,
sync command ([See Syncing Databases](#syncing-databases)) can be used.

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
Continue to the next section to see how to use {{feature_name}} with presto

### Syncing databases

When new tables or new partitions are added to the UDB, users can invoke the sync command
to refresh the information stored in the Alluxio namespace. 


```console
$ alluxio table sync <database name>
```

For the previous examples, to sync we would run:

```console
$ ${ALLUXIO_HOME}/bin/alluxio table sync alluxio_db
```

Note that the sync operation will not remove any tables or partitions from the Alluxio namespace,
nor would it change the existing tables or partitions.

## Alluxio Structured Data with Presto

{{feature_name}} is built to be used as connector in Presto.
The latest Alluxio distribution contains a presto connector jar which can be dropped into the
`${PRESTO_HOME}/plugins` directory to enable connectivity to {{feature_name}} via Presto.

### Enabling {{feature_name}} with Presto

Assuming you have Alluxio and Presto installation on your local machine at `${ALLUXIO_HOME}` and
`${PRESTO_HOME}` respectively:

```console
$ cp -R ${ALLUXIO_HOME}/client/presto/plugin/hive-alluxio ${PRESTO_HOME}/plugin
```

Additionally, you'll need to create a Presto configuration files with the following content:

`/etc/catalog/hive-alluxio.properties`
```properties
connector.name=hive-alluxio
hive.metastore=alluxio
```

Once configured on each node, restart all presto coordinators and workers

### Using {{feature_name}} with Presto

In order to utilize the Alluxio Presto plugin start the presto CLI with the following (assuming
the `/etc/catalog/hive-alluxio.properties` file has been created)

```console
$ presto --catalog hive-alluxio
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

- Run a simple query which will read data from the metastore a load data from a table:

```sql
DESCRIBE <schema name>.<table name>;
SELECT count(*) FROM <schema name>.<table name>;
```
