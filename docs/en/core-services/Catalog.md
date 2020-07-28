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

Alluxio 2.1.0 introduces a new service within Alluxio called the Alluxio Catalog Service.
The Alluxio Catalog Service is a service for managing access to structured data, which serves a purpose similar to the
[Apache Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-Metastore)

SQL engines like Presto, SparkSQL, and Hive, leverage these metastore-like
services to determine which, and how much data to read when executing queries.
They store information about different database catalogs, tables, storage formats, data
location, and more.
The Alluxio Catalog Service is designed to make it simple and straightforward to retrieve and
serve structured table metadata to Presto query engines, e.g. [PrestoSQL](https://prestosql.io/),
[PrestoDB](https://prestodb.io/), and [Starburst Presto](https://starburstdata.com).


## Architecture

The Alluxio Catalog Service is designed in a way very similar to the normal Alluxio filesystem.
The service itself is not responsible for retaining all data, but is rather a caching service for
metadata that originates in another location (i.e. MySQL, Hive).
These are called **UDBs** (**U**nder **D**ata**B**ase).
UDBs are responsible for the management and storage of the metadata.
Currently, Hive is the only supported UDB.
The Alluxio Catalog Service caches and makes the metadata available universally through the Alluxio filesystem
namespace.

```
Query Engine     Metadata service                   Under meta service
+--------+       +--------------------------+       +----------------+
| Presto | <---> | Alluxio Catalog Service  | <---> | Hive Metastore |
+--------+       +--------------------------+       +----------------+
```

Users that have tables which span multiple storage services (i.e. AWS S3, HDFS, GCS) - would
typically need to configure their SQL engines to connect to each one of these services individually
in order to make requests.
Using catalog service, all the user needs to do is configure a single Alluxio client, and data any
supported under storage systems locations can be served and read through Alluxio.

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
Use the Table CLI to perform any actions regarding attaching, browsing, or detaching
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
and additions in the UDB tables.

## Using Alluxio Structured Data with Presto

PrestoSQL version 332 or above and PrestoDB version 0.232 or above has built-in support for the Alluxio Catalog Service in their hive-hadoop2 connector.
For instructions to setup Alluxio Catalog Service with those versions of PrestoSQL or PrestoDB,
please consult PrestoSQL's [documentation](https://prestosql.io/docs/current/connector/hive.html#alluxio-configuration) 
or PrestoDB's [documentation](https://prestodb.io/docs/current/connector/hive.html#alluxio-configuration).

If you are using PrestoSQL or PrestoDB's earlier versions, you can use the hive-alluxio connector included in
the alluxio distribution.
The latest Alluxio distribution contains a presto connector jar which can be dropped into the
`${PRESTO_HOME}/plugins` directory to enable connectivity to the catalog service via Presto.

### Enabling the Alluxio Catalog Service with Presto

Assuming you have Alluxio and Presto installation on your local machine at `${ALLUXIO_HOME}` and
`${PRESTO_HOME}` respectively, you need to copy the Alluxio connector files into the
Presto installation as a new plugin. This must be done on all Presto nodes. 

For PrestoSQL installations, run the following. 

```console
$ cp -R ${ALLUXIO_HOME}/client/presto/plugins/presto-hive-alluxio-319/ ${PRESTO_HOME}/plugin/hive-alluxio/
```

For PrestoDB installations, run the following. 

```console
$ cp -R ${ALLUXIO_HOME}/client/presto/plugins/prestodb-hive-alluxio-227/ ${PRESTO_HOME}/plugin/hive-alluxio/
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
If you are using PrestoSQL version 332 or above and PrestoDB version 0.232 or above, support for Alluxio Catalog Service is built into
the hive-hadoop2 connector, so you should set `connector.name=hive-hadoop2` here.
The `hive.metastore=alluxio` means Hive metastore connection will use the `alluxio` type, in order
to communicate with the Alluxio Catalog service.
The setting `hive.metastore.alluxio.master.address=HOSTNAME:PORT` defines the host and port of the
Alluxio catalog service, which is the same host and port as the Alluxio master.
Once configured on each node, restart all presto coordinators and workers.

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
