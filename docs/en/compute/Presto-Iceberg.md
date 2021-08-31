---
layout: global
title: Running Presto on Iceberg Tables with Alluxio
nickname: Presto on Iceberg (Experimental)
group: Compute Integrations
priority: 2
---

Presto has introduced support for [Iceberg tables](https://iceberg.apache.org/) in version 0.256.

This document describes how to use Presto to query Iceberg tables through Alluxio.
This document is currently experimental, and the information provided here is subject to change.

* Table of Contents
{:toc}

In order to use Presto to query an Iceberg table, make sure you have a working setup of Presto, 
Hive Metastore and Alluxio, and Presto can access data through Alluxio's filesystem interface.
If not, please refer to the 
[guide]({{ '/en/compute/Presto.html' | relativize_url }}) on general Presto installation 
and configuration. Most of that guide apply for Iceberg workflows as well, and this document 
covers the specific instructions for working with Iceberg tables.

The [guide]({{ '/en/compute/Presto.html' | relativize_url }}) mentions two approaches that 
Presto can access data through Alluxio: via the filesystem interface, and via the catalog service. 
Alluxio has not added support for Iceberg in the catalog service yet, so for Iceberg tables Presto 
needs to access them through Alluxio's filesystem interface.

## Prerequisites

* All [prerequisites]({{ '/en/compute/Presto.html' | relativize_url }}#prerequisites) 
  from the general Presto setup;
* Presto server, version 0.257 or later.

## Basic Setup

### Install Alluxio client jar to Presto Iceberg connector

Copy the Alluxio client jar located at `{{site.ALLUXIO_CLIENT_JAR_PATH}}` into Presto Iceberg 
connector's directory located at `${PRESTO_HOME}/plugin/iceberg/`. Then restart the Presto server:

```console
$ ${PRESTO_HOME}/bin/launcher restart
```

Also note that the same client jar file needs to be on Hive's classpath. 
If not, please refer to the [section]({{ '/en/compute/Hive.html' | relativize_url }}#basic-setup)
on setting up Hive to work with Alluxio.

### Configure Presto to use the Iceberg connector

Presto reads and writes an Iceberg table using the 
[Iceberg connector](https://prestodb.io/docs/current/connector/iceberg.html). To enable the Iceberg 
connector, create a catalog for Iceberg connector in Presto's installation directory as 
`${PRESTO_HOME}/etc/catalog/iceberg.properties`:

```properties
connector.name=iceberg
hive.metastore.uri=thrift://localhost:9083
```

Change the Hive Metastore connection URI to match your setup.

## Examples: Use Presto to Query Iceberg Tables on Alluxio

### Create a schema and an Iceberg table

For demonstration purposes, we will create an example schema and an Iceberg table.

Launch the Presto CLI client with the following command:

```console
./presto --server localhost:8080 --catalog iceberg --debug
```

For more information on the client, please refer to this section on [querying tables using Presto]
({{ '/en/compute/Presto.html' | relativize_url }}#query-tables-using-presto). Note that the 
catalog is set to `iceberg` since we will be dealing with Iceberg tables.

Run the following statements from the client:

```sql
CREATE SCHEMA iceberg_test;
USE iceberg_test;
CREATE TABLE person (name varchar, age int, id int)
    WITH (location = 'alluxio://localhost:19998/person', format = 'parquet');
```

Change the hostname and port in the Alluxio connection URI to match your setup.

These statements create a schema `iceberg_test` and a table `person` at the directory 
`/person` in Alluxio filesystem, and with Parquet as the table's storage format.

### Insert sample data into the table

Insert one row of sample data into the newly created table:

```sql
INSERT INTO person VALUES ('alice', 18, 1000);
```

Note: there was a bug in the write path of Presto's Iceberg connector, so insertion may fail. 
This issue has been resolved in Presto version 0.257 by 
[this PR](https://github.com/prestodb/presto/pull/16275).

Now you can verify things are working by reading back the data from the table:

```sql
SELECT * FROM person;
```

As well as examine the files in Alluxio:

```console
$ bin/alluxio fs ls /person
drwxr-xr-x  alluxio    alluxio    10    PERSISTED 06-29-2021 16:24:02:007  DIR /person/metadata
drwxr-xr-x  alluxio    alluxio     1    PERSISTED 06-29-2021 16:24:00:049  DIR /person/data
$ bin/alluxio fs ls /person/data
-rw-r--r--  alluxio    alluxio   400    PERSISTED 06-29-2021 16:24:00:691 100% /person/data/6e6a451a-8f20-4d73-9ef6-ee48070dad27.parquet
$ bin/alluxio fs ls /person/metadata
-rw-r--r--  alluxio    alluxio  1406    PERSISTED 06-29-2021 16:23:28:608 100% /person/metadata/00000-2fd982ae-2a81-44a8-a4db-505e9ba6c09d.metadata.json
...
(snip)
```

You can see the metadata and data files of the Iceberg table have been created.
