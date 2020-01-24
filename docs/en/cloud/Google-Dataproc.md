---
layout: global
title: Running Alluxio on Google Cloud Dataproc
nickname: Google Dataproc
group: Alluxio in the Cloud
priority: 4
---

This guide describes how to configure Alluxio to run on [Google Cloud Dataproc](https://cloud.google.com/dataproc).

* Table of Contents
{:toc}

## Overview

[Google Cloud Dataproc](https://cloud.google.com/dataproc) is a managed on-demand service to run
Spark and Hadoop compute workloads.
It manages the deployment of various Hadoop Services and allows for hooks into these services for
customizations.
Aside from the added performance benefits of caching, Alluxio also enables users to run compute 
workloads against on-premise storage or even a different cloud provider's storage i.e. AWS, Azure
Blob Store.

## Prerequisites

* Account with Cloud Dataproc API enabled
* A GCS Bucket
* gcloud CLI: Make sure that the CLI is set up with necessary GCS interoperable storage access keys.
> Note: GCS interoperability should be enabled in the Interoperability tab in
> [GCS setting](https://console.cloud.google.com/storage/settings).

A GCS is bucket required as Alluxio's Root Under File System and to serve as the location for the
bootstrap script.
If required, the root UFS can be reconfigured to be HDFS or any other supported under store.

## Basic Setup

When creating a Dataproc cluster, Alluxio can be installed using an
[initialization action](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions)

The Alluxio initialization action is hosted in a publicly readable
GCS location at **gs://alluxio-public/dataproc/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-dataproc.sh**.
* A required argument is the root UFS URI using **alluxio_root_ufs_uri**.
* Additional properties can be specified using the metadata key **alluxio_site_properties** delimited
using `;`
```console
$ gcloud dataproc clusters create <cluster_name> \
--initialization-actions gs://alluxio-public/dataproc/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-dataproc.sh \
--metadata alluxio_root_ufs_uri=<gs://my_bucket>,alluxio_site_properties="alluxio.master.mount.table.root.option.fs.gcs.accessKeyId=<gcs_access_key_id>;alluxio.master.mount.table.root.option.fs.gcs.secretAccessKey=<gcs_secret_access_key>"
```
* Additional files can be downloaded into `/opt/alluxio/conf` using the metadata key `alluxio_download_files_list` by specifying `http(s)` or `gs` uris delimited using `;`
```console
$ gcloud dataproc clusters create <cluster_name> \
--initialization-actions gs://alluxio-public/dataproc/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-dataproc.sh \
--metadata alluxio_root_ufs_uri=<under_storage_address>,alluxio_download_files_list="gs://$my_bucket/$my_file;https://$server/$file"
```

## Next steps
The status of the cluster deployment can be monitored using the CLI.
```console
$ gcloud dataproc clusters list
```
Identify the instance name and SSH into this instance to test the deployment.
```console
$ gcloud compute ssh <cluster_name>-m
```
Test that Alluxio is running as expected
```console
$ alluxio runTests
```

Alluxio is installed in `/opt/alluxio/` by default.
Spark, Hive and Presto are already configured to connect to Alluxio.

> Note: The default Alluxio Worker memory is set to 1/3 of the physical memory on the instance.
> If a specific value is desired, set `alluxio.worker.memory.size` in the provided
> `alluxio-site.properties` or in the additional options argument.

## Spark on Alluxio in Dataproc

The Alluxio initialization script configures Spark for Alluxio.
To run a Spark application accessing data from Alluxio, simply refer to the path as
`alluxio://<cluster_name>-m:19998/<path_to_file>`.
Follow the steps in our Alluxio on Spark
[documentation]({{ '/en/compute/Spark.html#examples-use-alluxio-as-input-and-output' | relativize_url }})
to get started.

To test Spark on Alluxio, simply run:
```console
spark-shell
scala> sc.textFile("alluxio://<cluster_name>-m:19998/default_tests_files/BASIC_NO_CACHE_MUST_CACHE").count
```

## Hive  on Alluxio in Dataproc

The Alluxio initialization script configures Hive for Alluxio.

To test Hive on Alluxio, simply create a table and run a query:
```console
wget http://files.grouplens.org/datasets/movielens/ml-100k.zip
unzip ml-100k.zip

alluxio fs mkdir /ml-100k
alluxio fs copyFromLocal ~/ml-100k/u.user /ml-100k/

hive
hive> CREATE EXTERNAL TABLE u_user (
    userid INT,
    age INT,
    gender CHAR(1),
    occupation STRING,
    zipcode STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    LOCATION 'alluxio://<cluster_name>-m:19998/ml-100k';
hive> select * from u_user limit 10;
```

## Presto on Alluxio in Dataproc

The Alluxio initialization script configures Presto for Alluxio.
If installing the optional Presto component, Presto must be installed before Alluxio.
Initialization action are executed sequentially and the Presto action must precede the Alluxio action.
For instance, the sequence should be
`--initialization-actions gs://dataproc-initialization-actions/presto/presto.sh,gs://alluxio-public/dataproc/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-dataproc.sh`
> Note: The Presto initialization action should install in the home directory `/opt/presto-server`
> for Alluxio to be configured correctly.

To test Presto on Alluxio, simply run a query on the table created in the Hive section above:
```console
presto --execute "select * from u_user limit 10;" --catalog hive --schema default
```
