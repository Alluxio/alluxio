---
layout: global
title: Running Alluxio on Google Cloud Dataproc
nickname: Google Dataproc
group: Data Applications
priority: 0
---

This guide describes how to configure Alluxio to run on [Google Cloud Dataproc](https://cloud.google.com/dataproc).

* Table of Contents
{:toc}

## Overview

Google Cloud Dataproc is a managed on-demand service to run Spark and Hadoop compute workloads.
It manages the deployment of various Hadoop Services and allows for hooks into these services for
customizations.
Aside from the added performance benefits of caching, Alluxio also enables users to run compute 
workloads against on-premise storage or even a different cloud provider's storage i.e. AWS, Azure
Blob Store.

## Prerequisites

* Account with Cloud Dataproc API enabled
* A GCS Bucket
* gcloud CLI: Make sure that the AWS CLI is set up and ready with the required AWS Access/Secret key

An GCS bucket can be configured as Alluxio's Root Under File System and to serve as the location for the
bootstrap script.
If required, the root UFS can be reconfigured to be HDFS or any other supported under store.

## Basic Setup

When creating a Dataproc cluster, Alluxio can be installed using an
[initialization action](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions)

1. The Alluxio initialization action is hosted in a publicly readable
GCS location **gs://alluxio-public/alluxio.sh**.
* A required argument when is the root UFS URI using **alluxio_root_ufs_uri**.
* Additional properties can be specified using the metadata key **alluxio_site_properties** delimited
using `;`
```console
$ gcloud dataproc clusters create <cluster_name> \
  ...
  --metadata alluxio_root_ufs_uri=<gs://my_bucket>,site_properties="alluxio.master.mount.table.root.option.fs.gcs.accessKeyId=<gcs_access_key_id>;alluxio.master.mount.table.root.option.fs.gcs.secretAccessKey=<gcs_secret_access_key"
```
* Additional files can be downloaded into `/opt/alluxio/conf` using the metadata key `alluxio_download_files_list` by specifying `http(s)` or `gs` uris delimited using `;`
```console
$ gcloud dataproc clusters create <cluster_name> \
  ...
  --metadata alluxio_root_ufs_uri=<under_storage_address>,alluxio_download_files_list="gs://$my_bucket/$my_file;https://$server/$file"
```
2. The status of the cluster deployment can be monitored using the CLI.
```console
$ gcloud dataproc clusters list
```
3. Identify the instance name and SSH into this instance to test the deployment.
```console
$ gcloud compute ssh <cluster_name>-m 
```
4. Test that Alluxio is running as expected
```console
$ alluxio runTests
```

Alluxio is installed in `/opt/alluxio/` by default.
Spark, Hive and Presto are already configured to connect to Alluxio.

> Note: The default Alluxio Worker memory is set to 1/3 of the physical memory on the instance.
If a specific value is desired, set `alluxio.worker.memory.size` in the provided
`alluxio-site.properties` or in the additional options argument.

## Run a Spark Job

The Alluxio bootstrap also takes care of setting up Spark for you.
To run a Spark application accessing data from Alluxio, simply refer to the path as
`alluxio://<cluster_name>-m:19998/<path_to_file>`; where `<cluster_name>-m` is the dataproc master
hostname.
Follow the steps in our Alluxio on Spark
[documentation]({{ '/en/compute/Spark.html#examples-use-alluxio-as-input-and-output' | relativize_url }})
to get started.
