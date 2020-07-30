---
layout: global
title: Running Alluxio on Google Cloud Dataproc
nickname: Google Dataproc
group: Cloud Native
priority: 4
---

This guide describes how to configure Alluxio to run on
[Google Cloud Dataproc](https://cloud.google.com/dataproc).

* Table of Contents
{:toc}

## Overview

[Google Cloud Dataproc](https://cloud.google.com/dataproc) is a managed on-demand service to run
Presto, Spark and Hadoop compute workloads.
It manages the deployment of various Hadoop Services and allows for hooks into these services for
customizations.
Aside from the added performance benefits of caching, Alluxio also enables users to run compute 
workloads against on-premise storage, or even a different cloud provider's storage such as AWS S3
and Azure Blob Store.

## Prerequisites

* A project with Cloud Dataproc API and Compute Engine API enabled.
* A GCS Bucket.
* Make sure that the gcloud CLI is set up with necessary GCS interoperable storage access keys.
> Note: GCS interoperability should be enabled in the Interoperability tab in
> [GCS setting](https://console.cloud.google.com/storage/settings).

A GCS bucket is required if mounted to the root of the Alluxio namespace.
Alternatively, the root UFS can be reconfigured to HDFS or any other supported under store.

## Basic Setup

When creating a Dataproc cluster, Alluxio can be installed using an
[initialization action](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions).

### Create a cluster

There are several properties set as metadata labels which control the Alluxio Deployment. 
* A required argument is the root UFS address configured using **alluxio_root_ufs_uri**. 
If value `LOCAL` is provided, hdfs launched by the current dataproc cluster will be used as Alluxio root UFS.
* Properties must be specified using the metadata key **alluxio_site_properties** delimited using
a semicolon (`;`).

Example 1: use google cloud storage bucket as Alluxio root UFS
```console
$ gcloud dataproc clusters create <cluster_name> \
--initialization-actions gs://alluxio-public/dataproc/{{site.ALLUXIO_VERSION_STRING}}/alluxio-dataproc.sh \
--metadata \
alluxio_root_ufs_uri=gs://<my_bucket>,\
alluxio_site_properties="fs.gcs.accessKeyId=<my_access_key>;fs.gcs.secretAccessKey=<my_secret_key>"
```

Example 2: use Dataproc internal HDFS as Alluxio root UFS
```console
$ gcloud dataproc clusters create <cluster_name> \
--initialization-actions gs://alluxio-public/dataproc/{{site.ALLUXIO_VERSION_STRING}}/alluxio-dataproc.sh \
--metadata \
alluxio_root_ufs_uri="LOCAL",\
alluxio_site_properties="alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration=/etc/hadoop/conf/core-site.xml:/etc/hadoop/conf/hdfs-site.xml"
```

### Customization

The Alluxio deployment on Google Dataproc can customized for more complex scenarios by passing
additional metadata labels to the `gcloud clusters create` command.
{% accordion download %}
  {% collapsible Enable Active Sync on HDFS Paths %}
[Active Sync]({{ '/en/core-services/Unified-Namespace.html#metadata-active-sync-for-hdfs' | relativize_url}})
can be enabled on paths in Alluxio for a root HDFS mount point using the metadata key
`alluxio_sync_list`.
Specify a list of paths in Alluxio delimited using `;`.
```console
...
--metadata \
alluxio_sync_list="/tmp;/user/hadoop",\
...
```
  {% endcollapsible %}

  {% collapsible Download Additional Files %}
Additional files can be downloaded into the Alluxio installation directory at `/opt/alluxio/conf`
using the metadata key `alluxio_download_files_list`.
Specify `http(s)` or `gs` uris delimited using `;`.
```console
...
--metadata \
alluxio_download_files_list="gs://<my_bucket>/<my_file>;https://<server>/<file>",\
...
```
  {% endcollapsible %}

  {% collapsible Tiered Storage %}
The default Alluxio Worker memory is set to 1/3 of the physical memory on the instance.
If a specific value is desired, set `alluxio.worker.memory.size` in the provided
`alluxio-site.properties`.

Alternatively, when volumes such as
[Dataproc Local SSDs](https://cloud.google.com/dataproc/docs/concepts/compute/dataproc-local-ssds)
are mounted, specify the metadata label `alluxio_ssd_capacity_usage` to configure the percentage
of all available SSDs on the virtual machine provisioned as Alluxio worker storage.
Memory is not configured as the primary Alluxio storage tier in this case.

Pass additional arguments to the `gcloud clusters create` command.
```console
...
--num-worker-local-ssds=1 \
--metadata \
alluxio_ssd_capacity_usage="60",\
...
``` 
  {% endcollapsible %}
{% endaccordion %}

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
$ sudo runuser -l alluxio -c "alluxio runTests"
```

Alluxio is installed and configured in `/opt/alluxio/`. Alluxio services are started as `alluxio` user.

## Compute Applications

Spark, Hive and Presto on Dataproc are pre-configured to connect to Alluxio.

{% navtabs compute %}
{% navtab Spark %}
To run a Spark application accessing data from Alluxio, simply refer to the path as
`alluxio:///<path_to_file>`.

Open a shell.
```console
$ spark-shell
```

Run a sample job.
```console
scala> sc.textFile("alluxio:///default_tests_files/BASIC_NO_CACHE_MUST_CACHE").count
```

For further information, visit our Spark on Alluxio
[documentation]({{ '/en/compute/Spark.html#examples-use-alluxio-as-input-and-output' | relativize_url }}).

{% endnavtab %}
{% navtab Hive %}
Download a sample dataset.
```console
$ wget http://files.grouplens.org/datasets/movielens/ml-100k.zip
$ unzip ml-100k.zip
```

Copy the data to Alluxio
```console
$ alluxio fs mkdir /ml-100k
$ alluxio fs copyFromLocal ~/ml-100k/u.user /ml-100k/
```

Open the Hive CLI.
```console
$ hive
```

Create a table.
```console
hive> CREATE EXTERNAL TABLE u_user (
    userid INT,
    age INT,
    gender CHAR(1),
    occupation STRING,
    zipcode STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    LOCATION 'alluxio:///ml-100k';
```
Run a query.
```console
hive> select * from u_user limit 10;
```

For further information, visit our Hive on Alluxio 
[documentation]({{ '/en/compute/Hive.html' | relativize_url }}).

{% endnavtab %}
{% navtab Presto %}

Note: There are two ways to install Presto on Dataproc.
* [Optional Component for Presto](https://cloud.google.com/dataproc/docs/concepts/components/presto)
is the default Presto configuration with the install home as `/usr/lib/presto`.
To use this mechanism, no additional configuration is needed for the Alluxio initialization action.
* If using an initialization action to install an alternate distribution of Presto, override the
default home directory as it differs from the install home for the optional component.
Set the metadata label `alluxio_presto_home=/opt/presto-server` with the `gcloud clusters create`
command to ensure Presto is configured to use Alluxio.

To test Presto on Alluxio, simply run a query on the table created in the Hive section above:
```console
presto --execute "select * from u_user limit 10;" --catalog hive --schema default
```

For further information, visit our Presto on Alluxio 
[documentation]({{ '/en/compute/Presto.html' | relativize_url }}).

{% endnavtab %}
{% endnavtabs %}
