---
layout: global
title: Use Cases
group: Home
---

* Table of Contents
{:toc}


Here is a description of a few common use cases of Alluxio.

## A Distributed Caching Layer for Data on Cloud or Object Storage

In many companies or organizations, an increasingly popular architecture for
big-data analytics is to leverage the more cost-effective object storage as the
single source of data. In this architecture, Bigdata analytics like Hadoop
or Spark jobs directly
access either the public cloud object services (e.g., AWS S3, Azure blob
storage, Google Cloud Storage, Aliyun OSS, Tencent COS and etc) or the
on-premise object stores (e.g., Ceph, Swift and etc).

One problem is that even object stores may provide a filesystem-like interface
they are not a "real" filesystem for a few reasons:

- Lack of filesystem-level data caching so when multiple jobs are access the
 same set of data, they will not benefit from caching frequently accessed common
 data like in a local filesystem.
- Lack of node-level data locality on compute frameworks as data is always read
 remotely and never node local to the compute tasks.
- Operations to list objects can be very slow especially when there is a massive
 amount of objects in the bucket.
- Operations to rename objects can be very inefficient and often implies weaker
 consistency guarantees.

In architecture like this, deploying Alluxio on compute side and configured with
data persisted at these object stores can greatly benefit the
application. Alluxio can not only cache the hot data local to the compute and
possbiliy shared by different applications, but also manage the metadata
separately to avoid certain slow object operations to object stores.

See example use cases from
[BazzarVoice](https://www.slideshare.net/ThaiBui7/hybrid-collaborative-tiered-storage-with-alluxio), [Myntra](http://alluxio-com-site-prod.s3.amazonaws.com/resource/media/myntra-case-study-accelerating-analytics-in-the-cloud-for-mobile-e-commerce).

## ETL Elimination for Satellite Compute Clusters

For reasons including performance, security or resource isolation, many
organizations create satellite computation clusters (independent from their main
Hadoop cluster) using dedicated resource for mission-critical
applications. These satellite clusters often need to access data from the main
Hadoop cluster, leading to the need to either read data remotely on job
execution or run ETL pipelines to preload data into satellite clusters prior to
jobs execution.

Alluxio can accelerate the remote data read from main Hadoop cluster without
adding extra ETL steps ahead. Deployed on the compute nodes on the satellite
cluster and configured to connect to the main Hadoop cluster, an Alluxio service
can serve like a local data proxy layer that provides the same namespace of the
main HDFS data to the local compute jobs. In addition, Alluxio will
transparently cache frequently accessed data to improve the locality and reduce
network traffic of these jobs.

See example use cases from [Tencent News](http://alluxio-com-site-prod.s3.amazonaws.com/resource/media/tencent-case-study-delivering-customized-news-to-over-100-million-montly-users).

## Common Storage Access Service

Many users deploy Alluxio as a storage abstraction layer for common data access
request.  Alluxio supports storage connectors for many storage types including
public cloud such as AWS or Azure and on-premise storage services like HDFS,
Ceph and etc. As long as data applications integrate with Alluxio, and they are
enabled to access many different persistent storage systems without binary or
source code change at application. In other words, once connected to Alluxio,
applications are automatically connected to the most popular choices for storage
without implementing the connectors.

See example use cases from [Starburst Presto](https://www.starburstdata.com/technical-blog/starburst-presto-alluxio-better-together/),
[TensorFlow on Azure](https://blogs.msdn.microsoft.com/cloudai/2018/05/01/tensorflow-on-azure-enabling-blob-storage-via-alluxio/).

## A Single Entry Point for Multiple Data Sources

Alluxio provides a mounting API that makes it possible for applications to
access data across multiple sources in the same filesystem namespace without
the complexity to configure connection details like client library version,
different security models and etc. From applications' perspective, they are
access a "logical filesystem" whose data can be backed by multiple different
persistent storages (or under storage in Alluxio terminology). This can greatly
simplify the application development, maintenance and management.

See example use cases from
[Lenovo](http://alluxio-com-site-prod.s3.amazonaws.com/resource/media/lenovo-analyzes-petabytes-of-smartphone-data-from-multiple-locations-and-eliminates-etl-with-alluxio).
