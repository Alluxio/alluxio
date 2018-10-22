---
layout: global
title: Use Cases
group: Home
priority: 1
---

* Table of Contents
{:toc}

Here is a description of a few common use cases of Alluxio.

## A Distributed Caching Layer for Data on Object Storage

In many organizations, an increasingly popular architecture is to leverage object storage as the
single source of data for big data analytics jobs like Hadoop, Spark or Presto.
Object storage can be public object storage services (e.g. AWS S3, Azure blob storage,
Google Cloud Storage, Aliyun OSS, or Tencent COS) or on-premise object stores (e.g. Ceph or Swift).

Though object stores are often more cost-effective, easier to scale, and easier to maintain, they
do not have the same capabilities as a file system, even if a filesystem-like interface is provided.
Some missing capabilities include:

- Lack of filesystem-level data caching: Multiple jobs accessing the same data set cannot benefit
from caching frequently accessed data.

- Lack of node-level data locality on computation: Data is always read remotely and never
node-local to the compute tasks.

- Different performance implications: Operations to list objects can be slow
especially when there are many objects in the bucket. Operations to rename objects can also be slow
with weaker consistency guarantees.

- Throughput limitation: Storage systems such as S3 can limit the throughput to a compute node.

- Security models: Object stores often employ different Security models than file systems.

In such architectures, deploying Alluxio on the compute side where data is configured to persist
from object stores can significantly benefit applications. 
Alluxio can cache data locally alongside different applications and manage its corresponding
metadata to avoid particular inefficient metadata operations of object stores.

See example use cases from
[BazzarVoice](https://www.slideshare.net/ThaiBui7/hybrid-collaborative-tiered-storage-with-alluxio),
[Myntra](http://alluxio-com-site-prod.s3.amazonaws.com/resource/media/myntra-case-study-accelerating-analytics-in-the-cloud-for-mobile-e-commerce).

## ETL Elimination for Satellite Compute Clusters

For reasons such as performance, security, or resource isolation, organizations maintain
satellite computation clusters that are independent from their main data cluster, using dedicated
resources for mission-critical applications. These satellite clusters often need to access data
from the main cluster. This requires either reading data remotely during job execution or
running ETL pipelines to preload data prior to job execution.

Alluxio can accelerate the remote data read from the main data cluster without adding extra ETL steps.
When deployed on the compute nodes in the satellite cluster and configured to connect to the
main data cluster, Alluxio serves as a local data proxy layer that provides the same
namespace as the main data cluster. Alluxio will transparently
cache frequently accessed data local to the satellite cluster to reduce network traffic,
decreasing the overall load of the main data cluster.

See example use cases from
[Tencent News](http://alluxio-com-site-prod.s3.amazonaws.com/resource/media/tencent-case-study-delivering-customized-news-to-over-100-million-montly-users).

## A Common Storage Access Layer

Users deploy Alluxio as a storage abstraction layer for common data access requests.
Alluxio supports storage connectors for various storage types including public cloud, such as AWS or Azure,
and on-premise storage services, such as HDFS or Ceph. As long as applications integrate with
Alluxio, they are enabled to access different persistent storage systems without binary or
source code changes in the application. Once connected to Alluxio, applications are
automatically integrated with the most popular storage options without implementing any connectors.

See example use cases from [Starburst Presto](https://www.starburstdata.com/technical-blog/starburst-presto-alluxio-better-together/),
[TensorFlow on Azure](https://blogs.msdn.microsoft.com/cloudai/2018/05/01/tensorflow-on-azure-enabling-blob-storage-via-alluxio/).

## A Single Entry Point for Multiple Data Sources

Alluxio provides a mounting API that enables applications to access data across multiple sources
in the same filesystem namespace. Applications do not need to individually configure connection
details for each data source, such as the client library version or different security models.
From the perspective of an application, it is accessing a "logical filesystem"
whose data can be backed by multiple different persistent storages.
This drastically simplifies the development, maintenance and management of an application.

See example use cases from
[Lenovo](http://alluxio-com-site-prod.s3.amazonaws.com/resource/media/lenovo-analyzes-petabytes-of-smartphone-data-from-multiple-locations-and-eliminates-etl-with-alluxio).
