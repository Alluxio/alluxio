---
layout: global
title: Use Cases
group: Overview
priority: 6
---

* Table of Contents
{:toc}

Leading companies around the world run Alluxio in production to extract value
from their data. Some of them are listed in our
[Powered-By](https://www.alluxio.io/powered-by-alluxio) page.
In this section, we introduce some of the most common Alluxio use cases.

## Object Storage Data Acceleration

In many organizations, an increasingly popular architecture is to leverage object storage as an
important source of data for data analytics applications like Spark, Presto, Hadoop, or machine
learning/AI workloads such as Tensorflow etc.
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
[Myntra](https://www.alluxio.io/app/uploads/2019/05/myntra-case-study-accelerating-analytics-in-the-cloud-for-mobile-e-commerce.pdf).

## Satellite Compute Clusters Enabler

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
[Tencent News](https://www.alluxio.io/app/uploads/2019/05/tencent-case-study-delivering-customized-news-to-over-100-million-montly-users.pdf).

## A Common Data Access Layer

Users deploy Alluxio as a storage abstraction layer for common data access requests.
Alluxio supports storage connectors for various storage types including public cloud, such as AWS or Azure,
and on-premise storage services, such as HDFS or Ceph. As long as applications integrate with
Alluxio, they are enabled to access different persistent storage systems without binary or
source code changes in the application. Once connected to Alluxio, applications are
automatically integrated with the most popular storage options without implementing any connectors.

See example use cases from
[TensorFlow on Azure](https://blogs.msdn.microsoft.com/cloudai/2018/05/01/tensorflow-on-azure-enabling-blob-storage-via-alluxio/).

## A Single Entry Point for Multiple Data Sources (Data Unification)

Alluxio provides a mounting API that enables applications to access data across multiple sources
in the same filesystem namespace. Applications do not need to individually configure connection
details for each data source, such as the client library version or different security models.
From the perspective of an application, it is accessing a "logical filesystem"
whose data can be backed by multiple different persistent storages.
This drastically simplifies the development, maintenance and management of an application.

See example use cases from
[Lenovo](https://www.alluxio.io/app/uploads/2019/05/lenovo-analyzes-petabytes-of-smartphone-data-from-multiple-locations-and-eliminates-etl-with-alluxio.pdf).
