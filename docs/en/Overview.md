---
layout: global
title: Introduction
group: Overview
priority: 1
---

* Table of Contents
{:toc}

## What is Alluxio

Alluxio is world's first open source [data orchestration technology](https://www.alluxio.io/blog/data-orchestration-the-missing-piece-in-the-data-world/)
for analytics and AI for the cloud. It bridges the gap between data driven applications and storage
systems, bringing data from the storage tier closer to the data driven applications and makes it
easily accessible enabling applications to connect to numerous storage systems through a common
interface. Alluxio’s memory-first tiered architecture enables data access at speeds orders of
magnitude faster than existing solutions.

In the data ecosystem, Alluxio lies between data driven applications, such as Apache Spark, Presto, Tensorflow, Apache HBase, Apache Hive, or Apache Flink, and various persistent storage systems, such
as Amazon S3, Google Cloud Storage, OpenStack Swift, HDFS, IBM Cleversafe, EMC ECS, Ceph,
NFS, Minio, and Alibaba OSS. Alluxio unifies the data stored in these different storage systems,
presenting unified client APIs and a global namespace to its upper layer data driven applications.

The Alluxio project originated from [the UC Berkeley AMPLab](https://amplab.cs.berkeley.edu/software/) (see [papers](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html)) as
the data access layer of the Berkeley Data Analytics Stack ([BDAS](https://amplab.cs.berkeley.edu/bdas/)).
It is open source under [Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE).
Alluxio is one of the fastest growing open source projects that has attracted more than [1000 contributors](https://github.com/alluxio/alluxio/graphs/contributors) from over 300 institutions including Alibaba, Alluxio, Baidu, CMU, Google, IBM, Intel, NJU, Red Hat, Tencent, UC Berkeley, and Yahoo.

Today, Alluxio is deployed in production by [hundreds of organizations](https://www.alluxio.io/powered-by-alluxio)
with the largest deployment exceeding 1,500 nodes.

<p align="center">
<img src="https://d39kqat1wpn1o5.cloudfront.net/app/uploads/2021/07/alluxio-overview-r071521.png" width="800" alt="Ecosystem"/>
</p>

## Alluxio’s Next-Gen Architecture: DORA

DORA, short for Decentralized Object Repository Architecture, is the next-generation architecture of Alluxio.

As an open-source distributed caching storage system, DORA offers low latency, high throughput, and cost savings, while aiming to provide a unified data layer that can support various data workloads, including AI and data analytics.

DORA leverages decentralized storage and metadata management to provide higher performance and availability, as well as pluggable data security and governance, enabling better scalability and more efficient management of large-scale data access.

DORA’s architecture goal:
* Scalability: Scalability is a top priority for DORA, which needs to support billions of files to meet the demands of data-intensive applications, such as AI training.
* High Availability: DORA's architecture is designed with high availability in mind, with 99.99% uptime and protection against single points of failure at the master level.
* Performance: Performance is also a key goal for DORA, which prioritizes faster insights for Presto/Trino type of SQL analytics workloads and GPU utilization for AI workloads.

Please refer to this [doc]({{ '/en/overview/why-dora.html' | relativize_url }}) for more information of why moving from Alluxio 2.X to Dora.

The diagram below shows the architecture design of DORA, which consists of four major components: the service registry, scheduler, client, and worker.

![Dora Architecture]({{ '/img/dora_architecture.png' | relativize_url }})

* The service registry is responsible for service discovery and maintains a list of workers.
* The scheduler handles all asynchronous jobs, such as distributed load.
* The client runs inside the applications and includes a consistent hash algorithm to determine which worker to visit.
* The worker is the most important component, as it stores both metadata and data that are sharded by key, usually the path of the file.

## Technical Highlights

### Caching Data Affinity

The client obtains a list of all the DORA workers from a highly available service registry such as Alluxio Master based on Raft or Kubernetes ETCD,
which can support tens of thousands of Alluxio workers.
The client then uses a consistent hashing algorithm to determine which worker to visit based on the file path as the key, ensuring that the same file always goes to the same worker for a maximum cache hit rate.
As the service registry is not in the critical I/O path, it will not be a performance bottleneck.

In addition, DORA's architecture allows for easy scalability by simply adding more nodes to the cluster.
Each worker node can support tens of millions of files, making it easy to handle increasing data volumes and growing user bases.

### Page Data Store

DORA uses a page store module as its cache storage, offering finer-grained caching for small to medium-sized read requests on large files.
This page store has been battle-tested in applications like Presto in Meta, Uber, and TikTok, proving its reliability.
DORA's fine-grained caching has resulted in solving up to 150X read amplification issues and improving unstructured file position read up to 9X.
Additionally, it has improved structured file position read by 2 to 15X.

![Dora read approaches]({{ '/img/dora_read_approaches.png' | relativize_url }})

### Decentralized Metadata Store

DORA spreads metadata to every worker to ensure that metadata is always accessible and available.
To optimize metadata access, DORA utilizes a two-level caching system for metadata entries.
The first level of caching is the in-memory cache, which stores metadata entries in memory. This cache has a configurable maximum capacity and time-to-live (TTL) setting to set an expiration duration. The second level of caching is the persistent cache, which stores metadata entries on disk using RocksDB. The persistent cache has unbounded capacity, depending on available disk space, also uses TTL-based cache eviction, avoiding any active sync or invalidation. The stored metadata is hashed by the full UFS path like the Page Store.

The combination of in-memory and persistent caching helps ensure that metadata is readily available and accessible,
while also allowing for efficient use of system resources.
The decentralization of metadata avoids the bottleneck in the architecture where metadata is not primarily managed by the worker nodes.
With the ability to store up to 30 million to 50 million files per DORA worker,
the system can support large-scale data-intensive applications with billions of files.

### Zero-copy Networking

DORA provides a Netty-based data transmission solution that offers a 30%-50% performance improvement over gRPC.
This solution has several advantages, including fewer data copies through different thread pools,
zero-copy transmission that avoids serialization of Protobuf, optimized off-heap memory usage that prevents OOM errors,
and less data transfer due to the absence of additional HTTP headers.

![Dora read approaches]({{ '/img/zero_copy_network.png' | relativize_url }})

### Scheduler and Distribute Load

Our scheduler provides an intuitive, extendable solution for efficient job scheduling,
with consideration towards observability, scalability, and reliability.
It has also been used to implement a distributed load capable of loading billions of files.

## Benchmark Results

### Creating and Reading Large Numbers of Files (per worker)

During a simple scalability test, DORA was tested for its ability to store and serve files on a single worker node without any performance regression.
The test was conducted using three data points - 4.8 million files, 24 million files, and 48 million files.
While the worker was able to store and serve 480 million files without any significant performance downgrade.

![Single worker storage scalability]({{ '/img/single_worker_storage_scalability.png' | relativize_url }})

### Positioned Read on Structured Data

DORA uses a new position read approach that has led to significant performance improvements.
In single thread sequential read, with random seeks within 2MB, the performance for 100KB reads is similar to before,
but for 100MB warm reads, there is a significant improvement in local NVMe throughput, ranging from 1.4X to 20X performance improvement.
In addition, for structured data in the Apache Arrow format with 4 processes of random partial warm read,
there was an improvement of 15X to 20X in performance.

![Position read latency]({{ '/img/position_read_latency.png' | relativize_url }})

### Conclusion and Future Works

In conclusion, DORA is a decentralized object repository architecture that offers low latency, high throughput,
and cost savings while supporting various data workloads, including machine learning and analytics.
The architecture is designed with scalability, high availability, and performance in mind,
aiming to provide a unified data layer that can support billions of files.

DORA's release marks a significant milestone in the evolution of Alluxio,
enabling organizations to remain competitive in rapidly evolving markets.

We will continue to enhance DORA's scalability, reliability, and performance through collaborations with our partners in the open-source community.
Additionally, we will explore further in the following areas:

* Further optimizations for cost-efficiency and storage efficiency
* Enhancing RESTful APIs for both data and metadata
* Better support for ETL and remote shuffle workloads.

We welcome everyone to join our community and try out our Dora in your data infrastructure.
Feel free to post issues and pull requests to our [GitHub](https://github.com/alluxio/alluxio).
Also, please find us on the [Alluxio community chat](https://alluxio-community.slack.com/).

[Download Alluxio with Dora archiecture](https://downloads.alluxio.io/downloads/files/) today.
