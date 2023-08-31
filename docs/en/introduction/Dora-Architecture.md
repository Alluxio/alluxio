---
layout: global
title: Dora Architecture
---

## Dora Architecture

Dora, short for Decentralized Object Repository Architecture, is the foundation of the Alluxio system.

As an open-source distributed caching storage system, Dora offers low latency, high throughput, and cost savings,
while aiming to provide a unified data layer that can support various data workloads, including AI and data analytics.

Dora leverages decentralized storage and metadata management to provide higher performance and availability,
as well as pluggable data security and governance, enabling better scalability and more efficient management of large-scale data access.

Dora’s architecture goal:
* Scalability: Scalability is a top priority for Dora, which needs to support billions of files to meet the demands of data-intensive applications, such as AI training.
* High Availability: Dora's architecture is designed with high availability in mind, with 99.99% uptime and protection against single points of failure.
* Performance: Performance is a key goal for Dora, which prioritizes Presto/Trino powered SQL analytics workloads and GPU utilization for AI workloads.

The diagram below shows the architecture design of Dora, which consists of four major components: the service registry, scheduler, client, and worker.

![Dora Architecture]({{ '/img/dora_architecture.png' | relativize_url }})

* The worker is the most important component, as it stores both metadata and data that are sharded by key, usually the path of the file.
* The client runs inside the applications and utilizes the same consistent hash algorithm to determine the appropriate worker for the corresponding file.
* The service registry is responsible for service discovery and maintains a list of workers.
* The scheduler handles all asynchronous jobs, such as preloading data to workers.

## Technical Highlights

### Caching Data Affinity

The client obtains a list of Dora workers from a highly available service registry to support tens of thousands of Alluxio workers.
The client uses a consistent hashing algorithm to determine which worker to visit based on the file path as the key,
ensuring that the same file always goes to the same worker for a maximum cache hit rate.
This avoids a performance bottleneck because a client will directly interface with the appropriate worker
without needing to refer to the service registry.

In addition, Dora's architecture allows for easy scalability by adding more nodes to the cluster.
Each worker node can support tens of millions of files, making it easy to handle increasing data volumes and growing user bases.

### Paging Data Store

Dora uses a paging store model as its cache storage, offering finer-grained caching for small to medium-sized read requests on large files.
Workloads may request specific portions of a structured file as opposed to reading the entire file.
By utilizing a positioned read approach in this situation, the read algorithm can be up to 150x more efficient in streaming bytes as compared to a sequential read in a block store model.
From the perspective of the workload, this results in a 2x to 15x throughput improvement.
Other scenarios also reap benefits as well; 9x throughput improvement can be achieved when sequentially reading an unstructured file.

![Dora read approaches]({{ '/img/dora_read_approaches.png' | relativize_url }})

### Decentralized Metadata Store

Dora spreads metadata to every worker to ensure that metadata is always accessible and available.
To optimize metadata access, Dora utilizes a two-level caching system for metadata entries.
The first level of caching is the in-memory cache, which stores metadata entries in memory.
This cache has a configurable maximum capacity and time-to-live (TTL) setting to set an expiration duration.
The second level of caching is the persistent cache, which stores metadata entries on disk using RocksDB.
The persistent cache has unbounded capacity, depending on available disk space,
and also uses TTL-based cache eviction, avoiding any active sync or invalidation.

The combination of in-memory and persistent caching helps ensure that metadata is readily available and accessible,
while also allowing for efficient use of system resources.
The decentralization of metadata avoids the bottleneck in the architecture where metadata is primarily managed by the worker nodes.
With the capability of storing 30 to 50 million files per worker,
the system can support large-scale data-intensive applications with billions of files.

### Zero-copy Networking

Dora provides a Netty-based data transmission solution that offers a 30%-50% performance improvement over gRPC.
This solution has several advantages, including fewer data copies through different thread pools,
zero-copy transmission that avoids serialization of Protobuf, optimized off-heap memory usage that prevents memory capacity issues,
and less data transfer due to the absence of additional HTTP headers.

![Zero copy network]({{ '/img/zero_copy_network.png' | relativize_url }})

### Scheduler and Distribute Load

The scheduler provides an intuitive, extensible solution for efficient job scheduling,
with consideration towards observability, scalability, and reliability.
It has also been used to implement a distributed load capable of loading billions of files.

## Benchmark Results

### Creating and Reading Large Number of Files for a Single Worker

A single worker node was used to store and serve a large number of files as a straightforward scalability test.
The test was conducted using three data points - 4.8 million files, 24 million files, and 48 million files.
The worker was able to serve 48 million files without significant performance impact.

![Single worker storage scalability]({{ '/img/single_worker_storage_scalability.png' | relativize_url }})

### Positioned Read on Structured Data

In single thread sequential read operation with random seeks within 2MB, the performance for 100KB reads is similar to Alluxio 2.x,
but for 100MB warm reads, there is a significant improvement in local NVMe throughput, ranging from 1.4x to 20x improvement.
In addition, for structured data in the Apache Arrow format with 4 processes of random partial warm read,
there was an improvement of 15x to 20x.

![Position read latency]({{ '/img/position_read_latency.png' | relativize_url }})