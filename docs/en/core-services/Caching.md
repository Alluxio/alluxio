---
layout: global
title: Caching
nickname: Caching
group: Core Services
priority: 1
---

* Table of Contents
{:toc}

## Alluxio Storage Overview

The purpose of this documentation is to introduce users to the concepts behind Alluxio storage and
the operations that can be performed within Alluxio storage space.

Alluxio helps unify users' data across a variety of platforms while also helping to increase
overall I/O throughput. Alluxio accomplishes this by splitting storage
into two distinct categories.

- **UFS (Under File Storage, also referred to as under storage)**
    - This type of storage is represented as space which is not managed by Alluxio.
    UFS storage may come from an external file system, including HDFS or S3.
    Alluxio may connect to one or more of these UFSs and expose them within a single namespace.
    - Typically, UFS storage is aimed at storing large amounts of data persistently for extended
    periods of time.
- **Alluxio storage**
    - Alluxio manages the local storage, including memory, of Alluxio workers to act as a
    distributed buffer cache. This fast data layer between user applications and the various under
    storages results in vastly improved I/O performance.
    - Alluxio storage is mainly aimed at storing hot, transient data and is not focused on long term
    persistence.
    - The amount and type of storage for each Alluxio worker node to manage is determined by user
    configuration.
    - Even if data is not currently within Alluxio storage, files within a connected UFS are still
    visible to Alluxio clients. The data is copied into Alluxio storage when a client attempts to
    read a file that is only available from a UFS.

![Alluxio storage diagram]({{ '/img/stack.png' | relativize_url }})

Alluxio storage improves performance by storing data in memory co-located with compute nodes.
Data in Alluxio storage can be replicated to make "hot" data more readily available for
I/O operations to consume in parallel.

Replicas of data within Alluxio are independent of the replicas that may exist within a UFS.
The number of data replicas within Alluxio storage is determined dynamically by cluster activity.
Due to the fact that Alluxio relies on the under file storage for a majority of data storage,
Alluxio does not need to keep copies of data that are not being used.

Alluxio also supports tiered storage configurations such as memory, SSD, and HDD tiers which can
make the storage system media aware.
This enables decreased fetching latencies similar to how L1/L2 CPU caches operate.

## Configuring Alluxio Storage

### Paging Worker Storage

Alluxio supports finer-grained page-level (typically, 1 MB) caching storage on Alluxio workers, 
as an alternative option to the existing block-based (defaults to 64 MB) tiered caching storage. 
This paging storage supports general workloads including reading and writing, with customizable cache eviction policies similar to [block annotation policies]({{ '/en/core-services/Caching.html' | relativize_url }}#block-annotation-policies) in tiered block store.

To switch to the paging storage:
```properties
alluxio.worker.block.store.type=PAGE
```
You can specify multiple directories to be used as cache storage for the paging store.
For example, to use two SSDs (mounted at `/mnt/ssd1` and `/mnt/ssd2`):

```properties
alluxio.worker.page.store.dirs=/mnt/ssd1,/mnt/ssd2
```

You can set a limit to the maximum amount of storage for cache for each of the
directories. For example, to allocate 100 GB of space on each SSD:

```properties
alluxio.worker.page.store.sizes=100GB,100GB
```
Note that the ordering of the sizes must match the ordering of the dirs.
It is highly recommended to allocate all directories to be the same size, since the allocator will distribute data evenly.

To specify the page size:
```properties
alluxio.worker.page.store.page.size=1MB
```
A larger page size might improve sequential read performance, but it may take up more cache space. 
We recommend to use the default value (1MB) for Presto workload (reading Parquet or Orc files).

To enable the asynchronous writes for paging store:
```properties
alluxio.worker.page.store.async.write.enabled=true
```
You might find this property helpful if you notice performance degradation when there are a lot of cache misses.
