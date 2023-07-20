---
layout: global
title: FUSE SDK Local Cache Overview
---

## Local Cache

When an application runs an operation against the local FUSE mount point,
the request will be processed by FUSE kernel, Alluxio Fuse process, and under storage sequentially.
If at any level, cache is enabled and there is a hit, cached metadata/data will be returned to the application without going through the whole process to improve the overall read performance.

Alluxio FUSE provides local metadata/data cache on the application nodes to speed up the repeated metadata/data access.

Alluxio FUSE can provide two kinds of metadata/data cache, the kernel cache and the userspace cache.
- Kernel cache is executed by Linux kernel with metadata/data stored in operating system kernel cache.
- Userspace cache is controlled and managed by Alluxio FUSE process with metadata/data stored in user configured location (process memory for metadata, ramdisk/disk for data).

The following illustration shows the layers of data provider — FUSE kernel cache, FUSE userspace cache, and the persistent data storage (the mounted UFS path).

![POSIX LOCAL CACHE]({{ '/img/posix-local-cache.png' | relativize_url }})

Since FUSE kernel cache and userspace cache both provide caching capability, although they can be enabled at the same time,
it is recommended to choose only one of them to avoid double memory consumption.
Here is a guideline on how to choose between the two cache types based on your environment and needs.
- Kernel Cache (Recommended for metadata): kernel cache provides significantly better performance, scalability, and resource consumption compared to userspace cache.
  However, kernel cache is managed by the underlying operating system instead of Alluxio or end-users.
  High kernel memory usage may affect the Alluxio FUSE pod stability in the kubernetes environment.
  This is something to watch out for when using kernel cache.
- Userspace Cache (Recommended for data): userspace cache in contrast is relatively worse in performance, scalability, and resource consumption.
  It also requires pre-calculated and pre-allocated cache resources when launching the process.
  Despite the disadvantages, users can have more fine-grain control on the cache (e.g. cache medium, maximum cache size, eviction policy)
  and the cache will not affect other applications in containerized environment unexpectedly.

### Local Cache Limitations

Alluxio FUSE cache (Kernel cache or Userspace Cache) is a single-node cache solution,
which means modifications to the underlying data storage through other Alluxio FUSE mount points or other data storage clients
may not be visible immediately by the current Alluxio FUSE cache. This would cause cached data to become stale.
Some examples are listed below:
- metadata cache: the file or directory metadata such as size, or modification timestamp cached on `Node A` might be stale
  if the file is being modified concurrently by an application on `Node B`.
- data cache: `Node A` may read a cached file without knowing that Node B had already deleted or overwritten the file in the underlying persistent data storage.
  When this happens the content read by