---
layout: global
title: FUSE SDK Local Cache Overview
---

See [Local Cache Quick Start]({{ '/en/fuse-sdk/Local-Cache-Quick-Start.html' | relative_url }}) to quickly setup your FUSE SDK local cache solution
which can connects to your desired storage services.

## Local Cache Overview

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

Alluxio FUSE cache (Kernel Cache or Userspace Cache) is a single-node cache solution,
which means modifications to the underlying data storage through other Alluxio FUSE mount points or other data storage clients
may not be visible immediately by the current Alluxio FUSE cache. This would cause cached data to become stale.
Some examples are listed below:
- Metadata cache: the file or directory metadata such as size, or modification timestamp cached on `Node A` might be stale
  if the file is being modified concurrently by an application on `Node B`.
- Data cache: `Node A` may read a cached file without knowing that Node B had already deleted or overwritten the file in the underlying persistent data storage.
  When this happens the content read by `Node A` is outdated.

## Local Data Cache

### Local Kernel Data Cache Configuration

FUSE has the following I/O modes controlling whether data will be cached and the cache invalidation policy:
- `direct_io`: disables the kernel data cache. Supported in both libfuse 2 and libfuse 3, but is not supported by Alluxio FUSE libfuse 3 implementation yet.
- `kernel_cache`: always cache data in kernel and no cache invalidation is happening. This should only be enabled on filesystem where the file data is never changed externally (not through the current FUSE mount point)
- `auto_cache`: cache data in kernel and invalidate cache if the modification time or the size of the file has changed

Set up to one of the data cache option via mount command:
```shell
$ bin/alluxio-fuse mount <under_storage_dataset> <mount_point> -o direct_io
```

Kernel data cache will significantly improve the I/O performance but is easy to consume a large amount of node memory.
In plain machine environment, kernel memory will be reclaimed automatically when the node is under memory pressure
and will not affect the stability of AlluxioFuse process or other applications on the node.
However, in containerized environment, kernel data cache will be calculated as the container used memory.
When the container used memory exceeds the configured container maximum memory,
Kubernetes or other container management tool may kill one of the process in the container
which will cause the AlluxioFuse process to exit and the application running on top of the Alluxio FUSE mount point to fail.
To avoid this circumstances, use `direct_io` mode or use a script to cleanup the node kernel cache periodically.

### Local Userspace Data Cache Configuration

Userspace data cache can be enabled via
```shell
$ bin/alluxio-fuse mount <under_storage_dataset> <mount_point> \
    -o local_data_cache=<local_cache_directory> \
    -o local_data_cache_size=<size>
```
`local_data_cache` (Default = "" which means disabled): Local folder to use for local data cache
`local_data_cache_size` (Default = `512MB`): Maximum cache size for local data cache directory

Data can be cached on ramdisk or disk based on the type of the cache directory.

Example of mounting S3 bucket with local userspace cache enabled:
```shell
$ mkdir /tmp/local_cache
$ bin/alluxio-fuse mount s3://bucket_name/path/to/dataset/ /path/to/mount_point \
    -o s3a.accessKeyId=<S3 ACCESS KEY> -o s3a.secretKey=<S3 SECRET KEY> \
    -o local_data_cache=/tmp/local_cache \
    -o local_data_cache_size=5GB
# Assume s3://bucket_name/path/to/dataset/ already has a test file with 1GB size
$ time cat /path/to/mount_point/testfile > /dev/null
read 0m44.817s
user 0m0.016s
sys  0m0.293s
$ time cat /path/to/mount_point/testfile > /dev/null
read 0m0.522s
user 0m0.010s
sys  0m0.346s
```
With local disk userspace data cache enabled, reading 1GB file can achieve more than 80 times faster.

## Local Metadata Cache
Metadata cache may significantly improve the read training performance especially when loading a large amount of small files repeatedly.
FUSE kernel issues extra metadata read operations (sometimes can be 3 - 7 times more) compared to [Alluxio Java API]({{ '/en/api/Java-API.html' | relativize_url }}))
when applications are doing metadata operations or even data operations.
Even a 1-minute temporary metadata cache may double metadata read throughput or small file data loading throughput.

### Local Kernel Metadata Cache Configuration

If your environment is as follows:
- Launching Alluxio FUSE in bare metal machine
- Enough memory resources will be allocated to Alluxio FUSE container so that it will not be killed unexpectedly when memory usage (Fuse process memory + Fuse kernel cache) exceeds the configured container memory limit.

Then the recommendation is to use kernel metadata cache.

Kernel metadata cache is defined by the following FUSE mount options:
- [attr_timeout](https://manpages.debian.org/testing/fuse/mount.fuse.8.en.html#attr_timeout=T): Specifies the timeout in seconds for which file/directory metadata are cached. The default is 1.0 second.
- [entry_timeout](https://manpages.debian.org/testing/fuse/mount.fuse.8.en.html#entry_timeout=T): Specifies the timeout in seconds for which directory listing results are cached. The default is 1.0 second.

The timeout time can be enlarged via Fuse mount command:
```shell
$ bin/alluxio-fuse mount <under_storage_dataset> <mount_point> -o attr_timeout=600 -o entry_timeout=600 
```

Recommend to set the timeout values based on the following factors:
- Memory resources. The longer the timeout, the more metadata may be cached by kernel which contributes to higher memory consumption.
  One can pre-decide how much memory to allocate to metadata kernel cache first.
  Monitor the actual memory consumption while setting a large enough timeout value.
  Then decide the timeout value suitable for the target memory usage.
- Dataset in-use time. If the timeout value is bigger than the whole dataset in-use time and there are enough available memory resources,
  cache invalidation and refresh will not be triggered, thus the highest cache-hit ratio and best performance can be achieved.
- Dataset size. Kernel metadata cache for a single file takes around 300 bytes (up to 1KB), 3GB (up to 10GB) for 10 million files.
  If the memory space needed for caching the metadata of the whole dataset is much smaller than the available memory resources,
  recommend setting the timeout to your dataset in-use time. Otherwise, you may need to trade-off between memory consumption and cache-hit ratio.

Note that, even a short period (e.g. `timeout=60` or `timeout=600`) of kernel metadata cache may significantly improve the overall metadata read performance and/or data read performance.
Test against your common workloads to find out the optimal value.

### Local Userspace Metadata Cache Configuration

Userspace metadata cache can be enabled via
```shell
$ alluxio-fuse mount <under_storage_dataset> <mount_point> -o local_metadata_cache_size=<size> -o local_metadata_cache_expire=<timeout>
```
`local_metadata_cache_size` (Default = `20000` around 40MB memory): Maximum number of entries in the metadata cache. Each 1000 entries cause about 2MB memory.
`local_metadata_cache_expire` (Default = not_set, which means never expire): Specify expire time for entries in the metadata cache

The metadata is cached in the AlluxioFuse Java process heap so make sure `local_metadata_cache_size * 2KB * 2 < AlluxioFuse process maximum memory allocated`.
For example, if AlluxioFuse is launched with `-Xmx=16GB` and metadata cache can use up to 8GB memory, then `local_metadata_cache_size` should be smaller than 4 million.
