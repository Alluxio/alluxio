---
layout: global
title: FUSE SDK Local Metadata Cache
---


Metadata cache may significantly improve the read training performance especially when loading a large amount of small files repeatedly.
FUSE kernel issues extra metadata read operations (sometimes can be 3 - 7 times more) compared to [Alluxio Java API]({{ '/en/api/Java-API.html' | relativize_url }}))
when applications are doing metadata operations or even data operations.
Even a 1-minute temporary metadata cache may double metadata read throughput or small file data loading throughput.

## Local Kernel Metadata Cache Configuration

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

## Local Userspace Metadata Cache Configuration

Userspace metadata cache can be enabled via
```shell
$ alluxio-fuse mount <under_storage_dataset> <mount_point> -o local_metadata_cache_size=<size> -o local_metadata_cache_expire=<timeout>
```
`local_metadata_cache_size` (Default = `20000` around 40MB memory): Maximum number of entries in the metadata cache. Each 1000 entries cause about 2MB memory.
`local_metadata_cache_expire` (Default = not_set, which means never expire): Specify expire time for entries in the metadata cache

The metadata is cached in the AlluxioFuse Java process heap so make sure `local_metadata_cache_size * 2KB * 2 < AlluxioFuse process maximum memory allocated`.
For example, if AlluxioFuse is launched with `-Xmx=16GB` and metadata cache can use up to 8GB memory, then `local_metadata_cache_size` should be smaller than 4 million.
