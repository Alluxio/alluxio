---
layout: global
title: FUSE SDK Local Data Cache
group: FUSE SDK
priority: 6
---

* Table of Contents
  {:toc}

## Local Kernel Data Cache

FUSE has the following I/O modes controlling whether data will be cached and the cache invalidation policy:
- `direct_io`: disables the kernel data cache. Supported in both libfuse 2 and libfuse 3, but is not supported by Alluxio FUSE libfuse 3 implementation yet.
- `kernel_cache`: always cache data in kernel and no cache invalidation is happening. This should only be enabled on filesystem where the file data is never changed externally (not through the current FUSE mount point)
- `auto_cache`: cache data in kernel and invalidate cache if the modification time or the size of the file has changed

Set up to one of the data cache option via mount command:
```console
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

## Local Userspace Data Cache

Userspace data cache can be enabled via
```console
$ bin/alluxio-fuse mount <under_storage_dataset> <mount_point> -o local_data_cache=<local_cache_directory> -o local_data_cache_size=<size>
```
`local_data_cache` (Default = "" which means disabled): Local folder to use for local data cache
`local_data_cache_size` (Default = `512MB`): Maximum cache size for local data cache directory

Data can be cached on ramdisk or disk based on the type of the cache directory.

Example of mounting S3 bucket with local userspace cache enabled:
```console
$ mkdir /tmp/local_cache
$ bin/alluxio-fuse mount s3://bucket_name/path/to/dataset/ /path/to/mount_point -o s3a.accessKeyId=<S3 ACCESS KEY> -o s3a.secretKey=<S3 SECRET KEY> -o local_data_cache=/tmp/local_cache -o local_data_cache_size=5GB
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
