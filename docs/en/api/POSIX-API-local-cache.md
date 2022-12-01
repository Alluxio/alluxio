---
layout: global
title: FUSE-based POSIX API with Alluxio Local Cache
nickname: POSIX API with Alluxio Local Cache
group: Client APIs
priority: 3
---

* Table of Contents
  {:toc}

The Alluxio POSIX API is a feature that allows mounting the training dataset
in a specific storage service (e.g. S3, HDFS) to the local filesystem
and provides local caching capabilities to speed up I/O access to frequently used data.

## Prerequisites

The followings are the basic requirements running ALLUXIO POSIX API.

- On one of the following supported operating systems
  * MacOS 10.10 or later
  * CentOS - 6.8 or 7
  * RHEL - 7.x
  * Ubuntu - 16.04
- Install JDK 11, or newer
  - JDK 8 has been reported to have some bugs that may crash the FUSE applications, see [issue](https://github.com/Alluxio/alluxio/issues/15015) for more details.
- Install libfuse
  - On Linux, we support libfuse both version 2 and 3
    - To use with libfuse2, install [libfuse](https://github.com/libfuse/libfuse) 2.9.3 or newer (2.8.3 has been reported to also work with some warnings). For example on a Redhat, run `yum install fuse`
    - To use with libfuse3, install [libfuse](https://github.com/libfuse/libfuse) 3.2.6 or newer (We are currently testing against 3.2.6). For example on a Redhat, run `yum install fuse3`
    - See [Select which libfuse version to use](#select-libfuse-version) to learn more about the libfuse version used by alluxio
  - On MacOS, install [osxfuse](https://osxfuse.github.io/) 3.7.1 or newer. For example, run `brew install osxfuse`

## Installation

Download the binary distribution of `alluxio-fuse`:
```console
$ wget https://downloads.alluxio.io/downloads/files/2.9.1/alluxio-fuse-{{site.ALLUXIO_VERSION_STRING}}.tar.gz
$ tar -xzf alluxio-fuse-{{site.ALLUXIO_VERSION_STRING}}.tar.gz
$ cd alluxio-fuse-{{site.ALLUXIO_VERSION_STRING}}
```

## Mount Under Storage Dataset

Alluxio POSIX API allows accessing data from under storage as local directories.
This is enabled by using the `mount` command to mount a UFS address to local mount point:
```console
$ bin/alluxio-fuse mount ufs_path mount_point -o alluxio_property_key=value
```
`ufs_path`: The full UFS path
`mount_point`: The local mount point to mount the UFS path to. Note that the `<mount_point>` must be an existing and empty path in your local file system hierarchy
and that the user that runs the `mount` command must own the mount point and have read and write permissions on it.
`-o alluxio_property_key=value`: All the under storage credentials and configuration can be provided using this format.
The available under storage configuration and their detail documentation can be found under the `Storage Integrations` tap of the left of the doc page.

After mounting, `alluxio-fuse` mount can be found
```console
$ mount | grep "alluxio-fuse"
alluxio-fuse on mount_point type fuse.alluxio-fuse (rw,nosuid,nodev,relatime,user_id=1000,group_id=1000)
```

AlluxioFuse process is launched
```console
$ jps
34884 AlluxioFuse
```

All the fuse logs can be found at `logs/fuse.log` and all the fuse outputs can be found at `logs/fuse.out` which are
useful for troubleshooting when errors happen on operations under the filesystem.

### Example: Mounts a S3 dataset

Mounts the dataset in target S3 bucket to a local folder:
```console
$ bin/alluxio-fuse mount s3://my_bucket/directory/ /mnt/alluxio-fuse -o s3a.accessKeyId=<S3 ACCESS KEY> -o s3a.secretKey=<S3 SECRET KEY>
```
Other [S3 configuration]({{ '/en/ufs/S3.html' | relativize_url }}#advanced-setup) (e.g. `-o alluxio.underfs.s3.region=<region>`) can also be set via the `-o alluxio_property_key=value` format.

### Example: Mounts a GCS dataset

Mounts the dataset in target google cloud storage to a local folder:
```console
$ bin/alluxio-fuse mount gs://my_bucket/directory/ /mnt/alluxio-fuse -o fs.gcs.credential.path=/path/to/<google_application_credentials>.json
```

## Example: Run operations

After mounting the dataset from under storage to local mount point,
standard tools (for example, `ls`, `cat` or `mkdir`) will have basic access
to the under storage. With the POSIX API integration, applications can interact with the remote under storage no
matter what language (C, C++, Python, Ruby, Perl, or Java) they are written in without any under storage
library integrations.

### Write Through to Mounted Under Storage Dataset

All the local mount point write will be directly write through to the underlying mounted under storage dataset:
```console
$ cd /mnt/alluxio-fuse
$ mkdir testfolder
$ dd if=/dev/zero of=testfolder/testfile bs=5MB count=1
```

`folder` will be directly created at `ufs_path/testfolder` (e.g. `s3://my_bucket/directory/testfolder`
`testfolder/testfile` will be directly written to `ufs_path/testfolder/testfile` (e.g. `s3://my_bucket/directory/testfolder/testfile`

### Read Through from Mounted Under Storage Dataset

Without the [local cache](#local-cache) functionalities that we will talk about later, all the read operations
via the local mount point will be translated to read operations against the underlying data storage:

```console
$ cd /mnt/alluxio-fuse
$ cp -r /mnt/alluxio-fuse/testfolder /tmp/
$ ls /tmp/testfolder
-rwx------.  1 ec2-user ec2-user 5000000 Nov 22 00:27 testfile
```
The read from `/mnt/alluxio-fuse/testfolder` will be translated to a read targeting `ufs_path/testfolder/testfile` (e.g. `s3://my_bucket/directory/testfolder/testfile`.
Data will be read from remote under storage directly.

## Unmount

Unmount a mounted FUSE mount point
```console
$ bin/alluxio-fuse unmount mount_point
```
After unmounting the FUSE mount point, the corresponding `AlluxioFuse` process should be killed 
and the mount point should be removed. For example:
```console
$ bin/alluxio-fuse unmount /mnt/alluxio-fuse
$ mount | grep "alluxio-fuse"
$ jps | grep "AlluxioFuse"
```

## Local Cache

When an application runs an operation against the local FUSE mount point.
The request will be processed by FUSE kernel, Alluxio Fuse process, and under storage sequentially.
If at any level, cache is enabled and there is a hit, cached metadata/data will be returned to the application without going through the whole process to improve the overall read performance.

Alluxio FUSE provides local metadata/data cache on the application nodes to speed up the repeated metadata/data access.

Alluxio FUSE can provide two kinds of metadata/data cache, the kernel cache and the userspace cache.
- Kernel cache is executed by Linux kernel with metadata/data stored in operating system kernel cache.
- Userspace cache is controlled and managed by Alluxio FUSE process with metadata/data stored in user configured location (process memory for metadata, ramdisk/disk for data).

The following illustration shows the layers of data provider — FUSE kernel cache, FUSE userspace cache, and the persistent data storage (the mounted UFS path).

<p align="center">
<img src="{{ '/img/posix-local-cache.png' | relativize_url }}" alt="Alluxio stack with its POSIX API"/>
</p>

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

#### Local Cache Limitations

Alluxio FUSE cache (Kernel cache or Userspace Cache) is a single-node cache solution,
which means modifications to the underlying data storage through other Alluxio FUSE mount points or other data storage clients
may not be visible immediately by the current Alluxio FUSE cache. This would cause cached data to become stale.
Some examples are listed below:
- metadata cache: the file or directory metadata such as size, or modification timestamp cached on `Node A` might be stale
  if the file is being modified concurrently by an application on `Node B`.
- data cache: `Node A` may read a cached file without knowing that Node B had already deleted or overwritten the file in the underlying persistent data storage.
  When this happens the content read by `Node A` is stale.

#### Metadata Cache

Metadata cache may significantly improve the read training performance especially when loading a large amount of small files repeatedly.
FUSE kernel issues extra metadata read operations (sometimes can be 3 - 7 times more) compared to [Alluxio Java API]({{ '/en/api/Java-API.html' | relativize_url }}))
when applications are doing metadata operations or even data operations.
Even a 1-minute temporary metadata cache may double metadata read throughput or small file data loading throughput.

{% navtabs metadataCache %}
{% navtab Kernel Metadata Cache Configuration %}

If your environment is as follows:
- Launching Alluxio FUSE in bare metal machine
- Enough memory resources will be allocated to Alluxio FUSE container so that it will not be killed unexpectedly when memory usage (Fuse process memory + Fuse kernel cache) exceeds the configured container memory limit.

Then the recommendation is to use kernel metadata cache.

Kernel metadata cache is defined by the following FUSE mount options:
- [attr_timeout](https://manpages.debian.org/testing/fuse/mount.fuse.8.en.html#attr_timeout=T): Specifies the timeout in seconds for which file/directory metadata are cached. The default is 1.0 second.
- [entry_timeout](https://manpages.debian.org/testing/fuse/mount.fuse.8.en.html#entry_timeout=T): Specifies the timeout in seconds for which directory listing results are cached. The default is 1.0 second.

The timeout time can be enlarged via Fuse mount command:
```console
$ bin/alluxio-fuse mount ufs_path mount_point -o attr_timeout=600 -o entry_timeout=600 
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

{% endnavtab %}
{% navtab Userspace Metadata Cache Configuration %}

Userspace metadata cache can be enabled via
```console
$ bin/alluxio-fuse mount ufs_path mount_point -o metadata_cache_size=<size> -o metqadata_cache_expire=<timeout>
```
`metadata_cache_size` (Default = `0` which means disabled): Maximum number of entries in the metadata cache. Each 1000 entries cause about 2MB memory.
`metadata_cache_expire` (Default = `10min`): Specify expire time for entries in the metadata cache

The metadata is cached in the AlluxioFuse Java process heap so make sure `metadata_cache_size * 2KB * 2 < AlluxioFuse process maximum memory allocated`.
For example, if AlluxioFuse is launched with `-Xmx=16GB` and metadata cache can use up to 8GB memory, then `metadata_cache_size` should be smaller than 4 million.

{% endnavtab %}
{% endnavtabs %}

#### Data Cache

{% navtabs dataCache %}
{% navtab Kernel Data Cache Configuration %}

FUSE has the following I/O modes controlling whether data will be cached and the cache invalidation policy:
- `direct_io`: disables the kernel data cache. Does not support in AlluxioFuse with libfuse3 yet.
- `kernel_cache`: always cache data in kernel and no cache invalidation is happening. This should only be enabled on filesystem where the file data is never changed externally (not through the current FUSE mount point)
- `auto_cache`: cache data in kernel and invalidate cache if the modification time or the size of the file has changed

Set up to one of the data cache option via mount command:
```console
$ bin/alluxio-fuse mount ufs_path mount_point -o direct_io
```

Kernel data cache will significantly improve the I/O performance but is easy to consume a large amount of node memory.
In plain machine environment, kernel memory will be reclaimed automatically when the node is under memory pressure
and will not affect the stability of AlluxioFuse process or other applications on the node.
However, in containerized environment, kernel data cache will be calculated as the container used memory.
When the container used memory exceeds the configured container maximum memory,
Kubernetes or other container management tool may kill one of the process in the container
which will cause the AlluxioFuse process to exit and the application running on top of the Alluxio FUSE mount point to fail.
To avoid this circumstances, use `direct_io` mode or use a script to cleanup the node kernel cache periodically.

{% endnavtab %}
{% navtab Userspace Data Cache Configuration %}

Userspace data cache can be enabled via 
```console
$ bin/alluxio-fuse mount ufs_path mount_point -o data_cache=<local_cache_directory> -o data_cache_size=<size>
```
`data_cache` (Default = "" which means disabled): Local folder to use for local data cache
`data_cache_size` (Default = `512MB`): Maximum cache size for local data cache directory

Data can be cached on ramdisk or disk based on the type of the cache directory.

Example of mounting S3 bucket with local userspace cache enabled:
```console
$ mkdir /tmp/local_cache
$ bin/alluxio-fuse mount s3://my_bucket/directory/ /mnt/alluxio-fuse -o s3a.accessKeyId=<S3 ACCESS KEY> -o s3a.secretKey=<S3 SECRET KEY> -o data_cache=/tmp/local_cache -o data_cache_size=5GB
# Assume s3://my_bucket/directory/ already has a test file with 1GB size
$ time cat /mnt/alluxio-fuse/testfile > /dev/null
read 0m44.817s
user 0m0.016s
sys  0m0.293s
$ time cat /mnt/alluxio-fuse/testfile > /dev/null
read 0m0.522s
user 0m0.010s
sys  0m0.346s
```
With local disk userspace data cache enabled, reading 1GB file can achieve more than 80 times faster.

{% endnavtab %}
{% endnavtabs %}

## Limitations

Most basic file system operations are supported. However, some operations are under active development

<table class="table table-striped">
    <tr>
        <td>Category</td>
        <td>Supported Operations</td>
        <td>Not Supported Operations</td>
    </tr>
    <tr>
        <td>Metadata Write</td>
        <td>Create file, delete file, create directory, delete directory, rename, change owner, change group, change mode</td>
        <td>Symlink, link, change access/modification time (utimens), change special file attributes (chattr), sticky bit</td>
    </tr>
    <tr>
        <td>Metadata Read</td>
        <td>Get file status, get directory status, list directory status</td>
        <td></td>
    </tr>
    <tr>
        <td>Data Write</td>
        <td>Sequential write</td>
        <td>Append write, random write, overwrite, truncate, concurrently write the same file by multiple threads/clients</td>
    </tr>
    <tr>
        <td>Data Read</td>
        <td>Sequential read, random read, multiple threads/clients concurrently read the same file</td>
        <td></td>
    </tr>
    <tr>
        <td>Combinations</td>
        <td></td>
        <td>FIFO special file type, Rename when writing the source file, reading and writing concurrently on the same file</td>
    </tr>
</table>

Note that all file/dir permissions are checked against the user launching the AlluxioFuse process instead of the end user running the operations.
