---
layout: global
title: FUSE SDK with Local Cache Quick Start
group: FUSE SDK
priority: 2
---

* Table of Contents
  {:toc}

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
        - To use with libfuse3 (Default), install [libfuse](https://github.com/libfuse/libfuse) 3.2.6 or newer (We are currently testing against 3.2.6). For example on a Redhat, run `yum install fuse3`
        - See [Select which libfuse version to use](#select-libfuse-version) to learn more about the libfuse version used by alluxio
    - On MacOS, install [osxfuse](https://osxfuse.github.io/) 3.7.1 or newer. For example, run `brew install osxfuse`

## Build and Install

Download the Alluxio tarball from [this page](https://downloads.alluxio.io/downloads/files/).
Unpack the downloaded file with the following commands.

```console
$ tar -xzf alluxio-{{site.ALLUXIO_VERSION_STRING}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_VERSION_STRING}}
```

The `alluxio-fuse` launch command will be `dora/integration/fuse/bin/alluxio-fuse`

## Mount Under Storage Dataset

Alluxio POSIX API allows accessing data from under storage as local directories.
This is enabled by using the `mount` command to mount a dataset from under storage to local mount point:
```console
$ sudo yum install fuse3
$ alluxio-fuse mount <under_storage_dataset> <mount_point> -o option
```
- `under_storage_dataset`: The full under storage dataset address. e.g. `s3://bucket_name/path/to/dataset`, `hdfs://namenode_address:port/path/to/dataset`
- `mount_point`: The local mount point to mount the under storage dataset to.
  Note that the `<mount_point>` must be an existing and empty path in your local file system hierarchy.
  User that runs the `mount` command must own the mount point and have read and write permissions on it.
- `-o option`: All the `alluxio-fuse mount` options are provided using this format. Options include
    - Alluxio property key value pair in `-o alluxio_property_key=value` format
        - Under storage credentials and configuration. Detailed configuration can be found under the `Storage Integrations` tap of the left of the doc page.
    - Local cache configuration. Detailed usage can be found in the [local cache guide]({{ '/en/fuse-sdk/Local-Cache.html' | relative_url }})
    - Generic mount options. Detailed supported mount options information can be found in the [FUSE mount options section]({{ '/en/fuse-sdk/Advanced-Tuning.html' | relative_url }})

After mounting, `alluxio-fuse` mount can be found locally
```console
$ mount | grep "alluxio-fuse"
alluxio-fuse on mount_point type fuse.alluxio-fuse (rw,nosuid,nodev,relatime,user_id=1000,group_id=1000)
```

`AlluxioFuse` process will be launched
```console
$ jps
34884 AlluxioFuse
```

All the fuse logs can be found at `logs/fuse.log` and all the fuse outputs can be found at `logs/fuse.out` which are
useful for troubleshooting when errors happen on operations under the filesystem.

### Example: Mounts a S3 dataset

Mounts the dataset in target S3 bucket to a local folder:
```console
$ alluxio-fuse mount s3://bucket_name/path/to/dataset/ /path/to/mount_point -o s3a.accessKeyId=<S3 ACCESS KEY> -o s3a.secretKey=<S3 SECRET KEY>
```
Other [S3 configuration]({{ '/en/ufs/S3.html' | relative_url }}#advanced-credentials-setup) (e.g. `-o alluxio.underfs.s3.region=<region>`) can also be set via the `-o alluxio_property_key=value` format.

### Example: Mounts a HDFS dataset

Mounts the dataset in target HDFS cluster to a local folder:
```console
$ alluxio-fuse mount hdfs://nameservice/path/to/dataset /path/to/mount_point -o alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/core-site.xml:/path/to/hdfs/conf/hdfs-site.xml
```
The supported versions of HDFS can be specified via `-o alluxio.underfs.version=2.7` or `-o alluxio.underfs.version=3.3`.
Other [HDFS configuration]({{ '/en/ufs/HDFS.html' | relative_url }}) can also be set via the `-o alluxio_property_key=value` format.

## Example: Run operations

After mounting the dataset from under storage to local mount point,
standard tools (for example, `ls`, `cat` or `mkdir`) have basic access
to the under storage. With the POSIX API integration, applications can interact with the remote under storage no
matter what language (C, C++, Python, Ruby, Perl, or Java) they are written in without any under storage
library integrations.

### Write Through to Mounted Under Storage Dataset

All the write operations happening inside the local mount point will be directly
translated to write operations against the mounted under storage dataset
```console
$ cd /path/to/mount_point
$ mkdir testfolder
$ dd if=/dev/zero of=testfolder/testfile bs=5MB count=1
```

`folder` will be directly created at `under_storage_dataset/testfolder` (e.g. `s3://bucket_name/path/to/dataset/testfolder`
`testfolder/testfile` will be directly written to `under_storage_dataset/testfolder/testfile` (e.g. `s3://bucket_name/path/to/dataset/testfolder/testfile`

### Read Through from Mounted Under Storage Dataset

Without the [local cache]]({{ '/en/fuse-sdk/Local-Cache.html' | relative_url }}) functionalities that we will talk about later, all the read operations
via the local mount point will be translated to read operations against the underlying data storage:
```console
$ cd /path/to/mount_point
$ cp -r /path/to/mount_point/testfolder /tmp/
$ ls /tmp/testfolder
-rwx------.  1 ec2-user ec2-user 5000000 Nov 22 00:27 testfile
```
The read from `/path/to/mount_point/testfolder` will be translated to a read targeting `under_storage_dataset/testfolder/testfile` (e.g. `s3://bucket_name/path/to/dataset/testfolder/testfile`.
Data will be read from the under storage dataset directly.

## Unmount

Unmount a mounted FUSE mount point
```console
$ alluxio-fuse unmount <mount_point>
```
After unmounting the FUSE mount point, the corresponding `AlluxioFuse` process should be killed
and the mount point should be removed. For example:
```console
$ alluxio-fuse unmount /path/to/mount_point
$ mount | grep "alluxio-fuse"
$ jps | grep "AlluxioFuse"
```

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
