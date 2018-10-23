---
layout: global
title: Namespace Management
nickname: Namespace Management
group: Advanced
priority: 0
---

This page summarizes how to manage different under storage systems in Alluxio namespace.

* Table of Contents
{:toc}

## Introduction
Alluxio enables effective data management across different storage systems through its use of
transparent naming and mounting API.

### Unified namespace
One of the key benefits that Alluxio provides is a unified namespace to the applications.
This is an abstraction that allows applications to access multiple independent storage systems
through the same namespace and interface.
Rather than communicating to each individual storage system, applications can delegate this
responsibility by connecting through Alluxio, which will handle the different underlying storage
systems.

![unified]({{ site.baseurl }}{% link img/screenshot_unified.png %})

The directory specified by the `alluxio.underfs.address` is mounted to the root of the Alluxio
namespace. This directory identifies the "primary storage" for Alluxio.
In addition, users can use the mounting API to add and remove data sources:

```java
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath);
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options);
void unmount(AlluxioURI path);
void unmount(AlluxioURI path, UnmountOptions options);
```

For example, mount a S3 bucket to the `Data` directory through
```java
mount(new AlluxioURI("alluxio://host:port/Data"), new AlluxioURI("s3a://bucket/directory"));
```

### UFS namespace
In addition to the unified namespace Alluxio provides, each underlying file system that is mounted
in Alluxio namespace has its own namespace; this is referred to as the UFS namespace.
If a file in the UFS namespace is changed without going through Alluxio,
the UFS namespace and the Alluxio namespace can potentially get out of sync.
When this happens, a [UFS Metadata Sync](#Ufs-Metadata-Sync) operation is required to synchronize
the two namespaces.

### Transparent Naming

Transparent naming maintains an identity between the Alluxio namespace and the underlying storage
system namespace.

![transparent]({{ site.baseurl }}{% link img/screenshot_transparent.png %})

When a user creates objects in the Alluxio namespace, they can choose whether these objects should
be persisted in the underlying storage system. For objects that are persisted, Alluxio preserves the
object paths, relative to the underlying storage system directory in which Alluxio objects are
stored. For instance, if a user creates a top-level directory `Users` with subdirectories `Alice`
and `Bob`, the directory structure and naming is preserved in the underlying storage system.
Similarly, when a user renames or deletes a persisted object in the Alluxio namespace,
it is renamed or deleted in the underlying storage system.

Alluxio transparently discovers content present in the underlying storage system which
was not created through Alluxio. For instance, if the underlying storage system contains a directory
`Data` with files `Reports` and `Sales`, all of which were not created through Alluxio, their
metadata will be loaded into Alluxio the first time they are accessed, such as when a user requests to
open a file. The data of the file is not loaded to Alluxio during this process. To load the data into
Alluxio, one can read the data using `FileInStream` or use the `load` command of the Alluxio shell.


## Mounting Under Storage Systems
Mounting in Alluxio works similarly to mounting a volume in a Linux file system.
The mount command attaches a UFS to the file system tree in the Alluxio namespace. 

### Root Mount Point
The root mount point of the Alluxio namespace can be specified in `conf/alluxio-site.properties`.
The following line is an example configuration where a HDFS path is at the root of the Alluxio namespace.

```
alluxio.underfs.address=hdfs://HDFS_HOSTNAME:9000
``` 

Mount options for the root mount point can be configured using the configuration prefix:

`alluxio.master.mount.table.root.option.<some alluxio property>`

The following configuration adds AWS credentials for the root mount point.

```
alluxio.master.mount.table.root.option.aws.accessKeyId=<AWS_ACCESS_KEY_ID>
alluxio.master.mount.table.root.option.aws.secretKey=<AWS_SECRET_ACCESS_KEY>
```

The following configuration shows how to set other parameters for the root mount point. 

```
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.principal=client
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.keytab.file=keytab
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.impersonation.enabled=true
alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.version=hdp-2.6
```

### Nested Mount Points
In addition to the root mount point, other under file systems can be mounted into Alluxio namespace
by using the mount command. The `--option` flag allows the user to pass additional parameters
to the mount operation, such as credentials for S3 storage. 

```bash
$ ./bin/alluxio fs mount /mnt/hdfs hdfs://host1:9000/data/
$ ./bin/alluxio fs mount --option aws.accessKeyId=<accessKeyId> --option aws.secretKey=<secretKey>
  /mnt/s3 s3a://data-bucket/
```

Note that mount points can be nested as well. For example, if a UFS is mounted at
`alluxio:///path1`, another UFS could be mounted at `alluxio:///path1/path2`.

## Relationship Between Alluxio and UFS Namespace
Alluxio provides a unified namespace, acting as a cache for data in one or more
under file storage systems. This section discusses how Alluxio interacts with
under file systems to discover their files and make them available through Alluxio.

Accessing UFS files through Alluxio should behave the same as accessing them directly through the UFS.
If the root UFS is `s3://bucket/data`, listing `alluxio:///` should show the same result as listing `s3://bucket/data`.
Running `cat` on `alluxio:///file` should show the same result as running `cat` on `s3://bucket/data/file`.

Alluxio loads metadata from the UFS as needed.
In the above example, Alluxio does not have information about `s3://bucket/data/file` at startup.
The file will be discovered when the user lists `alluxio:///` or tries to cat `alluxio:///file`.
This prevents unnecessary work when mounting a new UFS.

By default, *Alluxio expects that all modifications to under file systems occur through Alluxio*.
This allows Alluxio to only scan each UFS directory once, significantly improving performance when
UFS metadata operations are slow.
In the scenario where changes are made to the UFS outside of Alluxio,
the metadata sync feature is used to synchronize the two namespaces.

### UFS Metadata Sync

> The UFS metadata sync feature has been available since version `1.7.0`.

When Alluxio scans a UFS directory and loads metadata for its sub-paths,
it creates a copy of the metadata so that future operations do not need to load from the UFS. 
By default, the cached copy lives forever but its lifetime can be configured using the
`alluxio.user.file.metadata.sync.interval` property.
This property applies to client side operations.
For example, if a client executes an operation with the interval set to one minute,
all relevant metadata must be synced within the past minute.
A value of `0` indicates that the metadata will always be synced for every operation,
whereas the default value of `-1` indicates the metadata is never synced again after the initial load.

Low interval values allow Alluxio clients to quickly discover external modifications to the UFS,
at the cost of decreasing performance since the number of calls to the UFS increases.

Metadata sync keeps a fingerprint of each UFS file so that Alluxio can update the file if it changes.
The fingerprint includes information such as file size and last modified time.
If a file is modified in the UFS, Alluxio will detect this from the fingerprint, free the existing
data for that file, and reload the metadata for the updated file.
If a file is added or deleted in the UFS, Alluxio will update the metadata in its namespace as well.

### Techniques for managing UFS sync

#### Periodic Metadata Sync

If the UFS updates at a scheduled interval, you can manually trigger the sync command after the update.
Set the sync interval to `0` by running the command: 

```bash
$ alluxio fs ls -R -Dalluxio.user.file.metadata.sync.interval=0
```

Then reset the sync interval back to the default value of `-1`,
since it is known that there will be no further external changes to the UFS until the next update.

#### Centralized Configuration

For clusters where jobs run with data from a frequently updated UFS,  
it is inconvenient for every client to specify a sync interval.
Requests will be handled with a default sync interval if one is set in the master configuration.

In `alluxio-site.properties` on master nodes:

`alluxio.user.file.metadata.sync.interval=1m`

Note master nodes need to be restarted to pick up configuration changes.

### Other Methods for Loading New UFS Files

The UFS sync discussed previously is the recommended method for loading files from the UFS.
Here are some other methods for loading files:

* `alluxio.user.file.metadata.load.type`: This property can be set to either
`ALWAYS`, `ONCE`, or `NEVER`. It acts similar to `alluxio.user.file.metadata.sync.interval`,
but with two caveats: 
    1. It only discovers new files and does not reload modified or deleted files
    1. It only applies to the `exists`, `list`, and `getStatus` RPCs
`ALWAYS` will always check the UFS for new files, `ONCE` will use the default
behavior of only scanning each directory once ever, and `NEVER` will prevent Alluxio
from scanning for new files at all.

* `alluxio fs ls -f /path`: The `-f` option to `ls` acts the same as setting
`alluxio.user.file.metadata.load.type` to `ALWAYS`. It discovers new files but
does not detect modified or deleted UFS files.
The only way to detect these types of changes is to pass the
`-Dalluxio.user.file.metadata.sync.interval=0` option to `ls`.

## Examples

The following examples assume that Alluxio source code exists in the `${ALLUXIO_HOME}` directory
and an instance of Alluxio is running locally.

### Transparent Naming

Create a temporary directory in the local file system that will be used as the under storage to mount:

```bash
$ cd /tmp
$ mkdir alluxio-demo
$ touch alluxio-demo/hello
```

Mount the local directory created into Alluxio namespace and verify it appears in Alluxio:

```bash
$ cd ${ALLUXIO_HOME}
$ bin/alluxio fs mount /demo file:///tmp/alluxio-demo
Mounted file:///tmp/alluxio-demo at /demo
$ bin/alluxio fs ls -R /
... # note that the output should show /demo but not /demo/hello
```

Verify that the metadata for content not created through Alluxio is loaded into Alluxio the first time it is accessed:

```bash
$ bin/alluxio fs ls /demo/hello
... # should contain /demo/hello
```

Create a file under the mounted directory and verify the file is created in the underlying file system with the same name:

```bash
$ bin/alluxio fs touch /demo/hello2
/demo/hello2 has been created
$ bin/alluxio fs persist /demo/hello2
persisted file /demo/hello2 with size 0
$ ls /tmp/alluxio-demo
hello hello2
```

Rename a file in Alluxio and verify the corresponding file is also renamed in the underlying file system:

```bash
$ bin/alluxio fs mv /demo/hello2 /demo/world
Renamed /demo/hello2 to /demo/world
$ ls /tmp/alluxio-demo
hello world
```

Delete a file in Alluxio and verify the file is also deleted in the underlying file system:

```bash
$ bin/alluxio fs rm /demo/world
/demo/world has been removed
$ ls /tmp/alluxio-demo
hello
```

Unmount the mounted directory and verify that the directory is removed from the Alluxio namespace,
but is still preserved in the underlying file system:

```bash
$ bin/alluxio fs unmount /demo
Unmounted /demo
$ bin/alluxio fs ls -R /
... # should not contain /demo
$ ls /tmp/alluxio-demo
hello
```

### Unified Namespace

This example will mount multiple under storages of different types to showcase the unified 
file system namespace abstraction. This example will use two S3 buckets owned by different AWS
accounts and a HDFS service.

Mount the first S3 bucket into Alluxio using its corresponding credentials `<accessKeyId1>` and `<secretKey1>` :

```java
$ bin/alluxio fs mkdir /mnt
$ bin/alluxio fs mount --option aws.accessKeyId=<accessKeyId1> --option aws.secretKey=<secretKey1>  /mnt/s3bucket1 s3a://data-bucket1/
```

Mount the second S3 bucket into Alluxio using its corresponding credentials `<accessKeyId2>` and `<secretKey2>`:

```java
$ bin/alluxio fs mount --option aws.accessKeyId=<accessKeyId2> --option aws.secretKey=<secretKey2>  /mnt/s3bucket2 s3a://data-bucket2/
```

Mount the HDFS storage into Alluxio:

```java
$ bin/alluxio fs mount /mnt/hdfs hdfs://<NAMENODE>:<PORT>/
```

All three directories are all contained in one space in Alluxio:

```bash
$ bin/alluxio fs ls -R /
... # should contain /mnt/s3bucket1, /mnt/s3bucket2, /mnt/hdfs
```


## Resources

- A blog post explaining [Unified Namespace](http://www.alluxio.com/2016/04/unified-namespace-allowing-applications-to-access-data-anywhere/)
- A blog post on [Optimizations to speed up metadata operations](https://www.alluxio.com/blog/how-to-speed-up-alluxio-metadata-operations-up-to-100x)