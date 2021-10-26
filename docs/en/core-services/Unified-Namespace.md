---
layout: global
title: Unified Namespace
nickname: Unified Namespace
group: Core Services
priority: 3
---

This page summarizes how to manage different under storage systems in the Alluxio file system
namespace.

* Table of Contents
{:toc}

## Introduction
Alluxio enables effective data management across different storage systems through its use of
transparent naming and mounting API.

### Unified Namespace
One of the key benefits that Alluxio provides is a unified namespace to the applications.
This is an abstraction that allows applications to access multiple independent storage systems
through the same namespace and interface.
Rather than communicating to each individual storage system, applications can delegate this
responsibility by connecting through Alluxio, which will handle the different underlying storage
systems.

![unified]({{ '/img/screenshot_unified.png' | relativize_url }})

The storage path specified by the URI of the master configuration property
`alluxio.master.mount.table.root.ufs` is mounted to the root of the Alluxio namespace, `/`.
This directory identifies the "primary storage" for Alluxio.
In addition, users can use the `mount` and `unmount` Java APIs to add and remove data sources:

```java
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath);
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options);
void unmount(AlluxioURI path);
void unmount(AlluxioURI path, UnmountOptions options);
```

For example, mount a S3 bucket to the `Data` directory through the Java API

```java
mount(new AlluxioURI("alluxio://host:port/Data"), new AlluxioURI("s3://bucket/directory"));
```

or use the Alluxio CLI to mount additional storage systems.

```console
$ ./bin/alluxio fs mount /mnt/new_storage s3://bucket/prefix
```

### UFS Namespace

In addition to the unified namespace Alluxio provides, each underlying file system that is mounted
in the Alluxio namespace has its own namespace; this is referred to as the _UFS namespace_.
If a file in the UFS namespace is changed without going through Alluxio,
the UFS namespace and the Alluxio namespace can potentially get out of sync.
When this happens, a [UFS Metadata Sync](#ufs-metadata-sync) operation is required to synchronize
the two namespaces.

### Transparent Naming

Transparent naming maintains an identity between the Alluxio namespace and the underlying storage
system namespace.

![transparent]({{ '/img/screenshot_transparent.png' | relativize_url }})

When a user creates objects in the Alluxio namespace, they can choose whether these objects should
be persisted in the underlying storage system.
For objects that are persisted, Alluxio preserves the object paths, relative to the underlying
storage system directory in which Alluxio objects are stored.
For instance, if a user creates a top-level directory `Users` with subdirectories `Alice`
and `Bob`, the directory structure and naming is preserved in the underlying storage system.
Similarly, when a user renames or deletes a persisted object in the Alluxio namespace,
it is renamed or deleted in the underlying storage system.

Alluxio transparently discovers content present in the underlying storage system which
was not created through Alluxio.
For instance, if the underlying storage system contains a directory `Data` with files `Reports`
and `Sales`, all of which were not created through Alluxio, their metadata will be loaded into
Alluxio the first time they are accessed, such as when a user requests to open a file.
The contents of the file is not loaded to Alluxio during this process.
To load the file contents into Alluxio, one can read the data using `FileInStream` or use
the `load` command of the Alluxio shell.
The frequency at which Alluxio will sync out-of-band changes from the UFS namespace is further 
explained in [UFS Metadata Sync](#ufs-metadata-sync).

## Mounting Under Storage Systems

Mounting an under storage system to the Alluxio file system namespace is the mechanism for
defining the association between the Alluxio namespace and the UFS namespace.
Mounting in Alluxio works similarly to mounting a volume in a Linux file system.
The `mount` command attaches a UFS to the file system tree in the Alluxio namespace.

### Root Mount Point

The root mount point of the Alluxio namespace is configured in `conf/alluxio-site.properties`
on the masters.
The following line is an example configuration where an HDFS path is mounted to the root of the
Alluxio namespace.

```
alluxio.master.mount.table.root.ufs=hdfs://HDFS_HOSTNAME:8020
```

Mount options for the root mount point are configured using the configuration prefix:

`alluxio.master.mount.table.root.option.<some alluxio property>`

For example, the following configuration adds AWS credentials for the root mount point.

```
alluxio.master.mount.table.root.option.aws.accessKeyId=<AWS_ACCESS_KEY_ID>
alluxio.master.mount.table.root.option.aws.secretKey=<AWS_SECRET_ACCESS_KEY>
```

The following configuration shows how to set other parameters for the root mount point.

```
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.principal=client
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.keytab.file=keytab
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.impersonation.enabled=true
alluxio.master.mount.table.root.option.alluxio.underfs.version=2.7
```

### Nested Mount Points

In addition to the root mount point, other under file systems can be mounted into Alluxio namespace.
These additional mount points are added to Alluxio at runtime, via the `mount` command.
The `--option` flag allows the user to pass additional parameters
to the mount operation, such as credentials.

```console
# the following command mounts an hdfs path to the Alluxio path `/mnt/hdfs`
$ ./bin/alluxio fs mount /mnt/hdfs hdfs://host1:9000/data/
# the following command mounts an s3 path to the Alluxio path `/mnt/s3` with additional options specifying the credentials
$ ./bin/alluxio fs mount \
  --option aws.accessKeyId=<accessKeyId> --option aws.secretKey=<secretKey> \
  /mnt/s3 s3://data-bucket/
```

Note that mount points can be nested as well. For example, if a UFS is mounted at
`alluxio:///path1`, another UFS could be mounted at `alluxio:///path1/path2`.

### Mount UFS with Specific Versions

Alluxio supports mounting HDFS with specified versions.
As a result, users can mount HDFS with different versions into a single Alluxio namespace. Please
refer to [HDFS Under Store]({{ '/en/ufs/HDFS.html#mount-hdfs-with-specific-versions' | relativize_url }}) for more details.

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
Alluxio keeps a fingerprint of each UFS file so that Alluxio can update the file if it changes.
The fingerprint includes information such as file size and last modified time.
If a file is modified in the UFS, Alluxio will detect this from the fingerprint, free the existing
data for that file. The next time the data is read, it will pull the newer version of the file from
the UFS. If a file is added or deleted in the UFS, Alluxio will update the metadata in its namespace
as well.

The frequency at which file and directory fingerprints are verified with the UFS is based on the
`alluxio.user.file.metadata.sync.interval` client configuration property.

For example, if a client executes an operation with the interval set to one minute,
the relevant metadata will be refreshed from the UFS if the last refresh was over a minute ago.
A value of `0` indicates that the metadata will always be synced for every operation,
whereas the default value of `-1` indicates the metadata is never synced again after the initial load.

The graph below illustrates the impact of setting the metadata sync interval on the latency of
client operations.

<p align="center">
<img style="text-align: center" src="{{ '/img/unified-namespace-metadata-sync-performance.png' | relativize_url }}" alt="Metadata sync interval"/>
</p>

The overall cost of syncing with sync interval enabled (blue line) is amortized across the number of
RPCs within the sync time interval. When the sync interval is enabled to some non-0 value users should
expect latency spikes in normal application usage. The frequency at which the spikes occur will
be directly correlated with how frequently users want to update their metadata from the UFS. When
configured to 0, users will generally see much higher RPC response times (possibly an order of
magnitude or more) greater than if they were to disable sync interval.

The reason syncing metadata is so costly is because of the latency induced by making an RPC call to
a UFS. This latency introduced by syncing is the cost of syncing metadata. Some major factors are
listed below:

* Namespace size (inodes)
	* Latency may increase with more inodes (more data to send over the wire)
* Geographical location of UFS w.r.t. Alluxio master.
	* Latency increases with larger physical separation
* UFS type and scale
	* This is best explained by an example: latency will vary between a small (< 1M inode) installation of HDFS which is co-located with Alluxio that only serves a small number of clients (1-10) and an Amazon S3 bucket that may need to serve thousands of requests/sec.

### Techniques for managing UFS sync

#### Periodic Metadata Sync

If the UFS updates at a scheduled interval, you can manually trigger the sync command after the update.
Set the sync interval to `0` by running the command:

```console
$ ./bin/alluxio fs ls -R -Dalluxio.user.file.metadata.sync.interval=0 /path/to/sync
```

#### Centralized Configuration

For clusters where jobs run with data from a frequently updated UFS,
it is inconvenient for every client to specify a sync interval.
Requests will be handled with a default sync interval if one is set in the master configuration.

In `alluxio-site.properties` on master nodes:

`alluxio.user.file.metadata.sync.interval=1m`

Note: master nodes need to be restarted to pick up configuration changes.

#### Utilizing Path Configuration

For clusters where the frequency of out-of-band changes varies based on the subtree in the
namespace,
[path configurations]({{ '/en/operation/Configuration.html#path-defaults' | relativize_url }})
should be used to set the metadata sync interval at a finer
granularity. For example, for paths which change often, a sync interval of `5 minutes` can
be set, and for paths which do not change, a sync interval of `-1` can be set.

### Metadata Sync Recommendations for Common Scenarios

#### All operations through Alluxio

If all operations are run through Alluxio without any access to the UFS from outside applications,
then we recommend users set a sync interval of -1 with a load metadata type of ONCE.

#### All operations through Alluxio except for a select few

##### HDFS UFS

With HDFS, if most operations except for some special cases are updated through Alluxio, we can
take two approaches. One is [active sync](#active-sync-for-hdfs). The other is to set the sync
interval using path configuration.

If HDFS is updated very frequently (not on the Alluxio mount point) and there is a large namespace,
it is more desirable to reduce load on the namenode by disabling active sync and simply using path
configuration instead. Otherwise, if the load on the namenode isn't a concern, use ActiveSync and
set the sync interval to -1.

##### Non-HDFS UFS

If a small number of paths are updated on regular patterns through the UFS directly instead
of Alluxio, we recommend using the path configuration feature to set the sync interval on the
specific path(s) based on the understanding of the rate of change in the UFS. This allows files to
be synced for the special cases without detrimentally affecting the performance of operations on
other parts of the namespace.

#### UFS frequently updated not through Alluxio

##### HDFS

If the updates happen across many or unknown number of parts of the namespace at a frequent rate,
use active sync in order to update metadata in a timely manner. If the rate of change is too fast,
namespace is too large, or the user does not want to update the max RPC size from the namenode, then
follow the suggestion in the section below for Non-HDFS UFS.

##### Non-HDFS UFS

Set the sync interval for each subtree or the entire cluster based on the understanding of the rate
of change in the UFS.

### Other Methods for Loading New UFS Files

We recommend to configure the sync interval exclusively. It is not recommended to consider
configuring the metadata load type. However, do note the following equivalencies

`alluxio.user.file.metadata.load.type=NEVER` is equivalent to `alluxio.user.file.metadata.sync.interval=-1`
`alluxio.user.file.metadata.load.type=ALWAYS` is equivalent to `alluxio.user.file.metadata.sync.interval=0`

If the metadata sync interval is configured the metadata load type is ignored.

### Active Sync for HDFS

In version 2.0, Alluxio introduced a new feature for maintaining synchronization between Alluxio space
and the UFS when the UFS is HDFS. The feature, called active sync, listens for HDFS events and
periodically synchronizes the metadata between the UFS and Alluxio namespace as a background task on
the master.
Because active sync feature depends on HDFS events being pushed to the Alluxio master, this feature
is only available when the UFS HDFS versions is later than 2.6.1.
You may need to change the value for `alluxio.underfs.version` in your configuration file.
Please refer to
[HDFS Under Store]({{ '/en/ufs/HDFS.html#supported-hdfs-versions' | relativize_url }}) for a list of
supported Hdfs versions.

To enable active sync on a directory, issue the following Alluxio command.

```console
$ ./bin/alluxio fs startSync /syncdir
```

You can control the active sync interval by changing the `alluxio.master.ufs.active.sync.interval` option, the default is 30 seconds.

To disable active sync on a directory, issue the following Alluxio command.

```console
$ ./bin/alluxio fs stopSync /syncdir
```

> Note: When `startSync` is issued, a full scan of the sync point is scheduled.
> If run as the Alluxio superuser, `stopSync` will interrupt any full scans which have not yet ended.
> If run as any other user, `stopSync` will wait for the full scan to finish before completing.

You can also examine which directories are currently under active sync, with the following command.

```console
$ ./bin/alluxio fs getSyncPathList
```

#### Quiet period for Active Sync

Active sync also tries to avoid syncing when the target directory is heavily used.
It tries to look for a quiet period in UFS activity to start syncing between the UFS and the Alluxio space, to avoid overloading the UFS when it is busy.
There are two configuration options that control this behavior.

`alluxio.master.ufs.active.sync.max.activities` is the maximum number of activities in the UFS directory.
Activity is a heuristic based on the exponential moving average of number of events in a directory.
For example, if a directory had 100, 10, and 1 events in the past three intervals.
Its activity would be `100/10*10 + 10/10 + 1 = 3`
`alluxio.master.ufs.active.sync.max.age` is the maximum number of intervals Alluxio will wait before
synchronizing the UFS and the Alluxio space.

The system guarantees that we will start syncing a directory if it is "quiet", or it has not been synced for a long period (period longer than the max age).

For example, the following setting

```
alluxio.master.ufs.active.sync.interval=30sec
alluxio.master.ufs.active.sync.max.activities=100
alluxio.master.ufs.active.sync.max.age=5
```

means that every 30 seconds, the system will count the number of events in the directory and
calculate its activity.
If the activity is less than 100, it will be considered a quiet period, and syncing will start
for that directory.
If the activity is greater than 100, and it has not synced for the last 5 intervals, or
5 * 30 = 150 seconds, it will start syncing the directory.
It will not perform active sync if the activity is greater than 100 and it has synced at least
once in the last 5 intervals.

## Examples

The following examples assume Alluxio is installed in the `${ALLUXIO_HOME}` directory
and an instance of Alluxio is running locally.

### Transparent Naming

Create a temporary directory in the local file system that will be used as the under storage to mount:

```console
$ cd /tmp
$ mkdir alluxio-demo
$ touch alluxio-demo/hello
```

Mount the local directory created into Alluxio namespace and verify it appears in Alluxio:

```console
$ cd ${ALLUXIO_HOME}
$ ./bin/alluxio fs mount /demo file:///tmp/alluxio-demo
Mounted file:///tmp/alluxio-demo at /demo
$ ./bin/alluxio fs ls -R /
... # note that the output should show /demo but not /demo/hello
```

Verify that the metadata for content not created through Alluxio is loaded into Alluxio the first time it is accessed:

```console
$ ./bin/alluxio fs ls /demo/hello
... # should contain /demo/hello
```

Create a file under the mounted directory and verify the file is created in the underlying file system with the same name:

```console
$ ./bin/alluxio fs touch /demo/hello2
/demo/hello2 has been created
$ ls /tmp/alluxio-demo
hello hello2
```

Rename a file in Alluxio and verify the corresponding file is also renamed in the underlying file system:

```console
$ ./bin/alluxio fs mv /demo/hello2 /demo/world
Renamed /demo/hello2 to /demo/world
$ ls /tmp/alluxio-demo
hello world
```

Delete a file in Alluxio and verify the file is also deleted in the underlying file system:

```console
$ ./bin/alluxio fs rm /demo/world
/demo/world has been removed
$ ls /tmp/alluxio-demo
hello
```

Unmount the mounted directory and verify that the directory is removed from the Alluxio namespace,
but is still preserved in the underlying file system:

```console
$ ./bin/alluxio fs unmount /demo
Unmounted /demo
$ ./bin/alluxio fs ls -R /
... # should not contain /demo
$ ls /tmp/alluxio-demo
hello
```

### Unified Namespace

This example will mount multiple under storages of different types to showcase the unified
file system namespace abstraction. This example will use two S3 buckets owned by different AWS
accounts and a HDFS service.

Mount the first S3 bucket into Alluxio using its corresponding credentials `<accessKeyId1>` and `<secretKey1>` :

```console
$ ./bin/alluxio fs mkdir /mnt
$ ./bin/alluxio fs mount \
  --option aws.accessKeyId=<accessKeyId1> \
  --option aws.secretKey=<secretKey1> \
  /mnt/s3bucket1 s3://data-bucket1/
```

Mount the second S3 bucket into Alluxio using its corresponding credentials `<accessKeyId2>` and `<secretKey2>`:

```console
$ ./bin/alluxio fs mount \
  --option aws.accessKeyId=<accessKeyId2> \
  --option aws.secretKey=<secretKey2> \
  /mnt/s3bucket2 s3://data-bucket2/
```

Mount the HDFS storage into Alluxio:

```console
$ ./bin/alluxio fs mount /mnt/hdfs hdfs://<NAMENODE>:<PORT>/
```

All three directories are all contained in one space in Alluxio:

```console
$ ./bin/alluxio fs ls -R /
... # should contain /mnt/s3bucket1, /mnt/s3bucket2, /mnt/hdfs
```

## Resources

- A blog post explaining [Unified Namespace](https://www.alluxio.io/resources/whitepapers/unified-namespace-allowing-applications-to-access-data-anywhere/)
- A blog post on [Optimizations to speed up metadata operations](https://www.alluxio.io/blog/how-to-speed-up-alluxio-metadata-operations-up-to-100x/)
