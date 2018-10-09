---
layout: global
title: Namespace Management
nickname: Namespace Management
group: Advanced
priority: 0
---

* Table of Contents
{:toc}

# Introduction
Alluxio enables effective data management across different storage systems through its use of transparent naming and mounting API.

## Unified namespace
One of the key benefit that Alluxio provides is a unified namespace to the applications.
This is an abstraction that allows applications to access multiple independent storage systems through the same name space and interface.
Applications simply communicate with Alluxio and Alluxio interacts with the different underlying storage systems.

![unified]({{ site.baseurl }}/img/screenshot_unified.png)

The directory specified by the `alluxio.underfs.address` property of Alluxio configuration is mounted to the root of the Alluxio namespace.
This directory identifies the "primary storage" for Alluxio.
In addition, users can use the mounting API to add new and remove
existing data sources:

```java
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath);
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options);
void unmount(AlluxioURI path);
void unmount(AlluxioURI path, UnmountOptions options);
```

For example, the primary storage could be HDFS and contain user directories; the `Data` directory
might be stored in an S3 bucket, which is mounted to the Alluxio namespace through
```java
mount(new AlluxioURI("alluxio://host:port/Data"), new AlluxioURI("s3a://bucket/directory"));
```


## UFS namespace
In addition to the unified namespace Alluxio provides, each underlying file system that is mounted in Alluxio namespace has its own namespace.
We call this the UFS namespace. In the example provided above, we have two UFS namespaces, one for the S3 under storage and one for the HDFS under storage. 
When the user changes the file in the UFS namespace without going through Alluxio, the UFS namespace and the Alluxio namespace can potentially get out of sync.
When this happens, a [UFS Metadata Sync](#Ufs-Metadata-Sync) operation is required to synchronize between the Alluxio namespace and the UFS namespace. 
 
## Transparent Naming

Transparent naming maintains an identity between the Alluxio namespace and the underlying storage
system namespace.

![transparent]({{ site.baseurl }}/img/screenshot_transparent.png)

When a user creates objects in the Alluxio namespace, they can choose whether these objects should
be persisted in the underlying storage system. For objects that are persisted, Alluxio preserves the
object paths, relative to the underlying storage system directory in which Alluxio objects are
stored. For instance, if a user creates a top-level directory `Users` with subdirectories `Alice`
and `Bob`, the directory structure and naming is preserved in the underlying storage system (e.g.
HDFS or S3). Similarly, when a user renames or deletes a persisted object in the Alluxio namespace,
it is renamed or deleted in the underlying storage system.

Furthermore, Alluxio transparently discovers content present in the underlying storage system which
was not created through Alluxio. For instance, if the underlying storage system contains a directory
`Data` with files `Reports` and `Sales`, all of which were not created through Alluxio, their
metadata will be loaded into Alluxio the first time they are accessed (e.g. when a user requests to
open a file). The data of the file is not loaded to Alluxio during this process. To load the data into
Alluxio, one can read the data using `FileInStream` or use the `load` command of the Alluxio shell.


# Mounting Under File System (UFS)
Mounting in Alluxio works similarly to mounting a volume in a Linux file system.
The mount command attaches a UFS to the file system tree in the Alluxio namespace. 

## Root Mount Point
The root mount point of the Alluxio namespace can be specified by adding the following line to the configuration file in `conf/alluxio-site.properties`.
Below is an example of configuration having an HDFS path to be the root of Alluxio namespace.

```
alluxio.underfs.address=hdfs://HDFS_HOSTNAME:9000
``` 

If the root mount point requires additional parameters such as credentials, it can be specified in the configuration file as well. 

```
aws.accessKeyId=<AWS_ACCESS_KEY_ID>
aws.secretKey=<AWS_SECRET_ACCESS_KEY>
```

## Nested Mount Points
In addition to the root mount point, other under filesystems can be mounted into Alluxio namespace by using the mount command. Using the `--option` flag, the user can pass additional parameters to the mount operation, such as credentials for S3 storage. 

```bash
$ ./bin/alluxio fs mount /mnt/hdfs hdfs://host1:9000/data/
$ ./bin/alluxio fs mount --option aws.accessKeyId=<accessKeyId> --option aws.secretKey=<secretKey>
  /mnt/s3 s3a://data-bucket/

```

The result of this mount command is that the ufs path will be attached to the Alluxio namespace at the designated location.
Note that mount points can be nested as well. For example, we can mount one UFS at  `alluxio:///path1`, and then mount another UFS at  `alluxio:///path1/path2`

# Relationship Between Alluxio Namespace and UFS Namespace
Alluxio provides a unified namespace, acting like a cache for data in one or more
under file storage (UFS) systems. This section discusses how Alluxio interacts with
UFSes to discover UFS files and make them available through Alluxio.

Alluxio aims to be transparent. That is, accessing UFS files through Alluxio
should behave the same as accessing them directly through the UFS. For example,
if the root UFS is `s3://bucket/data`, listing `alluxio:///` should show the
same result as listing `s3://bucket/data`, and running `cat` on `alluxio:///file`
should show the same result as running `cat` on `s3://bucket/data/file`.

To efficiently achieve this transparency, Alluxio lazily loads metadata from the
UFS. In the above example, Alluxio will not know about `s3://bucket/data/file` at
startup. `file` will only be discovered when the user lists `alluxio:///` or tries
to cat `alluxio:///file`. This makes mounting a new UFS very cheap, and prevents
unnecessary work.

By default, *Alluxio expects that all modifications to UFSes happen through Alluxio*.
This allows Alluxio to only scan each UFS directory a single time, significantly
improving performance when UFS metadata operations are slow. As long as all UFS
changes go through Alluxio, this has no user-facing impact.
However, it is sometimes necessary for Alluxio to handle out of band UFS changes.
That is where the metadata sync feature comes into play.

## UFS Metadata Sync

> The UFS metadata sync feature has been available since version `1.7.0`.

When Alluxio scans a UFS directory and loads metadata for its sub-paths, it
creates a cached copy of the metadata so that future operations don't need to
hit the UFS. By default the cached copy lives forever, but you can configure the
cache timeout with the `alluxio.user.file.metadata.sync.interval` configuration
property. This is a client-side property, so when a client makes a request, it
can specify `alluxio.user.file.metadata.sync.interval=1m` to say "when running
this operation, make sure all relevant metadata has been synced within the last
1 minute". Setting a value of `0` means that metadata will always be synced. The
default value of `-1` means to never re-sync metadata.

Low values for `alluxio.user.file.metadata.sync.interval` allow Alluxio clients
to quickly observe out of band UFS changes, but increase the number of calls to
the UFS, decreasing RPC response time performance.

Metadata sync keeps a fingerprint of each UFS file so that Alluxio can re-sync
the file if it changes. The fingerprint includes information such as file size
and last modified time. If a file is modified in the UFS, Alluxio
will detect this from the fingerprint, free the existing data for that file, and
re-load the metadata for the updated file. If a file is deleted in the UFS, Alluxio
will delete the file from Alluxio's namespace as well. Lastly, Alluxio will detect
newly added files and make them available to Alluxio clients.

## Techniques for managing UFS sync

### Daily Metadata Sync

If your UFS is only updated once a day, you can run

`alluxio fs ls -R -Dalluxio.user.file.metadata.sync.interval=0 /`

after the update, then use the default `-1` for the rest of the day to avoid calls
to the UFS.

### Centralized Configuration

For clusters where the UFS is often changing and jobs needs to see the updates,
it can be inconvenient for every client to need to specify a sync interval. To
address this, set a default sync interval on the master, and all requests will
use that sync interval by default.

In masters' `alluxio-site.properties`:

`alluxio.user.file.metadata.sync.interval=1m`

Note that master needs to be restarted to pick up configuration changes.

## Other Methods for Loading New UFS Files

The UFS sync discussed previously is the recommended method for loading files from
the UFS. There are a couple other methods mentioned here for completeness.

`alluxio.user.file.metadata.load.type`: This property can be set to either
`ALWAYS`, `ONCE`, or `NEVER`. It acts similar to
`alluxio.user.file.metadata.sync.interval`, but with two caveats: (1) It only
discovers new files, and doesn't re-load UFS-modified files or remove UFS-deleted
files, and (2) it only applies to the `exists`, `list`, and `getStatus` RPCs.
`ALWAYS` will always check the UFS for new files, `ONCE` will use the default
behavior of only scanning each directory once ever, and `NEVER` will prevent Alluxio
from scanning for new files at all.

`alluxio fs ls -f /path`: The `-f` option to `ls` acts the same as setting
`alluxio.user.file.metadata.load.type` to `ALWAYS`. It discovers new files, but
doesn't detect modified or deleted UFS files. For that, instead pass the
`-Dalluxio.user.file.metadata.sync.interval=0` option to `ls`.

# Examples

The following examples assume that Alluxio source code
exists in the `${ALLUXIO_HOME}` directory and that there is an instance of Alluxio running locally.

### Transparent Naming

In this example, we will showcase how Alluxio provides transparent naming between files in Alluxio space and under storage.

First, let's create a temporary directory in the local file system that will be used as the under storage to mount for the example:

```bash
$ cd /tmp
$ mkdir alluxio-demo
$ touch alluxio-demo/hello
```

Next, let's mount the local directory `/tmp/alluxio-demo` created above into Alluxio namespace and
verify the mounted directory appears in Alluxio:

```bash
$ cd ${ALLUXIO_HOME}
$ bin/alluxio fs mount /demo file:///tmp/alluxio-demo
Mounted file:///tmp/alluxio-demo at /demo
$ bin/alluxio fs ls -R /
... # should contain /demo but not /demo/hello
```

Next, let's verify that the metadata for content not created through Alluxio is loaded into Alluxio
the first time the content is accessed:

```bash
$ bin/alluxio fs ls /demo/hello
... # should contain /demo/hello
```

Next, create a file under the mounted directory and verify the file is created in the underlying file system with the same name:

```bash
$ bin/alluxio fs touch /demo/hello2
/demo/hello2 has been created
$ bin/alluxio fs persist /demo/hello2
persisted file /demo/hello2 with size 0
$ ls /tmp/alluxio-demo
hello hello2
```

Next, rename a file in Alluxio and verify the file is also renamed correspondingly in the
underlying file system:

```bash
$ bin/alluxio fs mv /demo/hello2 /demo/world
Renamed /demo/hello2 to /demo/world
$ ls /tmp/alluxio-demo
hello world
```

Next, delete a file in Alluxio and verify the file is also deleted in the underlying file system:

```bash
$ bin/alluxio fs rm /demo/world
/demo/world has been removed
$ ls /tmp/alluxio-demo
hello
```

Finally, unmount the mounted directory and verify that the directory is removed from the
Alluxio namespace, but its content is preserved in the underlying file system:

```bash
$ bin/alluxio fs unmount /demo
Unmounted /demo
$ bin/alluxio fs ls -R /
... # should not contain /demo
$ ls /tmp/alluxio-demo
hello
```

### Unified Namespace

In this example, we will showcase how Alluxio provides a unified file system namespace abstraction
across multiple under storages of different types. Particularly, we have one HDFS service and two
S3 buckets from different AWS accounts.

First, mount the first S3 bucket into Alluxio using credential `<accessKeyId1>` and `<secretKey1>` :

```java
$ bin/alluxio fs mkdir /mnt
$ bin/alluxio fs mount --option aws.accessKeyId=<accessKeyId1> --option aws.secretKey=<secretKey1>  /mnt/s3bucket1 s3a://data-bucket1/
```

Next, mount the second S3 bucket into Alluxio using possibly a different set of credential `<accessKeyId2>` and `<secretKey2>`:

```java
$ bin/alluxio fs mount --option aws.accessKeyId=<accessKeyId2> --option aws.secretKey=<secretKey2>  /mnt/s3bucket2 s3a://data-bucket2/
```

Finally, mount the HDFS storage also into Alluxio:

```java
$ bin/alluxio fs mount /mnt/hdfs hdfs://<NAMENODE>:<PORT>/
```

Now these different directories are all contained in one space from Alluxio:

```bash
$ bin/alluxio fs ls -R /
... # should contain /mnt/s3bucket1, /mnt/s3bucket2, /mnt/hdfs
```


## Resources

[Unified Namespace Blog Post](http://www.alluxio.com/2016/04/unified-namespace-allowing-applications-to-access-data-anywhere/)
