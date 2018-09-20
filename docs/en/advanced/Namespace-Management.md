---
layout: global
title: Namespace Management
nickname: Namespace Management
group: Advanced
priority: 0
---

* Table of Contents
{:toc}

TODO: new outline

Intro: Introduce concepts such as:
- Alluxio's unified namespace
- UFS namespace
- transparent naming

# Mounting UFS
  explain what mounting is and the types of mount points
  what happens and does not happen at mount time
## Root Mount Point
  include example
## Nested Mount Points
  include example
# Ufs Metadata Sync
  this section is probably the contents from Loading-Under-File-Storage-Metadata
# Examples
  reuse existing examples

Alluxio enables effective data management across different storage systems through its use of
transparent naming and mounting API.

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

## Unified Namespace

Alluxio provides a mounting API that makes it possible to use Alluxio to access data across multiple
data sources.

![unified]({{ site.baseurl }}/img/screenshot_unified.png)

By default, Alluxio namespace is mounted onto the directory specified by the
`alluxio.underfs.address` property of Alluxio configuration; this directory identifies the
"primary storage" for Alluxio. In addition, users can use the mounting API to add new and remove
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

## Examples

The following examples assume that Alluxio source code
exists in the `${ALLUXIO_HOME}` directory and that there is an instance of Alluxio running locally.

### Transparent Naming

In this example, we will showcase how Alluxio provides transparent naming between files in Alluxio space and under storage.

First, let's create a temporary directory in the local file system that will be used as the under stoage to mount for the example:

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
