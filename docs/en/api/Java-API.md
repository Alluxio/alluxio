---
layout: global
title: Java API
nickname: Java API
group: Client APIs
priority: 0
---

Applications primarily interact with Alluxio through its File System API. Java users
can either use the [Alluxio Java Client](#java-client), or the
[Hadoop-Compatible Java Client](#hadoop-compatible-java-client), which
wraps the Alluxio Java Client to implement the Hadoop API.

Another common option is through Alluxio 
[S3 API]({{ '/en/api/S3-API.html' | relativize_url }}). 
Users can interact with Alluxio using the same S3 clients used for AWS S3 operations. 
This makes it easy to change existing S3 workloads to use Alluxio.

Alluxio also provides a [POSIX API]({{ '/en/api/POSIX-API.html' | relativize_url }}) after mounting
Alluxio as a local FUSE volume. Right now Alluxio POSIX API mainly targets the ML/AI workloads (especially read heavy workloads).

By setting up an Alluxio Proxy, users can also interact with Alluxio through 
[REST API]({{ '/en/api/REST-API.html' | relativize_url }}) for operations 
[S3 API]({{ '/en/api/S3-API.html' | relativize_url }}) doesn't support. 
This is mainly for admin actions not supported by S3 API. For example, mount and unmount operations.
The REST API is currently used for the Go and Python language bindings.

* Table of Contents
{:toc}

## Java Client

Alluxio provides access to data through a file system interface. Files in Alluxio offer write-once
semantics: they become immutable after they have been written in their entirety and cannot be read
before being completed.
Alluxio provides users with two different File System APIs to access the same file system:

1. [Alluxio file system API](#alluxio-java-api) and
1. [Hadoop compatible file system API](#hadoop-compatible-java-client)

The Alluxio file system API provides full functionality, while the Hadoop compatible API
gives users the flexibility of leveraging Alluxio without having to modify existing code written
using Hadoop's API with limitations.

### Configuring Dependency

To build your Java application to access Alluxio File System using [maven](https://maven.apache.org/),
include the artifact `alluxio-shaded-client` in your `pom.xml` like the following:

```xml
<dependency>
  <groupId>org.alluxio</groupId>
  <artifactId>alluxio-shaded-client</artifactId>
  <version>{{site.ALLUXIO_VERSION_STRING}}</version>
</dependency>
```

Available since `2.0.1`, this artifact is self-contained by including all its
transitive dependencies in a shaded form to prevent potential dependency conflicts.
This artifact is recommended generally for a project to use Alluxio client.

Alternatively, an application can also depend on the `alluxio-core-client-fs` artifact for
the [Alluxio file system interface](#alluxio-java-api)
or the `alluxio-core-client-hdfs` artifact for the
[Hadoop compatible file system interface](#hadoop-compatible-java-client) of Alluxio.
These two artifacts do not include transitive dependencies and therefore much smaller in size.
They also both include in `alluxio-shaded-client` artifact.

### Alluxio Java API

This section introduces the basic operations to use the Alluxio `FileSystem` interface.
Read the [javadoc](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/file/FileSystem.html)
for the complete list of API methods.
All resources with the Alluxio Java API are specified through an
[AlluxioURI](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/AlluxioURI.html)
which represents the path to the resource.

#### Getting a File System Client

To obtain an Alluxio File System client in Java code, use
[`FileSystem.Factory#get()`](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/file/FileSystem.Factory.html#get--):

```java
FileSystem fs = FileSystem.Factory.get();
```

#### Creating a File

All metadata operations as well as opening a file for reading or creating a file for writing are
executed through the `FileSystem` object. Since Alluxio files are immutable once written, the
idiomatic way to create files is to use
[`FileSystem#createFile(AlluxioURI)`](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/file/FileSystem.html#createFile-alluxio.AlluxioURI-),
which returns a stream object that can be used to write the file. For example:

> Note: there are some file path name limitation when creating files through Alluxio. Please check [Alluxio limitations]({{ '/en/administration/Troubleshooting.html' | relativize_url }}#file-path-limitations)

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Create a file and get its output stream
FileOutStream out = fs.createFile(path);
// Write data
out.write(...);
// Close and complete file
out.close();
```

#### Accessing an existing file in Alluxio

All operations on existing files or directories require the user to specify the `AlluxioURI`.
An `AlluxioURI` can be used to perform various operations, such as modifying the file
metadata (i.e. TTL or pin state) or getting an input stream to read the file.

#### Reading Data

Use [`FileSystem#openFile(AlluxioURI)`](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/file/FileSystem.html#openFile-alluxio.AlluxioURI-)
to obtain a stream object that can be used to read a file. For example:

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Open the file for reading
FileInStream in = fs.openFile(path);
// Read data
in.read(...);
// Close file relinquishing the lock
in.close();
```

#### Specifying Operation Options

For all `FileSystem` operations, an additional `options` field may be specified, which allows
users to specify non-default settings for the operation. For example:

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Generate options to set a custom blocksize of 64 MB
CreateFilePOptions options = CreateFilePOptions
                              .newBuilder()
                              .setBlockSizeBytes(64 * Constants.MB)
                              .build();
FileOutStream out = fs.createFile(path, options);
```

#### Programmatically Modifying Configuration

Alluxio configuration can be set through `alluxio-site.properties`, but these properties apply to
all instances of Alluxio that read from the file. If fine-grained configuration management is
required, pass in a customized configuration object when creating the `FileSystem` object.
The generated `FileSystem` object will have modified configuration properties, independent of any
other `FileSystem` clients.

```java
FileSystem normalFs = FileSystem.Factory.get();
AlluxioURI normalPath = new AlluxioURI("/normalFile");
// Create a file with default properties
FileOutStream normalOut = normalFs.createFile(normalPath);
...
normalOut.close();

// Create a file system with custom configuration
InstancedConfiguration conf = InstancedConfiguration.defaults();
conf.set(PropertyKey.SECURITY_LOGIN_USERNAME, "alice");
FileSystem customizedFs = FileSystem.Factory.create(conf);
AlluxioURI customizedPath = new AlluxioURI("/customizedFile");
// The newly created file will be created under the username "alice"
FileOutStream customizedOut = customizedFs.createFile(customizedPath);
...
customizedOut.close();

// normalFs can still be used as a FileSystem client with the default username.
// Likewise, using customizedFs will use the username "alice".
```

#### IO Options

Alluxio uses two different storage types: Alluxio managed storage and under storage. Alluxio managed
storage is the memory, SSD, and/or HDD allocated to Alluxio workers. Under storage is the storage
resource managed by the underlying storage system, such as S3, Swift or HDFS. Users can specify the
interaction with Alluxio managed storage and under storage through `ReadType` and `WriteType`.
`ReadType` specifies the data read behavior when reading a file. `WriteType` specifies the data
write behavior when writing a new file, i.e. whether the data should be written in Alluxio Storage.

Below is a table of the expected behaviors of `ReadType`. Reads will always prefer Alluxio storage
over the under storage.

<table class="table table-striped">
<tr><th>Read Type</th><th>Behavior</th>
</tr>
{% for readtype in site.data.table.ReadType %}
<tr>
  <td>{{readtype.readtype}}</td>
  <td>{{site.data.table.en.ReadType[readtype.readtype]}}</td>
</tr>
{% endfor %}
</table>

Below is a table of the expected behaviors of `WriteType`.

<table class="table table-striped">
<tr><th>Write Type</th><th>Behavior</th>
</tr>
{% for writetype in site.data.table.WriteType %}
<tr>
  <td>{{writetype.writetype}}</td>
  <td>{{site.data.table.en.WriteType[writetype.writetype]}}</td>
</tr>
{% endfor %}
</table>

#### Location policy

Alluxio provides location policy to choose which workers to store the blocks of a file.

Using Alluxio's Java API, users can set the policy in `CreateFilePOptions` for writing files and
`OpenFilePOptions` for reading files into Alluxio.

Users can override the default policy class in the
[configuration file]({{ '/en/operation/Configuration.html' | relativize_url }}).
Two configuration properties are available:

1. `alluxio.user.ufs.block.read.location.policy`
   This controls which worker is selected to cache a block that is not currently cached in Alluxio and will be
   read from UFS.
2. `alluxio.user.block.write.location.policy.class`
   This controls which worker is selected to cache a block generated from the client, and possibly persist it to the 
   UFS.

The built-in policies include:

* [LocalFirstPolicy](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/block/policy/LocalFirstPolicy.html)

  **This is the default policy.**

  > A policy that returns the local worker first, and if the local worker doesn't
  > exist or doesn't have enough capacity, will select the nearest worker from the active
  > workers list with sufficient capacity.

  * If no worker meets capacity criteria, will randomly select a worker from the list of all workers.

* [LocalFirstAvoidEvictionPolicy](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/block/policy/LocalFirstAvoidEvictionPolicy.html)

  This is the same as `LocalFirstPolicy` with the following addition:

  > The property `alluxio.user.block.avoid.eviction.policy.reserved.size.bytes`
  > is used as buffer space on each worker when calculating available space
  > to store each block.

  * If no worker meets availability criteria, will randomly select a worker from the list of all workers.

* [MostAvailableFirstPolicy](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/block/policy/MostAvailableFirstPolicy.html)

  > A policy that returns the worker with the most available bytes.

  * If no worker meets availability criteria, will randomly select a worker from the list of all workers.

* [RoundRobinPolicy](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/block/policy/RoundRobinPolicy.html)

  > A policy that chooses the worker for the next block in a round-robin manner
  > and skips workers that do not have enough space.

  * If no worker meets availability criteria, will randomly select a worker from the list of all workers.

* [SpecificHostPolicy](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/block/policy/SpecificHostPolicy.html)

  > Always returns a worker with the hostname specified by property `alluxio.worker.hostname`.

  * If no value is set, will randomly select a worker from the list of all workers.

* [DeterministicHashPolicy](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/block/policy/DeterministicHashPolicy.html)

  > This policy maps the blockId to several deterministic Alluxio workers. The number of workers a block
  > can be mapped to can be passed through the constructor. The default is 1. It skips the workers
  > that do not have enough capacity to hold the block.
  >
  > This policy is useful for limiting the amount of replication that occurs when reading blocks from
  > the UFS with high concurrency. With 30 workers and 100 remote clients reading the same block
  > concurrently, the replication level for the block would get close to 30 as each worker reads
  > and caches the block for one or more clients. If the clients use DeterministicHashPolicy with
  > 3 shards, the 100 clients will split their reads between just 3 workers, so that the replication
  > level for the block will be only 3 when the data is first loaded.
  >
  > Note that the hash function relies on the number of workers in the cluster, so if the number of
  > workers changes, the workers chosen by the policy for a given block will likely change.

Alluxio supports custom policies, so you can also develop your own policy appropriate for your
workload by implementing the interface `alluxio.client.block.policy.BlockLocationPolicy`. Note that a
default policy must have a constructor which takes `alluxio.conf.AlluxioConfiguration`.
To use the `ASYNC_THROUGH` write type, all the blocks of a file must be written to the same worker.

#### Write Tier

Alluxio allows a client to select a tier preference when writing blocks to a local worker. Currently
this policy preference exists only for local workers, not remote workers; remote workers will write
blocks to the highest tier.

By default, data is written to the top tier. Users can modify the default setting through the
`alluxio.user.file.write.tier.default` [property]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.user.file.write.tier.default)
or override it through an option to the 
[`FileSystem#createFile(AlluxioURI, CreateFilePOptions)`](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/client/file/FileSystem.html#createFile-alluxio.AlluxioURI-alluxio.grpc.CreateFilePOptions-)
API call.

#### Javadoc

For additional API information, please refer to the
[Alluxio javadocs](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/index.html).

### Hadoop-Compatible Java Client

On top of the [Alluxio file system](#java-client), Alluxio also has a convenience class
`alluxio.hadoop.FileSystem` that provides applications with a
[Hadoop compatible `FileSystem` interface](https://cwiki.apache.org/confluence/display/HADOOP2/HCFS).
This client translates Hadoop file operations to Alluxio file system operations,
allowing users to reuse existing code written for Hadoop without modification.
Read its [javadoc](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/alluxio/hadoop/FileSystem.html)
for more details.

#### Example

Here is a piece of example code to read ORC files from the Alluxio file system using the Hadoop 
interface.

```java
// create a new hadoop configuration
org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
// enforce hadoop client to bind alluxio.hadoop.FileSystem for URIs like alluxio://
conf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
conf.set("fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");

// Now alluxio address can be used like any other hadoop-compatible file system URIs
org.apache.orc.OrcFile.ReaderOptions options = new org.apache.orc.OrcFile.ReaderOptions(conf)
org.apache.orc.Reader orc = org.apache.orc.OrcFile.createReader(
    new Path("alluxio://localhost:19998/path/file.orc"), options);
```
