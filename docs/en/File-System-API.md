---
layout: global
title: Filesystem Client API
nickname: Filesystem API
group: Features
priority: 1
---

* Table of Contents
{:toc}

Alluxio provides access to data through a filesystem interface. Files in Alluxio offer write-once
semantics: they become immutable after they have been written in their entirety and cannot be read
before being completed. Alluxio provides two different Filesystem APIs, a native API and a Hadoop
compatible API. The native API provides better performance, while the Hadoop compatible API gives
users the flexibility of leveraging Alluxio without having to modify existing code written using
Hadoop's API.

# Native API

Alluxio provides a Java like API for accessing and modifying files in the Alluxio namespace. All
resources are specified through a `AlluxioURI` which represents the path to the resource.

### Getting a Filesystem Client

To obtain a Alluxio filesystem client in Java code, use:

```java
FileSystem fs = FileSystem.Factory.get();
```

### Creating a File

All metadata operations as well as opening a file for reading or creating a file for writing are
executed through the FileSystem object. Since Alluxio files are immutable once written, the
idiomatic way to create files is to use `FileSystem#createFile(AlluxioURI)`, which returns
a stream object that can be used to write the file. For example:

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

### Specifying Operation Options

For all FileSystem operations, an additional `options` field may be specified, which allows
users to specify non-default settings for the operation. For example:

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Generate options to set a custom blocksize of 128 MB
CreateFileOptions options = CreateFileOptions.defaults().setBlockSize(128 * Constants.MB);
FileOutStream out = fs.createFile(path, options);
```

### IO Options

Alluxio uses two different storage types: Alluxio managed storage and under storage. Alluxio managed
storage is the memory, SSD, and/or HDD allocated to Alluxio workers. Under storage is the storage
resource managed by the underlying storage system, such as S3, Swift or HDFS. Users can specify the
interaction with the Alluxio's native storage and under storage through `ReadType` and `WriteType`.
`ReadType` specifies the data read behavior when reading a new file, ie. whether the data should be
saved in Alluxio Storage. `WriteType` specifies the data write behavior when writing a new file, ie.
whether the data should be written in Alluxio Storage.

Below is a table of the expected behaviors of `ReadType`. Reads will always prefer Alluxio storage
over the under storage system.

<table class="table table-striped">
<tr><th>Read Type</th><th>Behavior</th>
</tr>
<tr>
  <td>CACHE_PROMOTE</td>
  <td>Data is moved to the highest tier in the worker where the data was read. If the data was not
  in the Alluxio storage of the local worker, a replica will be added to the local Alluxio worker
  for each completely read data block. This is the default read type.</td>
</tr>
<tr>
  <td>CACHE</td>
  <td>If the data was not in the Alluxio storage of the local worker, a replica will be added to the
  local Alluxio worker for each completely read data block.</td>
</tr>
<tr>
  <td>NO_CACHE</td>
  <td>No replicas will be created.</td>
</tr>
</table>

Below is a table of the expected behaviors of `WriteType`

<table class="table table-striped">
<tr><th>Write Type</th><th>Behavior</th>
</tr>
<tr>
  <td>CACHE_THROUGH</td>
  <td>Data is written synchronously to a Alluxio worker and the under storage system.</td>
</tr>
<tr>
  <td>MUST_CACHE</td>
  <td>Data is written synchronously to a Alluxio worker. No data will be written to the under
  storage. This is the default write type.</td>
</tr>
<tr>
  <td>THROUGH</td>
  <td>Data is written synchronously to the under storage. No data will be written to Alluxio.</td>
</tr>
<tr>
  <td>ASYNC_THROUGH</td>
  <td>Data is written synchronously to a Alluxio worker and asynchronously to the under storage
  system. Experimental.</td>
</tr>
</table>

### Location policy

Alluxio provides location policy to choose which workers to store the blocks of a file. User can set
the policy in `CreateFileOptions` for writing files and `OpenFileOptions` for reading files into
Alluxio. Alluxio supports custom location policy, and the built-in polices include:

* **LocalFirstPolicy**

    Returns the local host first, and if the local worker doesn't have enough capacity of a block,
    it randomly picks a worker from the active workers list. This is the default policy.

* **MostAvailableFirstPolicy**

    Returns the worker with the most available bytes.

* **RoundRobinPolicy**

    Chooses the worker for the next block in a round-robin manner and skips workers that do not have
    enough capacity.

* **SpecificHostPolicy**

    Returns a worker with the specified host name. This policy cannot be set as default policy.

Alluxio supports custom policies, so you can also develop your own policy appropriate for your
workload. Note that a default policy must have an empty constructor. And to use ASYNC_THROUGH write
type, all the blocks of a file must be written to the same worker.

### Accessing an existing file in Alluxio

All operations on existing files or directories require the user to specify the `AlluxioURI`.
With the AlluxioURI, the user may use any of the methods of `FileSystem` to access the resource.

### Reading Data

A `AlluxioURI` can be used to perform Alluxio FileSystem operations, such as modifying the file
metadata, ie. ttl or pin state, or getting an input stream to read the file.

For example, to read a file:

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Open the file for reading and obtains a lock preventing deletion
FileInStream in = fs.openFile(path);
// Read data
in.read(...);
// Close file relinquishing the lock
in.close();
```

# Hadoop API

Alluxio has a wrapper of the native client which provides the Hadoop compatible `FileSystem`
interface. With this client, Hadoop file operations will be translated to FileSystem
operations. The latest documentation for the `FileSystem` interface may be found
[here](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html).

The Hadoop compatible interface is provided as a convenience class, allowing users to retain
previous code written for Hadoop.
