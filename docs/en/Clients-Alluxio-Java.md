---
layout: global
title: Alluxio Java Client
nickname: Alluxio Java Client
group: Clients
priority: 1
---

* Table of Contents
{:toc}

Alluxio provides access to data through a filesystem interface. Files in Alluxio offer write-once
semantics: they become immutable after they have been written in their entirety and cannot be read
before being completed. Alluxio provides two different Filesystem APIs, the Alluxio API and a Hadoop
compatible API. The Alluxio API provides additional functionality, while the Hadoop compatible API
gives users the flexibility of leveraging Alluxio without having to modify existing code written
using Hadoop's API.

All resources with the Alluxio Java API are specified through a `AlluxioURI` which represents the
path to the resource.

### Getting a Filesystem Client

To obtain an Alluxio filesystem client in Java code, use:

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
interaction with Alluxio managed storage and under storage through `ReadType` and `WriteType`.
`ReadType` specifies the data read behavior when reading a file. `WriteType` specifies the data
write behavior when writing a new file, ie. whether the data should be written in Alluxio Storage.

Below is a table of the expected behaviors of `ReadType`. Reads will always prefer Alluxio storage
over the under storage system.

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

Below is a table of the expected behaviors of `WriteType`

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

### Location policy

Alluxio provides location policy to choose which workers to store the blocks of a file.

Using Alluxio's Java API, users can set the policy in `CreateFileOptions` for writing files and
`OpenFileOptions` for reading files into Alluxio.

Users can simply override the default policy class in the
[configuration file](Configuration-Settings.html) at property
`alluxio.user.file.write.location.policy.class`. The built-in policies include:

* **LocalFirstPolicy (alluxio.client.file.policy.LocalFirstPolicy)**

    Returns the local host first, and if the local worker doesn't have enough capacity of a block,
    it randomly picks a worker from the active workers list. This is the default policy.

* **MostAvailableFirstPolicy (alluxio.client.file.policy.MostAvailableFirstPolicy)**

    Returns the worker with the most available bytes.

* **RoundRobinPolicy (alluxio.client.file.policy.RoundRobinPolicy)**

    Chooses the worker for the next block in a round-robin manner and skips workers that do not have
    enough capacity.

* **SpecificHostPolicy (alluxio.client.file.policy.SpecificHostPolicy)**

    Returns a worker with the specified host name. This policy cannot be set as default policy.

Alluxio supports custom policies, so you can also develop your own policy appropriate for your
workload by implementing interface `alluxio.client.file.policy.FileWriteLocationPolicy`. Note that a
default policy must have an empty constructor. And to use ASYNC_THROUGH write type, all the blocks
of a file must be written to the same worker.

### Write Tier

Alluxio allows a client to select a tier preference when writing blocks to a local worker. Currently
this policy preference exists only for local workers, not remote workers; remote workers will write
blocks to the highest tier.

By default, data is written to the top tier. Users can modify the default setting through the
`alluxio.user.file.write.tier.default` [configuration](Configuration-Settings.html) property or
override it through an option to the `FileSystem#createFile(AlluxioURI)` API call.

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
// Open the file for reading
FileInStream in = fs.openFile(path);
// Read data
in.read(...);
// Close file relinquishing the lock
in.close();
```

### Javadoc

For additional API information, please refer to the
[Alluxio javadocs](http://www.alluxio.org/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/index.html).
