---
layout: global
title: Filesystem Client API
nickname: Filesystem API
group: Features
priority: 1
---

* Table of Contents
{:toc}

Tachyon provides access to data through a filesystem interface. Files in Tachyon offer write-once
semantics: they become immutable after they have been written in their entirety and cannot be read
before being completed. Tachyon provides two different Filesystem APIs, a native API and a Hadoop
compatible API. The native API provides better performance, while the Hadoop compatible API gives
users the flexibility of leveraging Tachyon without having to modify existing code written using
Hadoop's API.

# Native API

Tachyon provides a Java like API for accessing and modifying files in the Tachyon namespace. All
resources are specified through a `TachyonURI` which represents the path to the resource.

### Getting a Filesystem Client

To obtain a Tachyon filesystem client in Java code, use:

{% include File-System-API/get-fileSystem.md %}

### Creating a File

All metadata operations as well as opening a file for reading or creating a file for writing are
executed through the FileSystem object. Since Tachyon files are immutable once written, the
idiomatic way to create files is to use `FileSystem#createFile(TachyonURI)`, which returns
a stream object that can be used to write the file. For example:

{% include File-System-API/write-file.md %}

### Specifying Operation Options

For all FileSystem operations, an additional `options` field may be specified, which allows
users to specify non-default settings for the operation. For example:

{% include File-System-API/specify-options.md %}

### IO Options

Tachyon uses two different storage types: Tachyon managed storage and under storage. Tachyon managed
storage is the memory, SSD, and/or HDD allocated to Tachyon workers. Under storage is the storage
resource managed by the underlying storage system, such as S3, Swift or HDFS. Users can specify the
interaction with the Tachyon's native storage and under storage through `ReadType` and `WriteType`.
`ReadType` specifies the data read behavior when reading a new file, ie. whether the data should be
saved in Tachyon Storage. `WriteType` specifies the data write behavior when writing a new file, ie.
whether the data should be written in Tachyon Storage.

Below is a table of the expected behaviors of `ReadType`. Reads will always prefer Tachyon storage
over the under storage system.

<table class="table table-striped">
<tr><th>Read Type</th><th>Behavior</th>
</tr>
{% for readtype in site.data.table.ReadType %}
<tr>
  <td>{{readtype.readtype}}</td>
  <td>{{site.data.table.en.ReadType.[readtype.readtype]}}</td>
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
  <td>{{site.data.table.en.WriteType.[writetype.writetype]}}</td>
</tr>
{% endfor %}
</table>

### Location policy

Tachyon provides location policy to choose which workers to store the blocks of a file. User can set
the policy in `CreateFileOptions` for writing files and `OpenFileOptions` for reading files into
Tachyon. Tachyon supports custom location policy, and the built-in polices include:

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

Tachyon supports custom policies, so you can also develop your own policy appropriate for your
workload. Note that a default policy must have an empty constructor. And to use ASYNC_THROUGH write
type, all the blocks of a file must be written to the same worker.

### Accessing an existing file in Tachyon

All operations on existing files or directories require the user to specify the `TachyonURI`.
With the TachyonURI, the user may use any of the methods of `FileSystem` to access the resource.

### Reading Data

A `TachyonURI` can be used to perform Tachyon FileSystem operations, such as modifying the file
metadata, ie. ttl or pin state, or getting an input stream to read the file.

For example, to read a file:

{% include File-System-API/read-file.md %}

# Hadoop API

Tachyon has a wrapper of the native client which provides the Hadoop compatible `FileSystem`
interface. With this client, Hadoop file operations will be translated to FileSystem
operations. The latest documentation for the `FileSystem` interface may be found
[here](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html).

The Hadoop compatible interface is provided as a convenience class, allowing users to retain
previous code written for Hadoop.
