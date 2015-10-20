---
layout: global
title: Filesystem Client API
nickname: Filesystem API
group: Features
priority: 1
---

* Table of Contents
{:toc}

Tachyon provides access to data through a filesystem interface. Files in Tachyon offer the write-once
semantics: they become immutable after they have been written in their entirety. Tachyon provides two
different Filesystem APIs, a native API and a Hadoop compatible API. The native API provides better
performance, while the Hadoop compatible API gives users the flexibility of leveraging Tachyon without
having to modify existing code written using Hadoop's API.

# Native API

Tachyon provides a Java like API for accessing and modifying files in the Tachyon namespace.

### Getting a Filesystem Client

To obtain a Tachyon filesystem client in Java code, use:

```java
TachyonFileSystem tfs = TachyonFileSystemFactory.get();
```

### Creating a File

All metadata operations as well as opening a file for reading or creating a file for writing are
executed through the TachyonFileSystem object. Since Tachyon files are immutable once written, the
idiomatic way to create files is to use `TachyonFileSystem#getOutStream(TachyonURI)`, which returns
a stream object that can be used to write the file. For example:

```java
TachyonFileSystem tfs = TachyonFileSystemFactory.get();
TachyonURI path = new TachyonURI("/myFile");
// Create a file and get its output stream
FileOutStream out = tfs.getOutStream(path);
// Write data
out.write(...);
// Close and complete file
out.close();
```

### Specifying Operation Options

For all TachyonFileSystem operations, an additional `options` field may be specified, which allows
users to specify non-default settings for the operation. For example:

```java
TachyonFileSystem tfs = TachyonFileSystemFactory.get();
TachyonURI path = new TachyonURI("/myFile");
// Generate options to set a custom blocksize of 128 MB
OutStreamOptions options = new OutStreamOptions.Builder(ClientContext.getConf()).setBlockSize(128 * Constants.MB).build();
FileOutStream out = tfs.getOutputStream(path, options);
```

### IO Options

Tachyon uses two different storage types: Tachyon managed storage and under storage. Tachyon managed
storage is the memory, SSD, and/or HDD allocated to Tachyon workers. Under storage is the storage
resource managed by the underlying storage system, such as S3, Swift or HDFS. Users can specify the
interaction with the Tachyon's native storage and under storage through the `NativeStorageType` and
`UnderStorageType` respectively. Note that read operations are affected by `TachyonStorageType`
(the data can be stored in more Tachyon workers) but not `UnderStorageType`.

Below is a table of commonly used `TachyonStorageType` and `UnderStorageType` combinations as well
as the deprecated `WriteType, ReadType` equivalents.

<table class="table">
<tr><th>TachyonStorageType</th><th>UnderStorageType</th><th>Previously</th>
<th>Read Behavior</th><th>Write Behavior</th>
</tr>
<tr>
  <td>PROMOTE</td>
  <td>NO_PERSIST</td>
  <td>MUST_CACHE, CACHE_PROMOTE</td>
  <td>Data is moved to the highest tier in the worker where the data was read. If the data was not
  on the local worker, a replica will be added to the local Tachyon worker for each completely read
  data block.</td>
  <td>Data is written synchronously to a Tachyon worker.</td>
</tr>
<tr>
  <td>PROMOTE</td>
  <td>SYNC_PERSIST</td>
  <td>CACHE_THROUGH, CACHE_PROMOTE</td>
  <td>Data is moved to the highest tier in the worker where the data was read. If the data was not
  in the Tachyon storage of the local worker, a replica will be added to the local Tachyon worker
  for each completely read data block.</td>
  <td>Data is written synchronously to a Tachyon worker and the under storage system.</td>
</tr>
<tr>
  <td>STORE</td>
  <td>NO_PERSIST</td>
  <td>MUST_CACHE, CACHE</td>
  <td>If the data was in the Tachyon storage of the local worker, a replica will be added to the
  local Tachyon worker for each completely read data block.</td>
  <td>Data is written synchronously to a Tachyon worker.</td>
</tr>
<tr>
  <td>STORE</td>
  <td>SYNC_PERSIST</td>
  <td>CACHE_THROUGH, CACHE</td>
  <td>If the data was in the Tachyon storage of the local worker, a replica will be added to the
  local Tachyon worker for each completely read data block.</td>
  <td>Data is written synchronously to a Tachyon worker and the under storage system.</td>
</tr>
<tr>
  <td>NO_STORE</td>
  <td>SYNC_PERSIST</td>
  <td>THROUGH, NO_CACHE</td>
  <td>Data is read preferrably from Tachyon storage, then under storage. No replicas will be
  created.</td>
  <td>Data is written synchronously to the under storage system.</td>
</tr>
</table>

### Opening a TachyonFile

Operations on an already existing file require its `TachyonFile` handle. The general contract of
the filesystem client is that operations creating new files expect `TachyonURI` while operations on
existing files expect `TachyonFile`. The `TachyonFile` handle is not a lock; other clients may
still delete or rename the file. However, for renames, the handle will still be valid and reference
the same file. A lock is used internally to guarantee atomicity of concurrent operations and also to
prevent deletion of files that are opened for reading. Below is an example of how to obtain a
`TachyonFile` handle:

```java
TachyonFileSystem tfs = TachyonFileSystemFactory.get();
// Assuming "/myFile" already exists
TachyonURI path = new TachyonURI("/myFile");
TachyonFile file = tfs.open(path);
```

### Reading Data

After obtaining a `TachyonFile` handle, the user may modify the file metadata or get an input stream
to read the file. For example:

```java
TachyonFileSystem tfs = TachyonFileSystemFactory.get();
TachyonURI path = new TachyonURI("/myFile");
TachyonFile file = tfs.open(path);
// Open the file for reading and obtains a lock preventing deletion
FileInStream in = tfs.getInStream(file);
// Read data
in.read(...);
// Close file relinquishing the lock
in.close();
```

# Hadoop API

Tachyon has a wrapper of the native client which provides the Hadoop compatible `FileSystem`
interface. With this client, Hadoop file operations will be translated to TachyonFileSystem
operations. The latest documentation for the `FileSystem` interface may be found
[here](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html).

The Hadoop compatible interface is provided as a convenience class, allowing users to retain
previous code written for Hadoop.
