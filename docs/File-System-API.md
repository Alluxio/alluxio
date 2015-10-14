---
layout: global
title: Tachyon Filesystem Client API
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
done through the TachyonFileSystem object. Since Tachyon files are immutable once written, the 
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

### Opening a TachyonFile

Operations on an already existing file require its `TachyonFile` handle. The general contract of
the filesystem client is that operations creating new files expect `TachyonURI` while operations on
existing files expect `TachyonFile`. The `TachyonFile` handle is not a lock; other clients may 
still delete or rename the file. However, for renames, the handle will still be valid and reference 
the same file. A lock is used internally to guarantee atomicity of concurrent operations and also to prevent deletion of files that are opened for reading. Below is an example of how to obtain a `TachyonFile` 
handle:

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
