---
layout: global
title: Tachyon Filesystem Client API
nickname: Filesystem Client
group: Features
---

Tachyon provides access to data through a filesystem interface. Files in Tachyon are write once and immutable after the file has been completed. Tachyon provides two different Filesystem APIs, a native API and a Hadoop compatible API. The native API will provide better performance, while the Hadoop compatible API gives users the flexibility of leveraging Tachyon without modifying existing code written with Hadoop's API.

# Native API

Tachyon provides a Java like API for accessing and modifying files in the Tachyon namespace.

### Getting a Filesystem Client

To obtain a Tachyon filesystem client in Java code, use:

	TachyonFileSystem tfs = TachyonFileSystem.get();

### Creating a File

All metadata operations as well as opening a file for reading or creating a file for writing are done through the TachyonFileSystem object. Since Tachyon is write once for files, it is encouraged to use `TachyonFileSystem#getOutStream(TachyonURI)` to create a file and get the stream object to write it at the same time. For example:

	TachyonFileSystem tfs = TachyonFileSystem.get();
	TachyonURI path = new TachyonURI("/myFile");
	// Create file and get output stream
	FileOutStream out = tfs.getOutStream(path);
	// Write data
    out.write(...);
    // Close and complete file
	out.close();

### Specifying Operation Options

For all TachyonFileSystem operations, an additional `options` field may be specified, which allows users to specify non-default settings for the operation. For example:

	TachyonFileSystem tfs = TachyonFileSystem.get();
	TachyonURI path = new TachyonURI("/myFile");
	// Generate options to set a custom blocksize of 128 MB
	OutStreamOptions options = new OutStreamOptions.Builder(ClientContext.getConf()).setBlockSize(128 * Constants.MB).build();
	FileOutStream out = tfs.getOutputStream(path, options);

### Opening a TachyonFile

Operations on an already existing file require its `TachyonFile` handler. The general contract of the filesystem client is operations creating a new file will use `TachyonURI` and operations on existing files require a `TachyonFile`. By obtaining the handler, the client is guaranteed the file existed in Tachyon but not a lock on the file. Other clients may still delete or rename the file. For renames, the handler will still be valid and reference the same file. A lock on the file (preventing deletion) is only obtained during operations or after opening a stream for reading the file. Below is an example of how to obtain a `TachyonFile`:

	TachyonFileSystem tfs = TachyonFileSystem.get();
	// Assuming "/myFile" already exists
	TachyonURI path = new TachyonURI("/myFile");
	TachyonFile file = tfs.open(path);

### Reading Data

After obtaining a `TachyonFile`, the user my modify the file metadata or get an inputstream to read the file. For example:

	TachyonFileSystem tfs = TachyonFileSystem.get();
	TachyonURI path = new TachyonURI("/myFile");
	TachyonFile file = tfs.open(path);
	// Open the file for reading and obtains a lock preventing deletion
	FileInStream in = tfs.getInStream(file);
	// Read data
	in.read(...);
	// Close file relinquishing the lock
	in.close();

# Hadoop API

Tachyon has a wrapper of the native client which provides the Hadoop compatible `FileSystem` interface. With this client, Hadoop file operations will be translated to TachyonFileSystem operations. The latest documentation for the `FileSystem` interface may be found [here](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html).

The Hadoop compatible interface is provided as a convenience class, allowing users to retain previous code written for Hadoop.