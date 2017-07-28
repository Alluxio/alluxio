---
layout: global
title: Hadoop-Compatible Java Client
nickname: Hadoop-Compatible Java
group: Clients
priority: 2
---

* Table of Contents
{:toc}

Alluxio provides access to data through a filesystem interface. Files in Alluxio offer write-once
semantics: they become immutable after they have been written in their entirety and cannot be read
before being completed. Alluxio provides two different Filesystem APIs, a native API and a Hadoop
compatible API. The native API provides additional functionality, while the Hadoop compatible API
gives users the flexibility of leveraging Alluxio without having to modify existing code written
using Hadoop's API.

Alluxio has a wrapper of the [native client](Clients-Java-Native.html) which provides the Hadoop
compatible `FileSystem` interface. With this client, Hadoop file operations will be translated to
FileSystem operations. The latest documentation for the `FileSystem` interface may be found
[here](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html).

The Hadoop compatible interface is provided as a convenience class, allowing users to reuse
previous code written for Hadoop.
