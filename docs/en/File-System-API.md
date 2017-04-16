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

## Native API

Alluxio provides a Java like API for accessing and modifying files in the Alluxio namespace. All
resources are specified through a `AlluxioURI` which represents the path to the resource.

### Getting a Filesystem Client

To obtain an Alluxio filesystem client in Java code, use:

{% include File-System-API/get-fileSystem.md %}

### Creating a File

All metadata operations as well as opening a file for reading or creating a file for writing are
executed through the FileSystem object. Since Alluxio files are immutable once written, the
idiomatic way to create files is to use `FileSystem#createFile(AlluxioURI)`, which returns
a stream object that can be used to write the file. For example:

{% include File-System-API/write-file.md %}

### Specifying Operation Options

For all FileSystem operations, an additional `options` field may be specified, which allows
users to specify non-default settings for the operation. For example:

{% include File-System-API/specify-options.md %}

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

Using Alluxio's Java API, users can set the policy in `CreateFileOptions` for writing files and `OpenFileOptions` for reading files into
Alluxio.

Users can simply override the default policy class in the [configuration file](Configuration-Settings.html) at property `alluxio.user.file.write.location.policy.class`. The built-in policies include:

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

Alluxio supports custom policies, so you can also develop your own policy appropriate for your workload by implementing interface `alluxio.client.file.policyFileWriteLocationPolicy`. Note that a default policy must have an empty constructor. And to use ASYNC_THROUGH write
type, all the blocks of a file must be written to the same worker.

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

{% include File-System-API/read-file.md %}

### REST API

For portability with other languages, the Alluxio native API is also accessible via an HTTP proxy in
the form of a REST API.

The REST API documentation is generated as part of Alluxio build and accessible through
`${ALLUXIO_HOME}/core/server/proxy/target/miredot/index.html`. In particular, the `paths`
resource endpoints correspond to the `FileSystem` API endpoints. The main difference between
the REST API and the Native API is in how streams are represented. While the native API
can use in-memory streams, the REST API decouples the stream creation and access (see the
`create` and `open` REST API methods and the `streams` resource endpoints for details).

The HTTP proxy is a standalone server that can be started using `${ALLUXIO_HOME}/bin/alluxio-start.sh proxy`
and stopped using `${ALLUXIO_HOME}/bin/alluxio-stop.sh proxy`. By default, the REST API is available on port 39999.

There are performance implications of using the HTTP proxy. In particular, using the proxy requires an
extra hop. For optimal performance, it is recommended to run the proxy server an Alluxio worker on each
compute node.

## Hadoop API

Alluxio has a wrapper of the native client which provides the Hadoop compatible `FileSystem`
interface. With this client, Hadoop file operations will be translated to FileSystem
operations. The latest documentation for the `FileSystem` interface may be found
[here](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html).

The Hadoop compatible interface is provided as a convenience class, allowing users to retain
previous code written for Hadoop.
