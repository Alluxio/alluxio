---
layout: global
title: Unified and Transparent Namespace
nickname: Unified Namespace
group: Features
priority: 5
---

* Table of Contents
{:toc}

Alluxio enables effective data management across different storage systems through its use of
transparent naming and mounting API.

## Transparent Naming

Transparent naming maintains an identity between the Alluxio namespace and the underlying storage
system namespace.

![transparent]({{site.data.img.screenshot_transparent}})

When a user creates objects in the Alluxio namespace, they can choose whether these objects should
be persisted in the underlying storage system. For objects that are persisted, Alluxio preserve the
object paths, relative to the underlying storage system directory in which Alluxio objects are
stored. For instance, if a user creates a top-level directory `Users` with subdirectories `Alice`
and `Bob`, the directory structure and naming is preserved in the underlying storage system (e.g.
HDFS or S3). Similarly, when a user renames or deletes a persisted object in the Alluxio namespace,
it is renamed or deleted in the underlying storage system.

Furthermore, Alluxio transparently discovers content present in the underlying storage system which
was not created through Alluxio. For instance, if the underlying storage system contains a directory
`Data` with files `Reports` and `Sales`, all of which were not created through Alluxio, their
metadata will be loaded into Alluxio the first time they are accessed (e.g. when a user requests to
open a file). The data of file is not loaded to Alluxio during this process. To load the data into
Alluxio, one can read the data using `FileInStream` or use the `load` command of the Alluxio shell.

## Unified Namespace

Alluxio provides a mounting API that makes it possible to use Alluxio to access data across multiple
data sources.

![unified]({{site.data.img.screenshot_unified}})

By default, Alluxio namespace is mounted onto the directory specified by the
`alluxio.underfs.address` property of Alluxio configuration; this directory identifies the
"primary storage" for Alluxio. In addition, users can use the mounting API to add new and remove
existing data sources:

{% include Unified-and-Transparent-Namespace/mounting-API.md %}

For example, the primary storage could be HDFS and contains user directories; the `Data` directory
might be stored in an S3 bucket, which is mounted to the Alluxio namespace through the
`mount(alluxio://host:port/Data, s3://bucket/directory)` invocation.

## Example

In this example, we will showcase the above features. The example assumes that Alluxio source code
exists in the `${ALLUXIO_HOME}` directory and that there is an instance of Alluxio running locally.

First, let's create a temporary directory in the local file system that will use for the example:

{% include Unified-and-Transparent-Namespace/mkdir.md %}

Next, let's mount the directory created above into Alluxio and verify the mounted directory
appears in Alluxio:

{% include Unified-and-Transparent-Namespace/mount-demo.md %}

Next, let's verify that the metadata for content not created through Alluxio is loaded into Alluxio
the first time the content is accessed:

{% include Unified-and-Transparent-Namespace/ls-demo-hello.md %}

Next, let's create a file under the mounted directory and verify the file is created in the underlying
file system:

{% include Unified-and-Transparent-Namespace/create-file.md %}

Next, let's rename a file in Alluxio and verify the file is renamed in the underlying file system:

{% include Unified-and-Transparent-Namespace/rename.md %}

Next, let's delete a file in Alluxio and verify the file is deleted in the underlying file system:

{% include Unified-and-Transparent-Namespace/delete.md %}

Finally, let's unmount the mounted directory and verify that the directory is removed from the
Alluxio namespace, but its content is preserved in the underlying file system:

{% include Unified-and-Transparent-Namespace/unmount.md %}
