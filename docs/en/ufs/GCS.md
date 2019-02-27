---
layout: global
title: Google Cloud Storage
nickname: Google Cloud Storage
group: Under Stores
priority: 2
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [Google Cloud Storage (GCS)](https://cloud.google.com/storage/)
as the under storage system.

## Prerequisites

The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

In preparation for using GCS with Alluxio, create a bucket (or use an existing bucket). You
should also note the directory you want to use in that bucket, either by creating a new directory in
the bucket, or using an existing one. For the purposes of this guide, the GCS bucket name is called
`GCS_BUCKET`, and the directory in that bucket is called `GCS_DIRECTORY`.

For more information on GCS, please read its
[documentation](https://cloud.google.com/storage/docs/overview).

## Basic Setup

A GCS bucket can be mounted to the Alluxio either at the root of the namespace, or at a nested directory.

### Root Mount Point

Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Configure Alluxio to use GCS as its root under storage system. The first modification is to
specify an **existing** GCS bucket and directory as the under storage system by modifying
`conf/alluxio-site.properties` to include:

{% include Configuring-Alluxio-with-GCS/underfs-address.md %}

The google credentials must also be specified for the root mount point. In
`conf/alluxio-site.properties`, add:

```properties
alluxio.master.mount.table.root.option.fs.gcs.accessKeyId=<GCS_ACCESS_KEY_ID>
alluxio.master.mount.table.root.option.fs.gcs.secretAccessKey=<GCS_SECRET_ACCESS_KEY>
```

Replace `<GCS_ACCESS_KEY_ID>` and `<GCS_SECRET_ACCESS_KEY>` with actual
[GCS interoperable storage access keys](https://console.cloud.google.com/storage/settings),
or other environment variables that contain your credentials.
Note: GCS interoperability is disabled by default. Please click on the Interoperability tab
in [GCS setting](https://console.cloud.google.com/storage/settings) and enable this feature.
Click on `Create a new key` to get the Access Key and Secret pair.

After these changes, Alluxio should be configured to work with GCS as its under storage system, and
you can [Run Alluxio Locally with GCS](#running-alluxio-locally-with-gcs).

### Nested Mount Point

An GCS location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's [Command Line Interface]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}) can be used for this purpose.

```bash
$ ./bin/alluxio fs mount --option fs.gcs.accessKeyId=<GCS_ACCESS_KEY_ID> --option fs.gcs.secretAccessKey=<GCS_SECRET_ACCESS_KEY>\
  /mnt/gcs gs://GCS_BUCKET/GCS_DIRECTORY
```

## Running Alluxio Locally with GCS

Start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

{% include Common-Commands/runTests.md %}

Visit your GCS directory `GCS_BUCKET/GCS_DIRECTORY` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like:

```
GCS_BUCKET/GCS_DIRECTORY/default_tests_files/BASIC_CACHE_THROUGH
```

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}

## Advanced Setup

### Customize the Directory Suffix

Directories are represented in GCS as zero-byte objects named with a specified suffix. The
directory suffix can be updated with the configuration parameter
[alluxio.underfs.gcs.directory.suffix]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.underfs.gcs.directory.suffix).

### Configuring Application Dependency

When building your application to use Alluxio, your application should include a client module, the
`alluxio-core-client-fs` module to use the
[Alluxio file system interface]({{ '/en/api/FS-API.html' | relativize_url }}) or the
`alluxio-core-client-hdfs` module to use the
[Hadoop file system interface](https://wiki.apache.org/hadoop/HCFS). For example, if you
are using [maven](https://maven.apache.org/), you can add the dependency to your application with:

{% include Configuring-Alluxio-with-GCS/dependency.md %}

## GCS Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying object
storage.

The GCS credentials specified in Alluxio config represents a GCS user. GCS service backend checks
the user permission to the bucket and the object for access control. If the given GCS user does not
have the right access permission to the specified bucket, a permission denied error will be thrown.
When Alluxio security is enabled, Alluxio loads the bucket ACL to Alluxio permission on the first
time when the metadata is loaded to Alluxio namespace.

### Mapping from GCS user to Alluxio file owner

By default, Alluxio tries to extract the GCS user id from the credentials. Optionally,
`alluxio.underfs.gcs.owner.id.to.username.mapping` can be used to specify a preset gcs owner id to
Alluxio username static mapping in the format `id1=user1;id2=user2`. The Google Cloud Storage IDs
can be found at the console [address](https://console.cloud.google.com/storage/settings). Please use
the "Owners" one.

### Mapping from GCS ACL to Alluxio permission

Alluxio checks the GCS bucket READ/WRITE ACL to determine the owner's permission mode to a Alluxio
file. For example, if the GCS user has read-only access to the underlying bucket, the mounted
directory and files would have `0500` mode. If the GCS user has full access to the underlying bucket,
the mounted directory and files would have `0700` mode.

### Mount point sharing

If you want to share the GCS mount point with other users in Alluxio namespace, you can enable
`alluxio.underfs.object.store.mount.shared.publicly`.

### Permission change

Command such as `chown`, `chgrp`, and `chmod` to Alluxio directories and files do **NOT** propagate to the underlying
GCS buckets nor objects.
