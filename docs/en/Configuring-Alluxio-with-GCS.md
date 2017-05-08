---
layout: global
title: Configuring Alluxio with Google Cloud Storage
nickname: Alluxio with GCS
group: Under Store
priority: 0
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [Google Cloud Storage (GCS)](https://cloud.google.com/storage/) as the under storage system.

## Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file with `bootstrapConf` command.
For example, if you are running Alluxio on your local machine, `ALLUXIO_MASTER_HOSTNAME` should be
set to `localhost`

{% include Configuring-Alluxio-with-GCS/bootstrapConf.md %}

Alternatively, you can also create the configuration file from the template and set the contents
manually.

{% include Common-Commands/copy-alluxio-env.md %}


Also, in preparation for using GCS with Alluxio, create a bucket (or use an existing bucket). You
should also note the directory you want to use in that bucket, either by creating a new directory in
the bucket, or using an existing one. For the purposes of this guide, the GCS bucket name is called
`GCS_BUCKET`, and the directory in that bucket is called `GCS_DIRECTORY`.

If you are new to GCS, please read its
[documentations](https://cloud.google.com/storage/docs/overview).

## Configuring Alluxio

You need to configure Alluxio to use GCS as its under storage system. The first modification is to
specify an **existing** GCS bucket and directory as the under storage system by modifying
`conf/alluxio-site.properties` to include:

{% include Configuring-Alluxio-with-GCS/underfs-address.md %}

Next, you need to specify the Google credentials for GCS access. In `conf/alluxio-site.properties`,
add:

{% include Configuring-Alluxio-with-GCS/google.md %}

Here, `<GCS_ACCESS_KEY_ID>` and `<GCS_SECRET_ACCESS_KEY>` should be replaced with your actual
[GCS interoperable storage access keys](https://console.cloud.google.com/storage/settings),
or other environment variables that contain your credentials.
Note: GCS interoperability is disabled by default. Please click on the Interoperability tab
in [GCS setting](https://console.cloud.google.com/storage/settings) and enable this feature.
Then click on `Create a new key` to get the Access Key and Secret pair.

Alternatively, these configuration settings can be set in the `conf/alluxio-env.sh` file. More
details about setting configuration parameters can be found in
[Configuration Settings](Configuration-Settings.html#environment-variables).

After these changes, Alluxio should be configured to work with GCS as its under storage system, and
you can try [Running Alluxio Locally with GCS](#running-alluxio-locally-with-gcs).

### Configuring Application Dependency

When building your application to use Alluxio, your application should include a client module, the
`alluxio-core-client-fs` module to use the [Alluxio file system interface](File-System-API.html) or
the `alluxio-core-client-hdfs` module to use the
[Hadoop file system interface](https://wiki.apache.org/hadoop/HCFS). For example, if you
are using [maven](https://maven.apache.org/), you can add the dependency to your application with:

{% include Configuring-Alluxio-with-GCS/dependency.md %}

## Running Alluxio Locally with GCS

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit your GCS directory `GCS_BUCKET/GCS_DIRECTORY` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-GCS/gcs-file.md %}

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}

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
Alluxio username static mapping in the format "id1=user1;id2=user2". The Google Cloud Storage IDs
can be found at the console [address](https://console.cloud.google.com/storage/settings). Please use
the "Owners" one.

### Mapping from GCS ACL to Alluxio permission

Alluxio checks the GCS bucket READ/WRITE ACL to determine the owner's permission mode to a Alluxio
file. For example, if the GCS user has read-only access to the underlying bucket, the mounted
directory and files would have 0500 mode. If the GCS user has full access to the underlying bucket,
the mounted directory and files would have 0700 mode.

### Mount point sharing

If you want to share the GCS mount point with other users in Alluxio namespace, you can enable
`alluxio.underfs.object.store.mount.shared.publicly`.

### Permission change

In addition, chown/chgrp/chmod to Alluxio directories and files do NOT propagate to the underlying
GCS buckets nor objects.
