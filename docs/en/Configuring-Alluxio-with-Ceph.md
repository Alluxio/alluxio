---
layout: global
title: Configuring Alluxio with Ceph
nickname: Alluxio with Ceph
group: Under Store
priority: 1
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with Ceph as the under storage system. Alluxio supports
two different clients APIs to connect to [Ceph Object Storage](http://ceph.com/ceph-storage/object-storage/)
using [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/):
- [S3A](http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) (preferred)
- [Swift](http://docs.openstack.org/developer/swift/)

## Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file with `bootstrapConf` command.
For example, if you are running Alluxio on your local machine, `ALLUXIO_MASTER_HOSTNAME` should be
set to `localhost`

{% include Configuring-Alluxio-with-Swift/bootstrapConf.md %}

Alternatively, you can also create the configuration file from the template and set the contents
manually.

{% include Common-Commands/copy-alluxio-env.md %}

## Configuring Alluxio

You need to configure Alluxio to use Ceph as its under storage system by modifying
`conf/alluxio-site.properties`.

### Option 1: S3A Interface (preferred)

Modify `conf/alluxio-site.properties` to include:

{% include Configuring-Alluxio-with-Ceph/underfs-s3a.md %}

If using a Ceph release such as hammer (or older) specify `alluxio.underfs.s3a.signer.algorithm=S3SignerType`
to use v2 S3 signatures. To use GET Bucket (List Objects) Version 1 specify
`alluxio.underfs.s3a.list.objects.v1=true`.

### Option 2: Swift Interface
Modify `conf/alluxio-site.properties` to include:

{% include Configuring-Alluxio-with-Swift/underfs-address.md %}

Where `<swift-container>` is an existing Swift container.

The following configuration should be provided in the `conf/alluxio-site.properties`

{% include Configuring-Alluxio-with-Swift/several-configurations.md %}

Possible values of `<swift-use-public>` are `true`, `false`. Specify `<swift-auth-model>` as
`swiftauth` if using native Ceph RGW authentication and `<swift-auth-url>` as `http://<rgw-hostname>:8090/auth/1.0`.

Alternatively, these configuration settings can be set in the `conf/alluxio-env.sh` file. More
details about setting configuration parameters can be found in
[Configuration Settings](Configuration-Settings.html#environment-variables).

## Running Alluxio Locally with Ceph

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit your bucket/container to verify the files and directories created
by Alluxio exist.

If using the S3A connector, you should see files named like:
{% include Configuring-Alluxio-with-S3/s3-file.md %}

If using the Swift connector, you should see files named like:
{% include Configuring-Alluxio-with-Swift/swift-files.md %}

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}

## Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying Ceph Object Storage.
Depending on the interace used, refer to [S3A Access Control](Configuring-Alluxio-with-S3.html#s3-access-control)
or [Swift Access Control](Configuring-Alluxio-with-Swift.html#swift-access-control) for more information.
