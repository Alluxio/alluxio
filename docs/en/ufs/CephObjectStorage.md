---
layout: global
title: CephObjectStorage
nickname: CephObjectStorage
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with Ceph Object Storage as the under storage system. Alluxio supports
two different clients APIs to connect to [Ceph Object Storage](http://ceph.com/ceph-storage/object-storage/)
using [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/):
- [S3](http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) (preferred)
- [Swift](http://docs.openstack.org/developer/swift/)

## Prerequisites

The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

## Basic Setup

A Ceph bucket can be mounted to Alluxio either at the root of the namespace, or at a nested directory.

### Root Mount Point

Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

#### Option 1: S3 Interface (preferred)

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.master.mount.table.root.ufs=s3://<bucket>/<folder>
alluxio.master.mount.table.root.option.aws.accessKeyId=<access-key>
alluxio.master.mount.table.root.option.aws.secretKey=<secret-key>
alluxio.master.mount.table.root.option.alluxio.underfs.s3.endpoint=http://<rgw-hostname>:<rgw-port>
alluxio.master.mount.table.root.option.alluxio.underfs.s3.disable.dns.buckets=true
alluxio.master.mount.table.root.option.alluxio.underfs.s3.inherit.acl=<inherit-acl>
```

If using a Ceph release such as hammer (or older) specify `alluxio.underfs.s3.signer.algorithm=S3SignerType`
to use v2 S3 signatures. To use GET Bucket (List Objects) Version 1 specify
`alluxio.underfs.s3.list.objects.v1=true`.

#### Option 2: Swift Interface
Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.master.mount.table.root.ufs=swift://<bucket>/<folder>
alluxio.master.mount.table.root.option.fs.swift.user=<swift-user>
alluxio.master.mount.table.root.option.fs.swift.tenant=<swift-tenant>
alluxio.master.mount.table.root.option.fs.swift.password=<swift-user-password>
alluxio.master.mount.table.root.option.fs.swift.auth.url=<swift-auth-url>
alluxio.master.mount.table.root.option.fs.swift.auth.method=<swift-auth-method>
```
Replace `<bucket>/<folder>` with an existing Swift container location. Possible values of `<swift-use-public>` are
`true`, `false`. Specify `<swift-auth-model>` as `swiftauth` if using native Ceph RGW authentication and `<swift-auth-url>`
as `http://<rgw-hostname>:<rgw-port>/auth/1.0`.

### Nested Mount Point

An Ceph location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's [Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

Issue the following command to use the S3 interface:
```console
$ ./bin/alluxio fs mount \
  --option aws.accessKeyId=<CEPH_ACCESS_KEY_ID> \
  --option aws.secretKey=<CEPH_SECRET_ACCESS_KEY> \
  --option alluxio.underfs.s3.endpoint=<HTTP_ENDPOINT> \
  --option alluxio.underfs.s3.disable.dns.buckets=true \
  --option alluxio.underfs.s3.inherit.acl=false \
  /mnt/ceph s3://<BUCKET>/<FOLDER>
```

Similarly, to use the Swift interface:
```console
$ ./bin/alluxio fs mount \
  --option fs.swift.user=<SWIFT_USER> \
  --option fs.swift.tenant=<SWIFT_TENANT> \
  --option fs.swift.password=<SWIFT_PASSWORD> \
  --option fs.swift.auth.url=<AUTH_URL> \
  --option fs.swift.auth.method=<AUTH_METHOD> \
  /mnt/ceph swift://<BUCKET>/<FOLDER>
```

## Running Alluxio Locally with Ceph

Start up Alluxio locally to see that everything works.

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Visit your bucket to verify the files and directories created by Alluxio exist.

You should see files named like:
```
<bucket>/<folder>/default_tests_files/Basic_CACHE_THROUGH
```

To stop Alluxio, run:

```console
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying Ceph
Object Storage. Depending on the interace used, refer to
[S3 Access Control]({{ '/en/ufs/S3.html' | relativize_url }}#s3-access-control) or
[Swift Access Control]({{ '/en/ufs/Swift.html' | relativize_url }}#swift-access-control) for more information.
