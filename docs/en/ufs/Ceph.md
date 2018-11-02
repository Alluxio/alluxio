---
layout: global
title: Ceph
nickname: Ceph
group: Under Stores
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with Ceph as the under storage system. Alluxio supports
two different clients APIs to connect to [Ceph Object Storage](http://ceph.com/ceph-storage/object-storage/)
using [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/):
- [S3A](http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) (preferred)
- [Swift](http://docs.openstack.org/developer/swift/)

## Initial Setup

The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

## Configuring Alluxio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

### Option 1: S3A Interface (preferred)

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.address=s3a://<bucket>/<folder>
aws.accessKeyId=<access-key>
aws.secretKey=<secret-key>
alluxio.underfs.s3.endpoint=http://<rgw-hostname>:<rgw-port>
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3a.inherit_acl=<inherit-acl>
```

If using a Ceph release such as hammer (or older) specify `alluxio.underfs.s3a.signer.algorithm=S3SignerType`
to use v2 S3 signatures. To use GET Bucket (List Objects) Version 1 specify
`alluxio.underfs.s3a.list.objects.v1=true`.

### Option 2: Swift Interface
Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.address=swift://<container>/<folder>
fs.swift.user=<swift-user>
fs.swift.tenant=<swift-tenant>
fs.swift.password=<swift-user-password>
fs.swift.auth.url=<swift-auth-url>
fs.swift.use.public.url=<swift-use-public>
fs.swift.auth.method=<swift-auth-model>
```
Replace `<container>/<folder>` with an existing Swift container location. Possible values of `<swift-use-public>` are
`true`, `false`. Specify `<swift-auth-model>` as `swiftauth` if using native Ceph RGW authentication and `<swift-auth-url>`
as `http://<rgw-hostname>:<rgw-port>/auth/1.0`.

## Running Alluxio Locally with Ceph

Start up Alluxio locally to see that everything works.

```bash
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```bash
$ ./bin/alluxio runTests
```

Visit your bucket/container to verify the files and directories created
by Alluxio exist.

If using the S3A connector, you should see files named like:
```
<bucket>/<folder>/default_tests_files/Basic_CACHE_THROUGH
```

If using the Swift connector, you should see files named like:
```
<container>/<folder>/default_tests_files/Basic_CACHE_THROUGH
```

To stop Alluxio, run:

```bash
$ ./bin/alluxio-stop.sh local
```

## Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying Ceph
Object Storage. Depending on the interace used, refer to
[S3A Access Control]({{ '/en/ufs/S3.html' | relativize_url }}#s3-access-control) or
[Swift Access Control]({{ '/en/ufs/Swift.html' | relativize_url }}#swift-access-control) for more information.
