---
layout: global
title: CephObjectStorage
---


This guide describes how to configure Alluxio with [Ceph Object Storage](http://ceph.com/ceph-storage/object-storage/) as the under storage system. 

Ceph Object Storage is a distributed, open-source storage system designed for storing and retrieving large amounts of unstructured data. It provides a scalable and highly available storage solution that can be deployed on commodity hardware.

Alluxio supports two different clients APIs to connect to Ceph Object Storage using [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/):
- [S3](http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) (preferred)
- [Swift](http://docs.openstack.org/developer/swift/)

## Basic Setup

A Ceph bucket can be mounted to Alluxio either at the root of the namespace, or at a nested directory.

### Root Mount Point

Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

#### Option 1: S3 Interface (preferred)

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=s3://<bucket>/<folder>
s3a.accessKeyId=<access-key>
s3a.secretKey=<secret-key>
alluxio.underfs.s3.endpoint=http://<rgw-hostname>:<rgw-port>
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3.inherit.acl=<inherit-acl>
```

If using a Ceph release such as hammer (or older) specify `alluxio.underfs.s3.signer.algorithm=S3SignerType`
to use v2 S3 signatures. To use GET Bucket (List Objects) Version 1 specify
`alluxio.underfs.s3.list.objects.v1=true`.

#### Option 2: Swift Interface
Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=swift://<bucket>/<folder>
fs.swift.user=<swift-user>
fs.swift.tenant=<swift-tenant>
fs.swift.password=<swift-user-password>
fs.swift.auth.url=<swift-auth-url>
fs.swift.auth.method=<swift-auth-method>
```
Replace `<bucket>/<folder>` with an existing Swift container location. Possible values of `<swift-use-public>` are
`true`, `false`. Specify `<swift-auth-model>` as `swiftauth` if using native Ceph RGW authentication and `<swift-auth-url>`
as `http://<rgw-hostname>:<rgw-port>/auth/1.0`.

## Running Alluxio Locally with Ceph

Start up Alluxio locally to see that everything works.

```shell
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```shell
$ ./bin/alluxio runTests
```

Visit your bucket to verify the files and directories created by Alluxio exist.

You should see files named like:
```
<bucket>/<folder>/default_tests_files/Basic_CACHE_THROUGH
```

To stop Alluxio, run:

```shell
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying Ceph
Object Storage. Depending on the interace used, refer to
[S3 Access Control]({{ '/en/ufs/S3.html' | relativize_url }}#s3-access-control) or
[Swift Access Control]({{ '/en/ufs/Swift.html' | relativize_url }}#swift-access-control) for more information.
