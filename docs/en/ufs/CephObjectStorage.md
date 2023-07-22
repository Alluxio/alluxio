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

A Ceph bucket can be mounted to Alluxio either at the root of the namespace, or at a nested directory. In this guide, the Ceph bucket is called `CEPH_BUCKET`, and the directory in the bucket is called `CEPH_DIRECTORY`.

Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

### Option 1: S3 Interface (preferred)

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=s3://<CEPH_BUCKET>/<CEPH_DIRECTORY>
s3a.accessKeyId=<S3_ACCESS_KEY_ID>
s3a.secretKey=<S3_SECRET_KEY>
alluxio.underfs.s3.endpoint=http://<RGW-HOSTNAME>:<RGW-PORT>
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3.inherit.acl=<INHERIT_ACL>
```

If using a Ceph release such as hammer (or older) specify `alluxio.underfs.s3.signer.algorithm=S3SignerType`
to use v2 S3 signatures. To use GET Bucket (List Objects) Version 1 specify
`alluxio.underfs.s3.list.objects.v1=true`.

### Option 2: Swift Interface
Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=swift://<CEPH_BUCKET>/<CEPH_DIRECTORY>
fs.swift.user=<SWIFT_USER>
fs.swift.tenant=<SWIFT_TENANT>
fs.swift.password=<SWIFT_PASSWORD>
fs.swift.auth.url=<SWIFT_AUTH_URL>
fs.swift.auth.method=<SWIFT_AUTH_METHOD>
```
Replace `<CEPH_BUCKET>/<CEPH_DIRECTORY>` with an existing Swift bucket location. Specify `<SWIFT_AUTH_URL>` as `http://<rgw-hostname>:<rgw-port>/auth/1.0`.

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
<CEPH_BUCKET>/<CEPH_DIRECTORY>/default_tests_files/Basic_CACHE_THROUGH
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
