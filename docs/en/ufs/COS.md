---
layout: global
title: Tencent COS
---

This guide describes the instructions to configure [Tencent COS](https://cloud.tencent.com/product/cos) as Alluxio's
under storage system. 

Tencent Cloud Object Storage (COS) is a distributed storage service offered by Tencent Cloud for unstructured data and accessible via HTTP/HTTPS protocols. It can store massive amounts of data and features imperceptible bandwidth and capacity expansion, making it a perfect data pool for big data computation and analytics.

## Prerequisites

In preparation for using COS with Alluxio, create a new bucket or use an existing bucket. You
should also note the directory you want to use in that bucket, either by creating a new directory in
the bucket, or using an existing one. For the purposes of this guide, the COS bucket name is called
`COS_BUCKET`, and the directory in that bucket is called `COS_DIRECTORY`.

You also need to provide APPID and REGION. In this guide, the APPID is called `COS_APP_ID`, and the REGION is called `COS_REGION`. For more information, please refer [here](https://cloud.tencent.com/document/product/436/7751).

## Basic Setup

### Root Mount Point

Create `conf/alluxio-site.properties` if it does not exist.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Configure Alluxio to use COS as its under storage system by modifying `conf/alluxio-site.properties`.
Specify an **existing** COS bucket and directory as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=cos://COS_ALLUXIO_BUCKET/COS_DATA/
```

Note that if you want to mount the whole cos bucket, add a trailing slash after the bucket name
(e.g. `cos://COS_BUCKET/`).

Specify the COS credentials for COS access by setting `fs.cos.access.key` and `fs.cos.secret.key` in
`alluxio-site.properties`.

```properties
fs.cos.access.key=<COS_SECRET_ID>
fs.cos.secret.key=<COS_SECRET_KEY>
```

Specify the COS region by setting `fs.cos.region` in `alluxio-site.properties` (e.g. ap-beijing) and `fs.cos.app.id`.

```properties
fs.cos.region=<COS_REGION>
fs.cos.app.id=<COS_APP_ID>
```

After these changes, Alluxio should be configured to work with COS as its under storage system, and
you can try [Running Alluxio Locally with COS](#running-alluxio-locally-with-cos).


## Running Alluxio Locally with COS

Start the Alluxio servers:

```shell
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This will start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```shell
$ ./bin/alluxio runTests
```

Before running an example program, please make sure the root mount point
set in the `conf/alluxio-site.properties` is a valid path in the ufs.
Make sure the user running the example program has write permissions to the alluxio file system.

```shell
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Nested Mount

An COS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{ '/en/operation/User-CLI.html' | relativize_url }}#mount) can be used for this purpose.
For example, the following command mounts a directory inside an COS bucket into Alluxio directory
`/cos`:

```shell
$ ./bin/alluxio fs mount --option fs.cos.access.key=<COS_SECRET_ID> \
    --option fs.cos.secret.key=<COS_SECRET_KEY> \
    --option fs.cos.region=<COS_REGION> \
    --option fs.cos.app.id=<COS_APP_ID> \
    /cos cos://<COS_ALLUXIO_BUCKET>/<COS_DATA>/
```

### [Experimental] COS multipart upload

The default upload method uploads one file completely from start to end in one go. We use multipart-upload method to upload one file by multiple parts, every part will be uploaded in one thread. It won't generate any temporary files while uploading.

To enable COS multipart upload, you need to modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.cos.multipart.upload.enabled=true
```

There are other parameters you can specify in `conf/alluxio-site.properties` to make the process faster and better.

```properties
# Timeout for uploading part when using multipart upload.
alluxio.underfs.object.store.multipart.upload.timeout
```
```properties
# Thread pool size for COS multipart upload.
alluxio.underfs.cos.multipart.upload.threads
```
```properties
# Multipart upload partition size for COS. The default partition size is 64MB. 
alluxio.underfs.cos.multipart.upload.partition.size
```



