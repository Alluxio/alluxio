---
layout: global
title: Huawei OBS
nickname: Huawei OBS
group: Storage Integrations
priority: 0
---


This guide describes the instructions to configure [OBS](https://www.huaweicloud.com/product/obs) as Alluxio's
under storage system.

## Prerequisites

The Alluxio binaries must be on your machine. You can
[download the binaries locally]({{ '/en/overview/Getting-Started.html' | relativize_url }}).

In preparation for using OBS with Alluxio, create a new bucket or use an existing bucket. You
should also note the directory you want to use in that bucket, either by creating a new directory in
the bucket, or using an existing one. For the purposes of this guide, the OBS bucket name is called
`OBS_BUCKET`, and the directory in that bucket is called `OBS_DIRECTORY`.

## Basic Setup

### Root Mount Point

Create `conf/alluxio-site.properties` if it does not exist.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Configure Alluxio to use OBS as its under storage system by modifying `conf/alluxio-site.properties`.
Specify an **existing** OBS bucket and directory as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.dora.client.ufs.root=obs://<OBS_BUCKET>/<OBS_DIRECTORY>
```

Note that if you want to mount the whole obs bucket, add a trailing slash after the bucket name
(e.g. `obs://OBS_BUCKET/`).

Specify the OBS credentials for OBS access by setting `fs.obs.accessKey` and `fs.obs.secretKey` in
`alluxio-site.properties`.

```
fs.obs.accessKey=<OBS ACCESS KEY>
fs.obs.secretKey=<OBS SECRET KEY>
```

Specify the OBS region by setting `fs.obs.endpoint` in `alluxio-site.properties` (e.g. obs.cn-north-4.myhuaweicloud.com).

```
fs.obs.endpoint=<OBS ENDPOINT>
```

After these changes, Alluxio should be configured to work with OBS as its under storage system, and
you can try [Running Alluxio Locally with OBS](#running-alluxio-locally-with-obs).


## Running Alluxio Locally with OBS

Start the Alluxio servers:

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This will start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Before running an example program, please make sure the root mount point
set in the `conf/alluxio-site.properties` is a valid path in the ufs.
Make sure the user running the example program has write permissions to the alluxio file system.

```console
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Nested Mount

An OBS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{ '/en/operation/User-CLI.html' | relativize_url }}#mount) can be used for this purpose.
For example, the following command mounts a directory inside an OBS bucket into Alluxio directory
`/obs`:

```console
$ ./bin/alluxio fs mount --option fs.obs.accessKey=<OBS ACCESS KEY> \
  --option fs.obs.secretKey=<OBS SECRET KEY> \
  --option fs.obs.endpoint=<OBS_ENDPOINT> \
  /obs obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

### [Experimental] OBS multipart upload

The default upload method uploads one file completely from start to end in one go. We use multipart-upload method to upload one file by multiple parts, every part will be uploaded in one thread. It won't generate any temporary files while uploading.

To enable OBS multipart upload, you need to modify `conf/alluxio-site.properties` to include:

```
alluxio.underfs.obs.multipart.upload.enabled=true
```

There are other parameters you can specify in `conf/alluxio-site.properties` to make the process faster and better.

```properties
# Timeout for uploading part when using multipart upload.
alluxio.underfs.object.store.multipart.upload.timeout

# Thread pool size for OBS multipart upload.
alluxio.underfs.obs.multipart.upload.threads

# Multipart upload partition size for OBS. The default partition size is 64MB. 
alluxio.underfs.obs.multipart.upload.partition.size
```



