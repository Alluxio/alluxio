---
layout: global
title: Tinder Object Storage Service
nickname: Tinder Object Storage Service
group: Storage Integrations
priority: 10
---

* Table of Contents
  {:toc}

This guide describes how to configure [Tinder Object Storage Service (TOS)](https://www.volcengine.com/product/TOS) as Alluxio's under storage system. Tinder Object Storage Service (TOS) is a massive, secure, low-cost, easy-to-use, highly reliable, and highly available distributed cloud storage service provided by VolcEngine.

## Prerequisites

The Alluxio binaries must be installed on your machine. You can either [compile Alluxio from source]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or [download the binaries locally]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

Before using TOS with Alluxio, follow the [TOS quick start guide](https://www.volcengine.com/docs/6349/74830) to sign up for TOS and create a TOS bucket.

## Basic Setup

To configure Alluxio to use TOS as under storage, you will need to modify the configuration file `conf/alluxio-site.properties`. If the file does not exist, create the configuration file from the template.

```
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Edit the `conf/alluxio-site.properties` file to set the under storage address to the TOS bucket and the TOS directory you want to mount to Alluxio. For example, the under storage address can be `tos://alluxio-bucket/` if you want to mount the whole bucket to Alluxio, or `tos://alluxio-bucket/alluxio/data` if only the directory `/alluxio/data` inside the TOS bucket `alluxio-bucket` is mapped to Alluxio.

```
alluxio.master.mount.table.root.ufs=tos://<TOS_BUCKET>/<TOS_DIRECTORY>
``` 

Specify the VolcEngine credentials for TOS access. In `conf/alluxio-site.properties`, add:

```
fs.tos.accessKeyId=<TOS_ACCESS_KEY_ID>
fs.tos.accessKeySecret=<TOS_ACCESS_KEY_SECRET>
fs.tos.endpoint=<TOS_ENDPOINT>
fs.tos.region=<TOS_REGION>
```

`fs.tos.accessKeyId` and `fs.tos.accessKeySecret` are the [AccessKey](https://www.volcengine.com/docs/6291/65568) for TOS, which are created and managed in the [TOS AccessKey management console](https://console.volcengine.com/iam/keymanage/).

`fs.tos.endpoint` is the internet endpoint of this bucket, which can be found in the bucket overview page with values like `tos-cn-beijing.volces.com` and `tos-cn-guangzhou.volces.com`. Available endpoints are listed in the [TOS Internet Endpoints documentation](https://www.volcengine.com/docs/6349/107356).

`fs.tos.region` is the region where the bucket is located, such as `cn-beijing` or `cn-guangzhou`. Available regions are listed in the [TOS Regions documentation](https://www.volcengine.com/docs/6349/107356).

## Example: Running Alluxio Locally with TOS

Start the Alluxio servers:

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This will start an Alluxio master and an Alluxio worker. You can see the master UI at [http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Visit your TOS directory `tos://<TOS_BUCKET>/<TOS_DIRECTORY>` to verify the files and directories created by Alluxio exist. For this test, you should see files named like `<TOS_BUCKET>/<TOS_DIRECTORY>/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

Stop Alluxio by running:

```console
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Nested Mount

A TOS location can be mounted at a nested directory in the Alluxio namespace to have unified access to multiple under storage systems. Alluxio's [Mount Command]({{ '/cn/operation/User-CLI.html' | relativize_url }}#mount) can be used for this purpose. For example, the following command mounts a directory inside a TOS bucket into the Alluxio directory `/tos`:

```console
$ ./bin/alluxio fs mount --option fs.tos.accessKeyId=<TOS_ACCESS_KEY_ID> \
  --option fs.tos.accessKeySecret=<TOS_ACCESS_KEY_SECRET> \
  --option fs.tos.endpoint=<TOS_ENDPOINT> \
  /tos tos://<TOS_BUCKET>/<TOS_DIRECTORY>/
```

### TOS Streaming Upload

Due to the nature of TOS as an object storage service, files are uploaded by being sent from the client to the Worker node and stored in a temporary directory on the local disk, by default being uploaded to TOS in the `close()` method.

To enable streaming upload, add the following configuration in `conf/alluxio-site.properties`:

```
alluxio.underfs.tos.streaming.upload.enabled=true
```

The default upload process is safer but has the following issues:

- Slower upload speed. Files must be sent to the Alluxio worker, which then uploads the files to TOS. These two processes are sequential.
- The temporary directory must have the capacity to store the entire file.
- If the upload is interrupted, the file may be lost.

The TOS streaming upload feature solves these problems.

Advantages of TOS streaming upload:

- Shorter upload time. The Alluxio worker uploads buffered data while receiving new data. The total upload time is at least as fast as the default method.
- Less capacity required, as data is cached and uploaded in partitions (default partition size is 64MB). Once a partition is successfully uploaded, it is deleted.
- Faster `close()` method: The `close()` method execution time is greatly reduced since the file upload is already completed during the write process.

If TOS streaming upload is interrupted, intermediate partitions may be uploaded to TOS, and TOS will charge for these data.

To reduce costs, you can modify `conf/alluxio-site.properties` to include:

```
alluxio.underfs.cleanup.enabled=true
```

When Alluxio detects the cleanup interval, all intermediate partitions in non-read-only TOS mount points that exceed the cleanup age will be cleaned up.

### High Concurrency Tuning

When integrating Alluxio with TOS, you can optimize performance by adjusting the following configurations:

- `alluxio.underfs.tos.retry.max`: Controls the number of retries with TOS. Default value is 3.
- `alluxio.underfs.tos.read.timeout`: Controls the read timeout with TOS. Default value is 30000 milliseconds.
- `alluxio.underfs.tos.write.timeout`: Controls the write timeout with TOS. Default value is 30000 milliseconds.
- `alluxio.underfs.tos.streaming.upload.partition.size`: Controls the partition size for TOS streaming upload. Default value is 64MB.
- `alluxio.underfs.tos.connect.timeout`: Controls the connection timeout with TOS. Default value is 30000 milliseconds.
