---
layout: global
title: Baidu Cloud Object Storage Service
nickname: Baidu Cloud Object Storage Service
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure [Baidu Cloud BOS](https://cloud.baidu.com/product/bos.html) as Alluxio's under storage system. 
Baidu Cloud Object Storage Service (BOS) is a stable, secure, efficient, and highly scalable cloud storage service provided by Baidu Cloud. You can store arbitrary quantity of any form of unstructured data in BOS, then manage and process the data. BOS supports standard, low frequency, cold storage and other storage types to meet with the storage needs of your various scenarios.

## Prerequisites

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines. You can either [download the precompiled binaries directly]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }})
with the correct Hadoop version (recommended), or 
[compile the binaries from Alluxio source code]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}) (for advanced users).

In preparation for using BOS with Alluxio, follow the [BOS quick start guide](https://cloud.baidu.com/doc/BOS/GettingStarted.html)
to sign up for BOS and create a bos bucket.

## Basic Setup

To configure Alluxio to use BOS as under storage, you will need to modify the configuration file 
`conf/alluxio-site.properties`. If the file does not exist, create the configuration file from the template.

```
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Edit `conf/alluxio-site.properties` file to set the under storage address to the BOS bucket and 
the BOS directory you want to mount to Alluxio. For example, the under storage address can be `bos://alluxio-bucket/` if
you want to mount the whole bucket to Alluxio, or `bos://alluxio-bucket/alluxio/data` if only the directory `/alluxio/data`
inside the bos bucket `alluxio-bucket` is mapped to Alluxio.

```
alluxio.underfs.address=bos://<BOS_BUCKET>/<BOS_DIRECTORY>
``` 

Specify the Baidu Cloud credentials for BOS access. In `conf/alluxio-site.properties`, add:

```
fs.bos.accessKeyId=<BOS_ACCESS_KEY_ID>
fs.bos.accessKeySecret=<BOS_ACCESS_KEY_SECRET>
fs.bos.endpoint=<BOS_ENDPOINT>
```

`fs.bos.accessKeyId` and `fs.bos.accessKeySecret` is the [AccessKey](https://cloud.baidu.com/doc/Reference/GetAKSK.html) for BOS, 
which are created and managed in [Baidu Cloud AccessKey management console](https://console.bce.baidu.com/iam/#/iam/accesslist).

`fs.bos.endpoint` is the endpoint of this bucket, which can be found in the bucket overview page with
values like `bj.bcebos.com` and `gz.bcebos.com`. Available endpoints are listed in the 
[BOS Endpoints documentation](https://cloud.baidu.com/doc/BOS/GettingStarted-new.html).

## Example: Running Alluxio Locally with BOS

Start the Alluxio servers:

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

This will start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```bash
$ bin/alluxio runTests
```

Visit your BOS directory `bos://<BOS_BUCKET>/<BOS_DIRECTORY>` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like
`<BOS_BUCKET>/<BOS_DIRECTORY>/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

Stop Alluxio by running:

```bash
$ bin/alluxio-stop.sh local
```

## Advanced Setup

### Nested Mount

An BOS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{ '/en/operation/User-CLI.html' | relativize_url }}#mount) can be used for this purpose.
For example, the following command mounts a directory inside an BOS bucket into Alluxio directory
`/bos`:

```bash
$ ./bin/alluxio fs mount --option fs.bos.accessKeyId=<BOS_ACCESS_KEY_ID> \
  --option fs.bos.accessKeySecret=<BOS_ACCESS_KEY_SECRET> \
  --option fs.bos.endpoint=<BOS_ENDPOINT> \
  /bos bos://<BOS_BUCKET>/<BOS_DIRECTORY>/
```
