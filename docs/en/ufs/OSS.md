---
layout: global
title: Aliyun Object Storage Service
nickname: Aliyun Object Storage Service
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure [Aliyun OSS](https://intl.aliyun.com/product/oss) as Alluxio's under storage system. 
Object Storage Service (OSS) is a massive, secure and highly reliable cloud storage service provided by Aliyun.

## Prerequisites

The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

In preparation for using OSS with Alluxio, follow the [OSS quick start guide](https://www.alibabacloud.com/help/doc-detail/31883.htm)
to sign up for OSS and create an OSS bucket.

## Basic Setup

To configure Alluxio to use OSS as under storage, you will need to modify the configuration file 
`conf/alluxio-site.properties`. If the file does not exist, create the configuration file from the template.

```
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Edit `conf/alluxio-site.properties` file to set the under storage address to the OSS bucket and 
the OSS directory you want to mount to Alluxio. For example, the under storage address can be `oss://alluxio-bucket/` if
you want to mount the whole bucket to Alluxio, or `oss://alluxio-bucket/alluxio/data` if only the directory `/alluxio/data`
inside the oss bucket `alluxio-bucket` is mapped to Alluxio.

```
alluxio.master.mount.table.root.ufs=oss://<OSS_BUCKET>/<OSS_DIRECTORY>
``` 

Specify the Aliyun credentials for OSS access. In `conf/alluxio-site.properties`, add:

```
fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID>
fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET>
fs.oss.endpoint=<OSS_ENDPOINT>
```

`fs.oss.accessKeyId` and `fs.oss.accessKeySecret` is the [AccessKey](https://www.alibabacloud.com/help/doc-detail/29009.htm) for OSS, 
which are created and managed in [Aliyun AccessKey management console](https://ak-console.aliyun.com).

`fs.oss.endpoint` is the internet endpoint of this bucket, which can be found in the bucket overview page with
values like `oss-us-west-1.aliyuncs.com` and `oss-cn-shanghai.aliyuncs.com`. Available endpoints are listed in the 
[OSS Internet Endpoints documentation](https://intl.aliyun.com/help/doc-detail/31837.htm).

## Example: Running Alluxio Locally with OSS

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

Visit your OSS directory `oss://<OSS_BUCKET>/<OSS_DIRECTORY>` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like
`<OSS_BUCKET>/<OSS_DIRECTORY>/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

Stop Alluxio by running:

```console
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Nested Mount

An OSS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{ '/en/operation/User-CLI.html' | relativize_url }}#mount) can be used for this purpose.
For example, the following command mounts a directory inside an OSS bucket into Alluxio directory
`/oss`:

```console
$ ./bin/alluxio fs mount --option fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID> \
  --option fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET> \
  --option fs.oss.endpoint=<OSS_ENDPOINT> \
  /oss oss://<OSS_BUCKET>/<OSS_DIRECTORY>/
```
