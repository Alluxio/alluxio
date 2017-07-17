---
layout: global
title: Configuring Alluxio with OSS
nickname: Alluxio with OSS
group: Under Store
priority: 4
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with
[Aliyun OSS](https://intl.aliyun.com/product/oss) as the under storage system. Object Storage
Service (OSS) is a massive, secure and highly reliable cloud storage service provided by Aliyun.

## Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines.You can either
[compile the binaries from Alluxio source code](http://alluxio.org/documentation/master/Building-Alluxio-Master-Branch.html),
or [download the precompiled binaries directly](http://alluxio.org/documentation/master/Running-Alluxio-Locally.html).

Also, in preparation for using OSS with alluxio, create a bucket or use an existing bucket. You
should also note that the directory you want to use in that bucket, either by creating a new
directory in the bucket, or using an existing one. For the purposes of this guide, the OSS bucket
name is called `OSS_BUCKET`, and the directory in that bucket is called `OSS_DIRECTORY`. Also, for
using the OSS Service, you should provide an OSS endpoint to specify which range your bucket is
on. The endpoint here is called `OSS_ENDPOINT`, and to learn more about the endpoints for special
range you can see [here](https://intl.aliyun.com/help/en/doc-detail/31834.htm). For more
information about OSS Bucket, Please see [here](https://intl.aliyun.com/help/doc-detail/31885.htm)

## Mounting OSS

Alluxio unifies access to different storage systems through the 
[unified namespace](Unified-and-Transparent-Namespace.html) feature. An OSS location can be 
either mounted at the root of the Alluxio namespace or at a nested directory. 

### Root Mount

You need to configure Alluxio to use OSS as its under storage system. The first modification is to
specify an existing OSS bucket and directory as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.underfs.address=oss://OSS_BUCKET/OSS_DIRECTORY/
```

Next you need to specify the Aliyun credentials for OSS access. In `conf/alluxio-site.properties`,
add:

```
fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID>
fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET>
fs.oss.endpoint=<OSS_ENDPOINT>
```

Here `fs.oss.accessKeyId` is the Access Key Id string and `fs.oss.accessKeySecret` is the Access
Key Secret string, which are managed in [AccessKeys](https://ak-console.aliyun.com) in Aliyun UI.
`fs.oss.endpoint` is the endpoint of this bucket, which can be found in the Bucket overview with
possible values like "oss-us-west-1.aliyuncs.com", "oss-cn-shanghai.aliyuncs.com"
([OSS Internet Endpoint](https://intl.aliyun.com/help/doc-detail/31837.htm)).

After these changes, Alluxio should be configured to work with OSS as its under storage system,
and you can try to run alluxio locally with OSS.

### Nested Mount
An OSS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's 
[Command Line Interface](Command-Line-Interface.html) can be used for this purpose.
```bash 
$ ./bin/alluxio fs mount --option fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID> \
  --option fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET>\
  fs.oss.endpoint=<OSS_ENDPOINT>
```

## Running Alluxio Locally with OSS

After everything is configured, you can start up Alluxio locally to see that everything works.

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ bin/alluxio runTests
```

After this succeeds, you can visit your OSS directory OSS_BUCKET/OSS_DIRECTORY to verify the files
and directories created by Alluxio exist. For this test, you should see files named like
`OSS_BUCKET/OSS_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

To stop Alluxio, you can run:

```bash
$ bin/alluxio-stop.sh local
```
