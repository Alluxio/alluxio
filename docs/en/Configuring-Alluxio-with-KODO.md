---
layout: global
title: Configuring Alluxio with Kodo
nickname: Alluxio with Kodo
group: Under Store
priority: 4
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with
[Qiniu Kodo](https://www.qiniu.com/products/kodo) as the under storage system. Qiniu Object Storage
Service (Kodo) is a massive, secure and highly reliable cloud storage service.
## Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines.You can
[compile the binaries from Alluxio source code](Building-Alluxio-From-Source.html),
or [download the precompiled binaries directly](Running-Alluxio-Locally.html).

A Qiniu Kodo bucket is necessary before using Kodo with alluxio. In this guide, the Qiniu Kodo bucket
is called `KODO_BUCKET`, and the directory in the bucket is called `KODO_DIRECTORY`. In addition, you should provide a domain to identify the specified bucket, which is called `KODO_DOWNLOAD_HOST`.
 Through the `KODO_DOWNLOAD_HOST` you can get objects from the bucket.
## Mounting Kodo

Alluxio unifies access to different storage systems through the
[unified namespace](Unified-and-Transparent-Namespace.html) feature. The root of Alluxio namespace or its subdirectories are all available for the mount point of Kodo.
### Root Mount

If you want to use Qiniu Kodo as its under storage system in Alluxio, `conf/alluxio-site.properties` must be modified. In the beginning, an existing Kodo bucket and its directory should be specified for storage by the following code:
```
alluxio.master.mount.table.root.ufs=kodo://<KODO_BUCKET>/<KODO_DIRECTORY>/
```
Next ,some settings must be added to `conf/alluxio-site.properties`:
```
fs.kodo.accesskey=<KODO_ACCESS_KEY>
fs.kodo.secretkey=<KODO_SECRET_KET>
alluxio.underfs.kodo.downloadhost=<KODO_DOWNLOAD_HOST>
alluxio.underfs.kodo.endpoint=<KODO_ENDPOINT>

```
`AccessKey/SecretKey` can be found in [Qiniu Console - Key Management](https://portal.qiniu.com/user/key)

`alluxio.underfs.kodo.downloadhost` can be found in [Qiniu Console - Kodo](https://portal.qiniu.com/bucket) 
according to this [order](https://mars-assets.qnssl.com/alluxio_host.png)

`alluxio.underfs.kodo.endpoint` is the endpoint of this bucket, which can be found in the Bucket in this table:

| Region | Abbreviation| EndPoint |
| ------- | -------- | --------- |
|East China| z0|  iovip.qbox.me | 
|North China| z1| iovip-z1.qbox.me| 
|South China| z2| iovip-z2.qbox.me | 
|North America| na0| iovip-na0.qbox.me | 
|Southeast Asia| as0| iovip-as0.qbox.me |

### Nested Mount
An Kodo location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command](Command-Line-Interface.html#mount) can be used for this purpose.
For example, the following command mounts a directory inside an Kodo bucket into Alluxio directory

```bash 
./bin/alluxio fs mount --option fs.kodo.accessKey=<KODO_ACCESS_KEY> \
  --option fs.kodo.secretkey=<KODO_SECRET_KET> \
  --option alluxio.underfs.kodo.downloadhost=<KODO_DOWNLOAD_HOST> \
  --option alluxio.underfs.kodo.endpoint=<KODO_ENDPOINT> \
  kodo/ kodo://<KODO_BUCKET>/<KODO_DIRECTORY>/
```
## Running Alluxio Locally with Kodo

After everything is configured, you can start up Alluxio locally to see that everything works.

```bash
./bin/alluxio format
./bin/alluxio-start.sh local
```
This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
./bin/alluxio runTests
```
After this succeeds, you can visit your Kodo directory `kodo://<KODO_BUCKET>/<KODO_DIRECTORY>` to verify the files
and directories mounted by Alluxio exist. For this test, you should see files named like
`KODO_BUCKET/KODO_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

To stop Alluxio, you can run:
```bash
./bin/alluxio-stop.sh local
```
