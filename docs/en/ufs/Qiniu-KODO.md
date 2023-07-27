---
layout: global
title: Qiniu Kodo
---


This guide describes how to configure Alluxio with [Qiniu Kodo](https://www.qiniu.com/products/kodo) as the under storage system. 

Object Storage (Kodo) is a cloud-based object storage service provided by Qiniu Cloud, a Chinese cloud service provider. Kodo is a massive, secure and highly reliable cloud storge service that is designed to store, manage, and serve large amounts of unstructured data.

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using Qiniu Kodo with Alluxio, create a new bucket or use an existing bucket. You should also note the directory you want to use in that bucket, either by creating a new directory in the bucket or using an existing one. 

<!-- A Qiniu Kodo bucket is necessary before using Kodo with Alluxio. In this guide, the Qiniu Kodo bucket
is called `KODO_BUCKET`, and the directory in the bucket is called `KODO_DIRECTORY`. -->

In addition, you should provide a domain to identify the specified bucket, which is called `KODO_DOWNLOAD_HOST`, through which you can get objects from the bucket.

## Mounting Kodo

Alluxio unifies access to different storage systems through the
unified namespace feature.
The root of Alluxio namespace or its subdirectories are all available for the mount point of Kodo.

If you want to use Qiniu Kodo as its under storage system in Alluxio, you need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

In the beginning, an existing Kodo bucket and its directory should be specified for storage:
```properties
alluxio.dora.client.ufs.root=kodo://<KODO_BUCKET>/<KODO_DIRECTORY>/
```
Next, some settings must be added to `conf/alluxio-site.properties`:
```properties
fs.kodo.accesskey=<KODO_ACCESS_KEY>
fs.kodo.secretkey=<KODO_SECRET_KEY>
alluxio.underfs.kodo.downloadhost=<KODO_DOWNLOAD_HOST>
alluxio.underfs.kodo.endpoint=<KODO_ENDPOINT>
```
`accessKey/secretKey` can be found in [Qiniu Console - Key Management](https://portal.qiniu.com/user/key)

`alluxio.underfs.kodo.downloadhost` can be found in [Qiniu Console - Kodo](https://portal.qiniu.com/bucket)

`alluxio.underfs.kodo.endpoint` is the endpoint of this bucket, which can be found in the bucket in this table:

<table class="table table-striped">
<tr>
    <th>Region</th>
    <th>Abbreviation</th>
    <th>EndPoint</th>
</tr>
<tr>
    <td markdown="span">East China</td>
    <td markdown="span">z0</td>
    <td markdown="span">`iovip.qbox.me`</td>
</tr>
<tr>
    <td markdown="span">North China</td>
    <td markdown="span">z1</td>
    <td markdown="span">`iovip-z1.qbox.me`</td>
</tr>
<tr>
    <td markdown="span">South China</td>
    <td markdown="span">z2</td>
    <td markdown="span">`iovip-z2.qbox.me`</td>
</tr>
<tr>
    <td markdown="span">North America</td>
    <td markdown="span">na0</td>
    <td markdown="span">`iovip-na0.qbox.me`</td>
</tr>
<tr>
    <td markdown="span">Southeast Asia</td>
    <td markdown="span">as0</td>
    <td markdown="span">`iovip-as0.qbox.me`</td>
</tr>
</table>

## Running Alluxio Locally with Kodo

After everything is configured, you can start up Alluxio locally to see that everything works.

```shell
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```
This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```shell
$ ./bin/alluxio runTests
```
After this succeeds, you can visit your Kodo directory `kodo://<KODO_BUCKET>/<KODO_DIRECTORY>` to verify the files
and directories mounted by Alluxio exist. For this test, you should see files named like

```shell
KODO_BUCKET/KODO_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE
```

To stop Alluxio, you can run:
```shell
$ ./bin/alluxio-stop.sh local
```

## Contributed by the Alluxio Community

Qiniu KODO UFS integration is contributed and maintained by the Alluxio community.
The source code is located [here](https://github.com/Alluxio/alluxio/tree/master/underfs/kodo).
Feel free submit pull requests to improve the integration and update 
the documentation [here](https://github.com/Alluxio/alluxio/edit/master/docs/en/ufs/Qiniu-KODO.md) 
if any information is missing or out of date.
