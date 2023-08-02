---
layout: global
title: Qiniu Kodo
---


This guide describes how to configure Alluxio with [Qiniu Kodo](https://www.qiniu.com/products/kodo){:target="_blank"} as the under storage system. 

Object Storage (Kodo) is a cloud-based object storage service provided by Qiniu Cloud, a Chinese cloud service provider. Kodo is a massive, secure and highly reliable cloud storge service that is designed to store, manage, and serve large amounts of unstructured data.

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using Qiniu Kodo with Alluxio:
<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<KODO_BUCKET>`</td>
        <td markdown="span">Create a new bucket or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<KODO_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<KODO_ACCESS_KEY>`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<KODO_SECRET_KEY>`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<KODO_DOWNLOAD_HOST>`</td>
        <td markdown="span">Provide a domain to identify the specificed bucket through which you can get objects from the bucket</td>
    </tr>
</table>

In preparation for using Qiniu Kodo with Alluxio, create a new bucket or use an existing bucket. You should also note the directory you want to use in that bucket, either by creating a new directory in the bucket or using an existing one. 

## Basic Setup

To use Qiniu Kodo as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify the underfs address by modifying `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=kodo://<KODO_BUCKET>/<KODO_DIRECTORY>/
```
Specify credentials by adding the following properties in `conf/alluxio-site.properties`:
```properties
fs.kodo.accesskey=<KODO_ACCESS_KEY>
fs.kodo.secretkey=<KODO_SECRET_KEY>
alluxio.underfs.kodo.downloadhost=<KODO_DOWNLOAD_HOST>
alluxio.underfs.kodo.endpoint=<KODO_ENDPOINT>
```
`accessKey/secretKey` can be found in [Qiniu Console - Key Management](https://portal.qiniu.com/user/key){:target="_blank"}

`alluxio.underfs.kodo.downloadhost` can be found in [Qiniu Console - Kodo](https://portal.qiniu.com/bucket){:target="_blank"}

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

Once you have configured Alluxio to Qiniu Kodo, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

## Contributed by the Alluxio Community

Qiniu KODO UFS integration is contributed and maintained by the Alluxio community.
The source code is located [here](https://github.com/Alluxio/alluxio/tree/master/underfs/kodo){:target="_blank"}.
Feel free submit pull requests to improve the integration and update 
the documentation [here](https://github.com/Alluxio/alluxio/edit/master/docs/en/ufs/Qiniu-KODO.md){:target="_blank"} 
if any information is missing or out of date.
