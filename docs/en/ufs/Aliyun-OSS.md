---
layout: global
title: Aliyun Object Storage Service
---


This guide describes how to configure [Aliyun OSS](https://intl.aliyun.com/product/oss){:target="_blank"} as Alluxio's under storage system. 

Aliyun Object Storage Service (OSS) is a massive, secure and highly reliable cloud storage service provided by Alibaba Cloud. OSS provides multiple storage classes to help you manage and reduce storage costs.

For more information about Aliyun OSS, please read its [documentation](https://www.alibabacloud.com/help/en/oss/){:target="_blank"}

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using OSS with Alluxio:
<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<OSS_BUCKET>`</td>
        <td markdown="span">[Create a a new bucket in the OSS console](https://www.alibabacloud.com/help/en/oss/getting-started/create-buckets-6#task-2013189){:target="_blank"} or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OSS_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OSS_ACCESS_KEY_ID>`</td>
        <td markdown="span">ID used to identify a user. See [How to Obtain AccessKey Pair](https://www.alibabacloud.com/help/en/sls/developer-reference/accesskey-pair){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OSS_ACCESS_KEY_SECRET>`</td>
        <td markdown="span">Secret is used to verify the identify of the user. See [How to Obtain AccessKey Pair](https://www.alibabacloud.com/help/en/sls/developer-reference/accesskey-pair){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OSS_ENDPOINT>`</td>
        <td markdown="span">Endpoints are the domain names that other services can use to access OSS. See [Regions and OSS Endpoints in the Public Cloud](https://www.alibabacloud.com/help/en/oss/user-guide/regions-and-endpoints){:target="_blank"}</td>
    </tr>
</table>

## Basic Setup

To use Aliyun OSS as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```
Specify an OSS bucket and directory as the underfs address by modifying `conf/alluxio-site.properties`. 

For example, the under storage address can be `oss://alluxio-bucket/` if
you want to mount the whole bucket to Alluxio, or `oss://alluxio-bucket/alluxio/data` if only the directory `/alluxio/data`
inside the oss bucket `alluxio-bucket` is mapped to Alluxio.

```properties
alluxio.dora.client.ufs.root=oss://<OSS_BUCKET>/<OSS_DIRECTORY>
``` 

Specify credentials for Aliyun OSS access by adding the following properties in `conf/alluxio-site.properties`:

```properties
fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID>
fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET>
fs.oss.endpoint=<OSS_ENDPOINT>
```

## Running Alluxio Locally with Aliyun OSS

Once you have configured Alluxio to Aliyun OSS, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

## Advanced Setup

### [Experimental] OSS multipart upload

The default upload method uploads one file completely from start to end in one go. We use multipart-upload method to upload one file by multiple parts, every part will be uploaded in one thread. It won't generate any temporary files while uploading.

To enable OSS multipart upload, you need to modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.oss.multipart.upload.enabled=true
```

There are other parameters you can specify in `conf/alluxio-site.properties` to make the process faster and better.

```properties
# Timeout for uploading part when using multipart upload.
alluxio.underfs.object.store.multipart.upload.timeout
```
```properties
# Thread pool size for OSS multipart upload.
alluxio.underfs.oss.multipart.upload.threads
```
```properties
# Multipart upload partition size for OSS. The default partition size is 64MB. 
alluxio.underfs.oss.multipart.upload.partition.size
```
