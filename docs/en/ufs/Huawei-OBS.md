---
layout: global
title: Huawei Object Storage Service
---


This guide describes the instructions to configure [Huawei OBS](https://www.huaweicloud.com/product/obs){:target="_blank"} as Alluxio's
under storage system.

Huawei Object Storage Service (OBS) is a scalable service that provides secure, reliable, and cost-effective cloud storage for massive amounts of data. OBS provides unlimited storage capacity for objects of any format, catering to the needs of common users, websites, enterprises, and developers.

For more information about Huawei OBS, please read its [documentation](https://support.huaweicloud.com/intl/en-us/obs/index.html){:target="_blank"}.

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using Huawei OBS with Alluxio:
<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<OBS_BUCKET>`</td>
        <td markdown="span">[Create a new bucket](https://support.huaweicloud.com/intl/en-us/qs-obs/obs_qs_0007.html){:target="_blank"} or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OBS_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OBS_ACCESS_KEY>`</td>
        <td markdown="span">Used to authenticate the identity of a requester. See [Obtaining Access Keys (AK and SK)](https://support.huaweicloud.com/intl/en-us/qs-obs/obs_qs_0005.html){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OBS_SECRET_KEY>`</td>
        <td markdown="span">Used to authenticate the identity of a requester. See [Obtaining Access Keys (AK and SK)](https://support.huaweicloud.com/intl/en-us/qs-obs/obs_qs_0005.html){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OBS_ENDPOINT>`</td>
        <td markdown="span">Domain name to access OBS in a region and is used to process requests of that region. See [Regions and Endpoints](https://developer.huaweicloud.com/intl/en-us/endpoint){:target="_blank"}</td>
    </tr>
</table>

## Basic Setup

To use Huawei OBS as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify an **existing** OBS bucket and directory as the underfs addresss system by modifying
`conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=obs://<OBS_BUCKET>/<OBS_DIRECTORY>
```

Note that if you want to mount the whole obs bucket, add a trailing slash after the bucket name
(e.g. `obs://OBS_BUCKET/`).

Specify credentials for OBS access by setting `fs.obs.accessKey` and `fs.obs.secretKey` in
`alluxio-site.properties`.

```properties
fs.obs.accessKey=<OBS_ACCESS_KEY>
fs.obs.secretKey=<OBS_SECRET_KEY>
```

Specify the OBS region by setting `fs.obs.endpoint` in `alluxio-site.properties` (e.g. obs.cn-north-4.myhuaweicloud.com).

```properties
fs.obs.endpoint=<OBS_ENDPOINT>
```

## Running Alluxio Locally with OBS

Once you have configured Alluxio to Azure Blob Store, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

## Advanced Setup

### [Experimental] OBS multipart upload

The default upload method uploads one file completely from start to end in one go. We use multipart-upload method to upload one file by multiple parts, every part will be uploaded in one thread. It won't generate any temporary files while uploading.

To enable OBS multipart upload, you need to modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.obs.multipart.upload.enabled=true
```

There are other parameters you can specify in `conf/alluxio-site.properties` to make the process faster and better.

```properties
# Timeout for uploading part when using multipart upload.
alluxio.underfs.object.store.multipart.upload.timeout
```
```properties
# Thread pool size for OBS multipart upload.
alluxio.underfs.obs.multipart.upload.threads
```
```properties
# Multipart upload partition size for OBS. The default partition size is 64MB. 
alluxio.underfs.obs.multipart.upload.partition.size
```



