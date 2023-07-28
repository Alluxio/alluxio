---
layout: global
title: Tencent COS
---

This guide describes the instructions to configure [Tencent COS](https://cloud.tencent.com/product/cos){:target="_blank"} as Alluxio's
under storage system. 

Alluxio support two different implementations of under storage system for Tencent COS:

* [COS](https://cloud.tencent.com/product/cos){:target="_blank"}
: Tencent Cloud Object Storage (COS) is a distributed storage service offered by Tencent Cloud for unstructured data and accessible via HTTP/HTTPS protocols. It can store massive amounts of data and features imperceptible bandwidth and capacity expansion, making it a perfect data pool for big data computation and analytics.
: For more information about Tencent COS, please read its [documentation](https://www.tencentcloud.com/document/product/436){:target="_blank"}.

* [COSN](https://hadoop.apache.org/docs/stable/hadoop-cos/cloud-storage/index.html){:target="_blank"}, also known as Hadoop-COS
: COSN is a client that makes the upper computing systems based on HDFS be able to use Tencent COS as its underlying storage system. 
: For more information about COSN, please read its [documentation](https://www.tencentcloud.com/document/product/436/6884){:target="_blank"}.


## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using COS or COSN with Alluxio:

{% navtabs Prerequisites %}
{% navtab COS %}

<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<COS_BUCKET>`</td>
        <td markdown="span">[Create a new bucket](https://www.tencentcloud.com/document/product/436/32955#step-4.-create-a-bucket){:target="_blank"} or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<COS_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<COS_ACCESS_KEY>`</td>
        <td markdown="span">A developer-owned access key used for the project. It can be obtained at [Manage API Key](https://console.tencentcloud.com/capi){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<COS_SECRET_KEY>`</td>
        <td markdown="span">A developer-owned secret key used for the project. It can be obtained at [Manage API Key](https://console.tencentcloud.com/capi){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<COS_REGION>`</td>
        <td markdown="span">Region information. For more information about the enumerated values, please see [Regions and Access Endpoints](https://www.tencentcloud.com/document/product/436/6224){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<COS_APPID>`</td>
        <td markdown="span">A unique user-level resource identifier for COS access. It can be obtained at [Manage API Key](https://console.tencentcloud.com/capi){:target="_blank"}</td>
    </tr>
</table>

{% endnavtab %}
{% navtab COSN %}

<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<COSN_BUCKET>`</td>
        <td markdown="span">Create a new bucket or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<COSN_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<COSN_SECRET_ID>`</td>
        <td markdown="span">ID used to authenticate user</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<COSN_SECRET_KEY>`</td>
        <td markdown="span">Key used to authenticate user</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<COSN_REGION>`</td>
        <td markdown="span">Region information, for more information about the enumerated values please see [Regions and Access Endpoints](https://www.tencentcloud.com/document/product/436/6224){:target="_blank"}</td>
    </tr>
</table>

{% endnavtab %}
{% endnavtabs %}

## Basic Setup

{% navtabs Setup %}
{% navtab COS %}

To use Tencent COS as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify an **existing** COS bucket and directory as the underfs address by modifying `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=cos://COS_BUCKET/COS_DIRECTORY/
```

Note that if you want to mount the whole cos bucket, add a trailing slash after the bucket name
(e.g. `cos://COS_BUCKET/`).

Specify the COS credentials for COS access by setting `fs.cos.access.key` and `fs.cos.secret.key` in
`alluxio-site.properties`.

```properties
fs.cos.access.key=<COS_ACCESS_KEY>
fs.cos.secret.key=<COS_SECRET_KEY>
```

Specify the COS region by setting `fs.cos.region` in `alluxio-site.properties` (e.g. ap-beijing) and `fs.cos.app.id`.

```properties
fs.cos.region=<COS_REGION>
fs.cos.app.id=<COS_APPID>
```

{% endnavtab %}
{% navtab COSN %}

To use Tencent COSN as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties` and `conf/core-site.xml`. If they do not exist, create the files from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
$ cp conf/core-site.xml.template conf/core-site.xml
```

Specify an **existing** COSN bucket and directory as the underfs address by modifying
`conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=cosn://COSN_BUCKET/COSN_DIRECTORY/
```

Specify COSN configuration information in order to access COSN by modifying `conf/core-site.xml` to include `COSN_SECRET_KEY`, `COSN_SECRET_ID`, AND `COSN_REGION`:

```xml
<property>
   <name>fs.cosn.impl</name>
   <value>org.apache.hadoop.fs.CosFileSystem</value>
</property>
<property>
  <name>fs.AbstractFileSystem.cosn.impl</name>
  <value>org.apache.hadoop.fs.CosN</value>
</property>
<property>
  <name>fs.cosn.userinfo.secretKey</name>
  <value>COSN_SECRET_KEY</value>
</property>
<property>
  <name>fs.cosn.userinfo.secretId</name>
  <value>COSN_SECRET_ID</value>
</property>
<property>
  <name>fs.cosn.bucket.region</name>
  <value>COSN_REGION</value>
</property>
```

The above is the most basic configuration. For more configuration please refer to [here](https://hadoop.apache.org/docs/r3.3.1/hadoop-cos/cloud-storage/index.html){:target="_blank"}.

{% endnavtab %}
{% endnavtabs %}

After these changes, Alluxio should be configured to work with COS or COSN as its under storage system.

## Running Alluxio Locally with COS/COSN

Once you have configured Alluxio to Tencent COS or COSN, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

## Advanced Setup

### [Experimental] COS multipart upload

The default upload method uploads one file completely from start to end in one go. We use multipart-upload method to upload one file by multiple parts, every part will be uploaded in one thread. It won't generate any temporary files while uploading.

To enable COS multipart upload, you need to modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.cos.multipart.upload.enabled=true
```

There are other parameters you can specify in `conf/alluxio-site.properties` to make the process faster and better.

```properties
# Timeout for uploading part when using multipart upload.
alluxio.underfs.object.store.multipart.upload.timeout
```
```properties
# Thread pool size for COS multipart upload.
alluxio.underfs.cos.multipart.upload.threads
```
```properties
# Multipart upload partition size for COS. The default partition size is 64MB. 
alluxio.underfs.cos.multipart.upload.partition.size
```



