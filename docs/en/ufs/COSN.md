---
layout: global
title: COSN
nickname: COSN
group: Storage Integrations
priority: 5
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with Tencent [COS](https://cloud.tencent.com/product/cos) (Cloud Object Storage) as the under storage system.
Tencent Cloud Object Storage (COS) is a distributed storage service offered by Tencent Cloud for unstructured data and accessible via HTTP/HTTPS protocols.
It can store massive amounts of data and features imperceptible bandwidth and capacity expansion, making it a perfect data pool for big data computation and analytics.

## Basic Setup

Alluxio runs on multiple machines in cluster mode so its binary package needs to be deployed on the machines.
You can either [compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}) or [download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

In preparation for using COS with Alluxio, create a new bucket or use an existing bucket.
You should also note the directory you want to use in that bucket, either by creating a new directory in the bucket or using an existing one.
For the purposes of this guide, the COS Bucket name is called `COSN_ALLUXIO_BUCKET`, the directory in that bucket is called `COSN_DATA`, and COS Bucket region is called `COSN_REGION` which specifies the region of your bucket.

## Basic Setup

Alluxio unifies access to different storage systems through the [unified namespace]({{ '/en/core-services/Unified-Namespace.html' | relativize_url }}) feature.
COSN UFS is used to access Tencent Cloud object storage and a COS location can be either mounted at the root of the Alluxio namespace or as a nested directory.

### Root Mount Point

Create `conf/alluxio-site.properties` and `conf/core-site.xml` if they do not exist.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
$ cp conf/core-site.xml.template conf/core-site.xml
```

Configure Alluxio to use COSN as its under storage system by modifying `conf/alluxio-site.properties` and `conf/core-site.xml`.
Specify an existing COS bucket and directory as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.master.mount.table.root.ufs=cosn://COSN_ALLUXIO_BUCKET/COSN_DATA/
```

Specify COS configuration information in order to access COS by modifying `conf/core-site.xml` to include:

```
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
  <value>xxxx</value>
</property>
<property>
  <name>fs.cosn.userinfo.secretId</name>
  <value>xxxx</value>
</property>
<property>
  <name>fs.cosn.bucket.region</name>
  <value>xx</value>
</property>
```

The above is the most basic configuration. For more configuration please refer to [here](https://hadoop.apache.org/docs/r3.3.0/hadoop-cos/cloud-storage/index.html).
After these changes, Alluxio should be configured to work with COSN as its under storage system and you can try [Running Alluxio Locally with COSN](#running-alluxio-locally-with-cosn).

### Nested Mount

A COS location can be mounted at a nested directory in the Alluxio namespace to have unified access to multiple under storage systems.
The [mount command]({{ '/en/operation/User-CLI.html' | relativize_url }}#mount) can be used for this purpose.

```console
$ ./bin/alluxio fs mount --option fs.cosn.userinfo.secretId=<COSN_SECRET_ID> \
    --option fs.cosn.userinfo.secretKey=<COSN_SECRET_KEY> \
    --option fs.cosn.bucket.region=<COSN_REGION> \
    --option fs.cosn.impl=org.apache.hadoop.fs.CosFileSystem \
    --option fs.AbstractFileSystem.cosn.impl=org.apache.hadoop.fs.CosN \
    /cosn cosn://COSN_ALLUXIO_BUCKET/COSN_DATA/
```

## Running Alluxio Locally with COSN

Start up Alluxio locally to see that everything works.

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at [http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Visit your COS directory at `COSN_ALLUXIO_BUCKET/COSN_DATA` to verify the files and directories created by Alluxio exist.
For this test, you should see files named like:

```console
COSN_ALLUXIO_BUCKET/COSN_DATA/default_tests_files/BASIC_CACHE_THROUGH
```

To stop Alluxio, you can run:

```console
$ ./bin/alluxio-stop.sh local
```
