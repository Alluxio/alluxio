---
layout: global
title: OBS
nickname: OBS
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}
This guide describes how to configure Alluxio with
[Open Telecom OBS](http://www.huaweicloud.com/en-us/product/obs.html) as the under storage system. Object Storage
Service (OBS) is a massive, secure and highly reliable cloud storage service provided by Huawei Cloud.

## Prerequisites

The Alluxio binaries must be available on the machine.

In preparation for using OBS with Alluxio, follow the [OBS quick start guide](https://support-intl.huaweicloud.com/usermanual-obs/en-us_topic_0069825929.html) to create a OBS bucket. In this guide, the OBS bucket is called `OBS_BUCKET`, and the directory in the bucket is called `OBS_DIRECTORY`.
Alluxio also supports `PFS`(Parallel File System),  witch is an extended file system of OBS. Please see the next section for configuration.

## Basic setup

To configure Alluxio to use OBS as its under storage system, you will need to modify the configuration file
`conf/alluxio-site.properties`. If the file does not exist, create the configuration file from the template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Edit `conf/alluxio-site.properties` file to set the under storage address to an existing OBS bucket and folder you want to mount to Alluxio.
For example, the under storage address can be `obs://alluxio-obs-bucket/` to mount the whole bucket to Alluxio 
or `obs://alluxio-obs-bucket/alluxio/data` if only the folder `/alluxio/data` inside the bucket `obs://alluxio-obs-bucket` 
is mapped to Alluxio.

```
alluxio.master.mount.table.root.ufs=obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

Specify the Huawei Cloud credentials for OBS access. In `conf/alluxio-site.properties`,
add:

```
fs.obs.accessKey=<OBS_ACCESS_KEY>
fs.obs.secretKey=<OBS_SECRET_KEY>
fs.oss.endpoint=<OBS_ENDPOINT>
```

`fs.obs.accessKey` and `fs.obs.secretKey` is access keys for Huawei cloud, please follow the 
[Access keys Guide](http://support.huaweicloud.com/en-us/usermanual-ca/en-us_topic_0046606340.html) 
to create and manage your keys.

`fs.obs.endpoint` specifies the endpoint of the bucket. 
Valid values are similar to `obs.cn-east-2.myhuaweicloud.com` and `obs.cn-south-1.myhuaweicloud.com`. 
Available endpoints are listed in the [Region and Endpoint list](https://developer.huaweicloud.com/en-us/endpoint)
under the Object Storage Service category.

If you want to use the `PFS` parallel file system, please add the following configuration in `conf/alluxio-site.properties`. The rest of the operations are the same as the `OBS` file system. Add:

```
# default bucket type is obs
fs.obs.bucketType=pfs
```

## Excample: Running Alluxio Locally with OBS

Start the Alluxio servers:

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Visit your OBS folder `obs://<OBS_BUCKET>/<OBS_DIRECTORY>` to verify the files
and directories created by Alluxio exist.

To stop Alluxio, you can run:

```console
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Nested Mount

An OBS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{ '/en/operation/User-CLI.html' | relativize_url }}#mount) can be used for
this purpose. 

For example, the following command mounts a folder inside an OBS bucket into Alluxio
directory `/obs`:

```console
$ ./bin/alluxio fs mount --option fs.obs.accessKey=<OBS_ACCESS_KEY> \
  --option fs.obs.secretKey=<OBS_SECRET_KEY> \
  --option fs.obs.endpoint=<OBS_ENDPOINT> \
  /obs obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

## Contributed by the Alluxio Community

OBS UFS integration is contributed and maintained by the Alluxio community.
The source code is located [here](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/obs).
Feel free submit pull requests to improve the integration and update 
the documentation [here](https://github.com/Alluxio/alluxio/edit/master/docs/en/ufs/OBS.md) 
if any information is missing or out of date.
