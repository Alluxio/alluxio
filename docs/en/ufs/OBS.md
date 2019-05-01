---
layout: global
title: OBS
nickname: OBS
group: Under Stores
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with
[Open Telecom OBS](http://www.huaweicloud.com/en-us/product/obs.html) as the under storage system. Object Storage
Service (OBS) is a massive, secure and highly reliable cloud storage service provided by Huawei Cloud.

## Prerequisites

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines. You can either
[compile the binaries from Alluxio source code]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}),
or [download the precompiled binaries directly]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

OBS under storage is implemented as an under storage extension. A precompiled OBS under storage jar 
can be downloaded from [here](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/obs/target).

Execute the following command on master to install the extension to all masters and workers defined in `conf/masters` and `conf/workers`:

```bash
bin/alluxio extensions install /PATH/TO/DOWNLOADED/OBS/jar
```

See [here]({{ '/en/ufs/Ufs-Extensions.html' | relativize_url }}) for more details on Alluxio extension management.

In preparation for using OBS with Alluxio, follow the [OBS quick start guide](https://support-intl.huaweicloud.com/usermanual-obs/en-us_topic_0069825929.html)
to create a OBS bucket.

## Basic setup

To configure Alluxio to use OBS as its under storage system, you will need to modify the configuration file
`conf/alluxio-site.properties`. If the file does not exist, create the configuration file from the template.

```bash
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

## Excample: Running Alluxio Locally with OBS

Start the Alluxio servers:

```bash
bin/alluxio format
bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```bash
bin/alluxio runTests
```

Visit your OBS folder `obs://<OBS_BUCKET>/<OBS_DIRECTORY>` to verify the files
and directories created by Alluxio exist.

To stop Alluxio, you can run:

```bash
bin/alluxio-stop.sh local
```

## Advanced Setup

### Nested Mount

An OBS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#mount) can be used for
this purpose. 

For example, the following command mounts a folder inside an OBS bucket into Alluxio
directory `/obs`:

```bash
./bin/alluxio fs mount --option fs.obs.accessKey=<OBS_ACCESS_KEY> \
  --option fs.obs.secretKey=<OBS_SECRET_KEY> \
  --option fs.obs.endpoint=<OBS_ENDPOINT> \
  /obs obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```
