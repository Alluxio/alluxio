---
layout: global
title: Configuring Alluxio with OBS
nickname: Alluxio with OBS
group: Under Store
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with
[Open Telecom Cloud OBS](http://www.huaweicloud.com/en-us/product/obs.html) as the under storage system. Object Storage
Service (OBS) is a massive, secure and highly reliable cloud storage service provided by Huawei Cloud.

## Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines. You can either
[compile the binaries from Alluxio source code]({{site.baseurl}}{% link
en/contributor/Building-Alluxio-From-Source.md %}), or [download the precompiled binaries
directly]({{site.baseurl}}{% link en/deploy/Running-Alluxio-Locally.md %}).

OBS under storage is implemented as an under storage extension. A precompiled OBS under storage jar can be downloaded from [here](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/obs/target).

Then execute the following command on master to install the extension to all masters and workers defined in `conf/masters` and `conf/workers`:

```bash
$ bin/alluxio extensions install /PATH/TO/DOWNLOADED/OBS/jar
```

See [this doc]({{site.baseurl}}{% link en/ufs/Ufs-Extensions.md %}) for more details on Alluxio extension management.

A bucket and directory in OBS should exist before mounting OBS to Alluxio, create them if they do not exist.
Suppose the bucket is named `OBS_BUCKET` and the directory is named `OBS_DIRECTORY`.
Please refer to [this doc](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0046535383.html) for more
information on creating a bucket in OBS.

The OBS endpoint specifying the region of your bucket should also be set in Alluxio configurations.
Suppose the endpoint is named `OBS_ENDPOINT`.
Please refer to [this doc](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0075679174.html) for more
information on different regions and endpoints in OBS.

## Mounting OBS

Alluxio unifies access to different storage systems through the
[unified namespace]({{site.baseurl}}{% link en/advanced/Namespace-Management.md %}) feature. An OBS location can be
either mounted at the root of the Alluxio namespace or at a nested directory.

### Root Mount

You need to configure Alluxio to use OBS as its under storage system. The first modification is to
specify an existing OBS bucket and folder as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.underfs.address=obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

Next you need to specify the Huawei Cloud credentials for OBS access. In `conf/alluxio-site.properties`,
add:

```
fs.obs.accessKey=<OBS_ACCESS_KEY>
fs.obs.secretKey=<OBS_SECRET_KEY>
fs.oss.endpoint=<OBS_ENDPOINT>
```

Here `fs.obs.accessKey` is the Access Key string and `fs.obs.secretKey` is the Secret Key
string, please refer to [help on managing access keys](http://support.huaweicloud.com/en-us/usermanual-ca/en-us_topic_0046606340.html).
`fs.obs.endpoint` is the endpoint of this bucket, please refer to [this doc](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0075679174.html).

After these changes, Alluxio should be configured to work with OBS as its under storage system,
and you can try to run alluxio locally with OBS.

### Nested Mount
An OBS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#mount) can be used for
this purpose. For example, the following command mounts a folder inside an OBS bucket into Alluxio
directory `/obs`:

```bash
$ ./bin/alluxio fs mount --option fs.obs.accessKey=<OBS_ACCESS_KEY> \
  --option fs.obs.secretKey=<OBS_SECRET_KEY> \
  --option fs.obs.endpoint=<OBS_ENDPOINT> \
  /obs obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

## Running Alluxio Locally with OBS

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

After this succeeds, you can visit your OBS folder `obs://<OBS_BUCKET>/<OBS_DIRECTORY>` to verify the files
and directories created by Alluxio exist.

To stop Alluxio, you can run:

```bash
$ bin/alluxio-stop.sh local
```
