---
layout: global
title: Configuring Alluxio with OBS
nickname: Alluxio with OBS
group: Under Store
priority: 5
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with
[Huawei OBS](http://www.huaweicloud.com/en-us/product/obs.html) as the under storage system. Object Storage
Service (OBS) is a massive, secure and highly reliable cloud storage service provided by Huawei Cloud.

## Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines.You can either
[compile the binaries from Alluxio source code](http://alluxio.org/documentation/master/Building-Alluxio-Master-Branch.html),
or [download the precompiled binaries directly](http://alluxio.org/documentation/master/Running-Alluxio-Locally.html).

Also, in preparation for using OBS with alluxio, create a bucket or use an existing bucket. You
should also note that the directory you want to use in that bucket, either by creating a new
directory in the bucket, or using an existing one. For the purposes of this guide, the OBS bucket
name is called `OBS_BUCKET`, and the directory in that bucket is called `OBS_DIRECTORY`. Also, for
using the OBS Service, you should provide an OBS endpoint to specify which region your bucket is
on. The endpoint here is called `OBS_ENDPOINT`, and to learn more about the different endpoints for different
regions, please see [here](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0075679174.html). For more
information about OBS Bucket, please see [here](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0046535383.html).

## Mounting OBS

Alluxio unifies access to different storage systems through the
[unified namespace](Unified-and-Transparent-Namespace.html) feature. An OBS location can be
either mounted at the root of the Alluxio namespace or at a nested directory.

### Root Mount

You need to configure Alluxio to use OBS as its under storage system. The first modification is to
specify an existing OBS bucket and folder as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.underfs.address=obs://<OBS_BUCKET>/<OBS_FOLDER>/
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
[Mount Command](Command-Line-Interface.html#mount) can be used for this purpose.
For example, the following command mounts a folder inside an OBS bucket into Alluxio directory
`/obs`:

```bash
$ ./bin/alluxio fs mount --option fs.obs.accessKey=<OBS_ACCESS_KEY> \
  --option fs.obs.secretKey=<OBS_SECRET_KEY> \
  --option fs.obs.endpoint=<OBS_ENDPOINT> \
  /obs obs://<OBS_BUCKET>/<OBS_FOLDER>/
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

After this succeeds, you can visit your OBS folder `obs://<OBS_BUCKET>/<OBS_FOLDER>` to verify the files
and directories created by Alluxio exist.

To stop Alluxio, you can run:

```bash
$ bin/alluxio-stop.sh local
```
