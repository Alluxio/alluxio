---
layout: global
title: Minio
nickname: Minio
group: Under Stores
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [Minio](https://minio.io/) as the
under storage system. Alluxio natively provides the `s3a://` scheme (recommended for better
performance). You can use this scheme to connect Alluxio with Minio server.

## Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{site.baseurl}}{% link en/contributor/Building-Alluxio-From-Source.md %}), or
[download the binaries locally]({{site.baseurl}}{% link en/deploy/Running-Alluxio-Locally.md %}).

## Setup Minio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Minio is an object storage server built for cloud applications and DevOps. Minio provides an open
source alternative to AWS S3.

Launch a Minio server instance using the steps mentioned
[here](http://docs.minio.io/docs/minio-quickstart-guide). Then create a bucket (or use an existing
bucket). Once the server is launched, keep a note of Minio server endpoint, accessKey and secretKey.

You should also note the directory you want to use in that bucket, either by creating
a new directory in the bucket, or using an existing one. For the purposes of this guide, the Minio
bucket name is called `MINIO_BUCKET`, and the directory in that bucket is called `MINIO_DIRECTORY`.

## Configuring Alluxio

You need to configure Alluxio to use Minio as its under storage system by modifying
`conf/alluxio-site.properties`. The first modification is to specify an **existing** Minio
bucket and directory as the under storage system.

All the fields to be modified in `conf/alluxio-site.properties` file are listed here:

```properties
alluxio.underfs.address=s3a://<MINIO_BUCKET>/<MINIO_DIRECTORY>
alluxio.underfs.s3.endpoint=http://<MINIO_ENDPOINT>/
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3a.inherit_acl=false
aws.accessKeyId=<MINIO_ACCESS_KEY_ID>
aws.secretKey=<MINIO_SECRET_KEY_ID>
```

For these parameters, replace `<MINIO_ENDPOINT>` with the hostname and port of your Minio service,
e.g., `http://localhost:9000`. If the port value is left unset, it defaults to port 80 for `http`
and 443 for `https`.
