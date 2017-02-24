---
layout: global
title: Configuring Alluxio with Minio
nickname: Alluxio with Minio
group: Under Store
priority: 0
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [Minio](https://minio.io/) as the
under storage system. Alluxio natively provides the s3a:// scheme (recommended for better performance). You can
use this scheme to connect Alluxio with Minio server.

## Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file with `bootstrapConf` command.
For example, if you are running Alluxio on your local machine, `ALLUXIO_MASTER_HOSTNAME` should be
set to `localhost`

{% include Configuring-Alluxio-with-Minio/bootstrapConf.md %}

Alternatively, you can also create the configuration file from the template and set the contents
manually.

{% include Common-Commands/copy-alluxio-env.md %}

## Setup Minio

Minio is an object storage server built for cloud applications and DevOps. Minio provides an open source alternative to AWS S3.

Launch a Minio server instance using the steps mentioned [here](http://docs.minio.io/docs/minio-quickstart-guide). Then create a
bucket (or use an existing bucket). Once the server is launched, keep a note of Minio server endpoint, accessKey
and secretKey.

You should also note the directory you want to use in that bucket, either by creating
a new directory in the bucket, or using an existing one. For the purposes of this guide, the Minio bucket name is called
`MINIO_BUCKET`, and the directory in that bucket is called `MINIO_DIRECTORY`.

## Configuring Alluxio

You need to configure Alluxio to use Minio as its under storage system by modifying
`conf/alluxio-site.properties`. The first modification is to specify an **existing** Minio
bucket and directory as the under storage system.

All the fields to be modified in `conf/alluxio-site.properties` file are listed here:

{% include Configuring-Alluxio-with-Minio/minio.md %}

For these parameters, replace `<MINIO_ENDPOINT>`and `<MINIO_PORT>` with the URL and port of your Minio service.

Replace `<USE_HTTPS>` with `true` or `false`. If `true` (using HTTPS), also replace `<MINIO_HTTPS_PORT>`,
with the HTTPS port for the provider and remove the `alluxio.underfs.s3.endpoint.http.port`
parameter. If you replace `<USE_HTTPS>` with `false` (using HTTP) also replace `<MINIO_HTTP_PORT>` with
the HTTP port for the provider, and remove the `alluxio.underfs.s3.endpoint.https.port` parameter.
If the HTTP or HTTPS port values are left unset, `<HTTP_PORT>` defaults to port 80, and
`<HTTPS_PORT>` defaults to port 443.
