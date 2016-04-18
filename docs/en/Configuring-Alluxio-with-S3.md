---
layout: global
title: Configuring Alluxio with Amazon S3
nickname: Alluxio with S3
group: Under Store
priority: 0
---

This guide describes how to configure Alluxio with [Amazon S3](https://aws.amazon.com/s3/) as the
under storage system.

# Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

{% include Common-Commands/copy-alluxio-env.md %}

Also, in preparation for using S3 with Alluxio, create a bucket (or use an existing bucket). You
should also note the directory you want to use in that bucket, either by creating a new directory in
the bucket, or using an existing one. For the purposes of this guide, the S3 bucket name is called
`S3_BUCKET`, and the directory in that bucket is called `S3_DIRECTORY`.

# Configuring Alluxio

To configure Alluxio to use S3 as its under storage system, modifications to the
`conf/alluxio-env.sh` file must be made. The first modification is to specify an **existing** S3
bucket and directory as the under storage system. You specify it by modifying `conf/alluxio-env.sh`
to include:

{% include Configuring-Alluxio-with-S3/underfs-address.md %}

Next, you need to specify the AWS credentials for S3 access. In the `ALLUXIO_JAVA_OPTS` section of
the `conf/alluxio-env.sh` file, add:

{% include Configuring-Alluxio-with-S3/aws.md %}

Here, `<AWS_ACCESS_KEY_ID>` and `<AWS_SECRET_ACCESS_KEY>` should be replaced with your actual
[AWS keys](https://aws.amazon.com/developers/access-keys), or other environment variables that
contain your credentials.

The underlying S3 library JetS3t can incorporate bucket names that are DNS-compatible into the host
name of its requests. You can optionally configure this behavior in the `ALLUXIO_JAVA_OPTS` section
of the `conf/alluxio-env.sh` file by adding:

{% include Configuring-Alluxio-with-S3/jets3t.md %}

With `<DISABLE_DNS>` replaced with `false` (the default), a request directed at the bucket named "mybucket"
will be sent to the host name "mybucket.s3.amazonaws.com". With `<DISABLE_DNS>` replaced with `true`,
JetS3t will specify bucket names in the request path of the HTTP message rather than the Host header,
for example: "http://s3.amazonaws.com/mybucket". Without this parameter set, the system will default
to `false`. See http://www.jets3t.org/toolkit/configuration.html for further details.

After these changes, Alluxio should be configured to work with S3 as its under storage system, and
you can try [Running Alluxio Locally with S3](#running-alluxio-locally-with-s3).

## Accessing S3 through a proxy

To communicate with S3 through a proxy, modify the `ALLUXIO_JAVA_OPTS` section of
`conf/alluxio-env.sh` to include:

{% include Configuring-Alluxio-with-S3/proxy.md %}

Here, `<PROXY_HOST>` and `<PROXY_PORT>` should be replaced the host and port for your proxy, and
`<USE_HTTPS?>` should be set to either `true` or `false`, depending on whether https should be
used to communicate with the proxy.

These configuration parameters may also need to be set for the Alluxio client if it is running in
a separate JVM from the Alluxio Master and Workers. See
[Configuring Distributed Applications](#configuring-distributed-applications)

# Configuring Your Application

When building your application to use Alluxio, your application will have to include the
`alluxio-core-client` module. If you are using [maven](https://maven.apache.org/), you can add the
dependency to your application with:

{% include Configuring-Alluxio-with-S3/dependency.md %}

## Enabling the Hadoop S3 Client (instead of the native S3 client)

Alluxio provides a native client to communicate with S3. By default, the native S3 client is used
when Alluxio is configured to use S3 as its under storage system.

However, there is also an option to use a different implementation to communicate with S3; the S3
client provided by Hadoop. In order to disable the Alluxio S3 client (and enable the Hadoop S3
client), additional modifications to your application must be made. When including the
`alluxio-core-client` module in your application, the `alluxio-underfs-s3` should be excluded to disable
the native client, and to use the Hadoop S3 client:

{% include Configuring-Alluxio-with-S3/hadoop-s3-dependency.md %}

However, the Hadoop S3 client needs the `jets3t` package in order to use S3, but it is not included
as a dependency automatically. Therefore, you must also add the `jets3t` dependency manually. When
using maven, you can add the following to pull in the `jets3t` dependency:

{% include Configuring-Alluxio-with-S3/jets3t-dependency.md %}

The `jets3t` version `0.9.0` works for Hadoop version `2.3.0`. The `jets3t` version `0.7.1` should
work for older versions of Hadoop. To find the exact `jets3t` version for your Hadoop version,
please refer to [MvnRepository](http://mvnrepository.com/).

## Using a non-Amazon service provider

To use an S3 service provider other than "s3.amazonaws.com", modify the `ALLUXIO_JAVA_OPTS` section
of `conf/alluxio-env.sh` to include:

{% include Configuring-Alluxio-with-S3/non-amazon.md %}

For these parameters, replace `<S3_ENDPOINT>` with the host name of your S3 service. Only use this
parameter if you are using a provider other than `s3.amazonaws.com`.

Replace `<USE_HTTPS>` with `true` or `false`. If `true` (using HTTPS), also replace `<HTTPS_PORT>`, with
the HTTPS port for the provider and remove the `alluxio.underfs.s3.endpoint.http.port` parameter. If
you replace `<USE_HTTPS>` with `false` (using HTTP) also replace `<HTTP_PORT>` with the HTTP port for
the provider, and remove the `alluxio.underfs.s3.endpoint.https.port` parameter. If the HTTP or HTTPS
port values are left unset, `<HTTP_PORT>` defaults to port 80, and `<HTTPS_PORT>` defaults to port 443.

## Configuring Distributed Applications

If you are using an Alluxio client that is running separately from the Alluxio Master and Workers (in
a separate JVM), then you need to make sure that your AWS credentials are provided to the
application JVM processes as well. The easiest way to do this is to add them as command line options
when starting your client JVM process. For example:

{% include Configuring-Alluxio-with-S3/java-bash.md %}

# Running Alluxio Locally with S3

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit your S3 directory `S3_BUCKET/S3_DIRECTORY` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-S3/s3-file.md %}

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}
