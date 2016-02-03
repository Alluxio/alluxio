---
layout: global
title: Configuring Tachyon with Amazon S3
nickname: Tachyon with S3
group: Under Store
priority: 0
---

This guide describes how to configure Tachyon with [Amazon S3](https://aws.amazon.com/s3/) as the
under storage system.

# Initial Setup

First, the Tachyon binaries must be on your machine. You can either
[compile Tachyon](Building-Tachyon-Master-Branch.html), or
[download the binaries locally](Running-Tachyon-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

{% include Configuring-Tachyon-with-S3/copy-tachyon-env.md %}

Also, in preparation for using S3 with Tachyon, create a bucket (or use an existing bucket). You
should also note the directory you want to use in that bucket, either by creating a new directory in
the bucket, or using an existing one. For the purposes of this guide, the S3 bucket name is called
`S3_BUCKET`, and the directory in that bucket is called `S3_DIRECTORY`.

# Configuring Tachyon

To configure Tachyon to use S3 as its under storage system, modifications to the
`conf/tachyon-env.sh` file must be made. The first modification is to specify an **existing** S3
bucket and directory as the under storage system. You specify it by modifying `conf/tachyon-env.sh`
to include:

{% include Configuring-Tachyon-with-S3/underfs-address.md %}

Next, you need to specify the AWS credentials for S3 access. In the `TACHYON_JAVA_OPTS` section of
the `conf/tachyon-env.sh` file, add:

{% include Configuring-Tachyon-with-S3/aws.md %}

Here, `<AWS_ACCESS_KEY_ID>` and `<AWS_SECRET_ACCESS_KEY>` should be replaced with your actual
[AWS keys](https://aws.amazon.com/developers/access-keys), or other environment variables that
contain your credentials.

After these changes, Tachyon should be configured to work with S3 as its under storage system, and
you can try [Running Tachyon Locally with S3](#running-tachyon-locally-with-s3).

## Accessing S3 through a proxy

To communicate with S3 through a proxy, modify the `TACHYON_JAVA_OPTS` section of
`conf/tachyon-env.sh` to include:

{% include Configuring-Tachyon-with-S3/proxy.md %}

Here, `<PROXY_HOST>` and `<PROXY_PORT>` should be replaced the host and port for your proxy, and
`<USE_HTTPS?>` should be set to either `true` or `false`, depending on whether https should be
used to communicate with the proxy.

These configuration parameters may also need to be set for the Tachyon client if it is running in
a separate JVM from the Tachyon Master and Workers. See
[Configuring Distributed Applications](#configuring-distributed-applications)

# Configuring Your Application

When building your application to use Tachyon, your application will have to include the
`tachyon-client` module. If you are using [maven](https://maven.apache.org/), you can add the
dependency to your application with:

{% include Configuring-Tachyon-with-S3/dependency.md %}

## Enabling the Hadoop S3 Client (instead of the native S3 client)

Tachyon provides a native client to communicate with S3. By default, the native S3 client is used
when Tachyon is configured to use S3 as its under storage system.

However, there is also an option to use a different implementation to communicate with S3; the S3
client provided by Hadoop. In order to disable the Tachyon S3 client (and enable the Hadoop S3
client), additional modifications to your application must be made. When including the
`tachyon-client` module in your application, the `tachyon-underfs-s3` should be excluded to disable
the native client, and to use the Hadoop S3 client:

{% include Configuring-Tachyon-with-S3/hadoop-s3-dependency.md %}

However, the Hadoop S3 client needs the `jets3t` package in order to use S3, but it is not included
as a dependency automatically. Therefore, you must also add the `jets3t` dependency manually. When
using maven, you can add the following to pull in the `jets3t` dependency:

{% include Configuring-Tachyon-with-S3/jets3t-dependency.md %}

The `jets3t` version `0.9.0` works for Hadoop version `2.3.0`. The `jets3t` version `0.7.1` should
work for older versions of Hadoop. To find the exact `jets3t` version for your Hadoop version,
please refer to [MvnRepository](http://mvnrepository.com/).

## Configuring Distributed Applications

If you are using a Tachyon client that is running separately from the Tachyon Master and Workers (in
a separate JVM), then you need to make sure that your AWS credentials are provided to the
application JVM processes as well. The easiest way to do this is to add them as command line options
when starting your client JVM process. For example:

{% include Configuring-Tachyon-with-S3/java-bash.md %}

# Running Tachyon Locally with S3

After everything is configured, you can start up Tachyon locally to see that everything works.

{% include Configuring-Tachyon-with-S3/start-tachyon.md %}

This should start a Tachyon master and a Tachyon worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Configuring-Tachyon-with-S3/runTests.md %}

After this succeeds, you can visit your S3 directory `S3_BUCKET/S3_DIRECTORY` to verify the files
and directories created by Tachyon exist. For this test, you should see files named like:

{% include Configuring-Tachyon-with-S3/s3-file.md %}

To stop Tachyon, you can run:

{% include Configuring-Tachyon-with-S3/stop-tachyon.md %}
