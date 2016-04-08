---
layout: global
title: Configuring Alluxio with Google Cloud Storage 
nickname: Alluxio with GCS 
group: Under Store
priority: 0
---

This guide describes how to configure Alluxio with [Google Cloud Storage](https://cloud.google.com/storage/) as the
under storage system.

# Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

{% include Common-Commands/copy-alluxio-env.md %}

Also, in preparation for using GCS with Alluxio, create a bucket (or use an existing bucket). You
should also note the directory you want to use in that bucket, either by creating a new directory in
the bucket, or using an existing one. For the purposes of this guide, the GCS bucket name is called
`GCS_BUCKET`, and the directory in that bucket is called `GCS_DIRECTORY`.

If you are new to Google Cloud Storage, please read the GCS [documentations](https://cloud.google.com/storage/docs/overview) first.

# Configuring Alluxio

To configure Alluxio to use GCS as its under storage system, modifications to the
`conf/alluxio-env.sh` file must be made. The first modification is to specify an **existing** GCS 
bucket and directory as the under storage system. You specify it by modifying `conf/alluxio-env.sh`
to include:

{% include Configuring-Alluxio-with-GCS/underfs-address.md %}

Next, you need to specify the Google credentials for GCS access. In the `ALLUXIO_JAVA_OPTS` section of
the `conf/alluxio-env.sh` file, add:

{% include Configuring-Alluxio-with-GCS/google.md %}

Here, `<GCS_ACCESS_KEY_ID>` and `<GCS_SECRET_ACCESS_KEY>` should be replaced with your actual
[GCS interoperable storage access keys](https://console.cloud.google.com/storage/settings),
or other environment variables that contain your credentials.

Note: GCS interoperability is disabled by default. Please click on the Interoperability tab
in [GCS setting](https://console.cloud.google.com/storage/settings) and enable this feature.
Then click on `Create a new key` to get the Access Key and Secret pair.

After these changes, Alluxio should be configured to work with GCS as its under storage system, and
you can try [Running Alluxio Locally with GCS](#running-alluxio-locally-with-gcs).

# Configuring Your Application

When building your application to use Alluxio, your application will have to include the
`alluxio-core-client` module. If you are using [maven](https://maven.apache.org/), you can add the
dependency to your application with:

{% include Configuring-Alluxio-with-GCS/dependency.md %}

## Configuring Distributed Applications

If you are using an Alluxio client that is running separately from the Alluxio Master and Workers (in
a separate JVM), then you need to make sure that your Google credentials are provided to the
application JVM processes as well. The easiest way to do this is to add them as command line options
when starting your client JVM process. For example:

{% include Configuring-Alluxio-with-GCS/java-bash.md %}

# Running Alluxio Locally with GCS 

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit your GCS directory `GCS_BUCKET/GCS_DIRECTORY` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-GCS/gcs-file.md %}

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}
