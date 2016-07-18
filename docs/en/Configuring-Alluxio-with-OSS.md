---
layout: global
title: Configuring Alluxio with OSS
nickname: Alluxio with OSS
group: Under Store
priority: 4
---

This guide describes how to configure Alluxio with [Aliyun OSS](http://www.aliyun.com/product/oss/?lang=en) as the under storage system. Object Storage Service (OSS) is a massive, secure and highly reliable cloud storage service provided by Aliyun.

## Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these machines.You can either [compile the binaries from Alluxio source code](http://alluxio.org/documentation/master/Building-Alluxio-Master-Branch.html), or [download the precompiled binaries directly](http://alluxio.org/documentation/master/Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file with `bootstrapConf` command.
For example, if you are running Alluxio on your local machine, `ALLUXIO_MASTER_HOSTNAME` should be set to `localhost`

{% include Configuring-Alluxio-with-OSS/bootstrapConf.md %}

Alternatively, you can also create the configuration file from the template and set the contents manually. 

{% include Common-Commands/copy-alluxio-env.md %}

Also, in preparation for using OSS with alluxio, create a bucket or use an existing bucket. You should also note that the directory you want to use in that bucket, either by creating a new directory in the bucket, or using an existing one. For the purposes of this guide, the OSS bucket name is called OSS_BUCKET, and the directory in that bucket is called OSS_DIRECTORY. Also, for using the OSS Service, you should provide an oss endpoint to specify which range your bucket is on. The endpoint here is called OSS_ENDPOINT, and to learn more about the endpoints for special range you can see [here](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region). For more information about OSS Bucket, Please see [here](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/function&bucket)

## Confugurating Alluxio

You need to configure Alluxio to use OSS as its under storage system. The first modification is to specify an existing OSS bucket and directory as the under storage system by modifying `conf/alluxio-site.properties` to include:

{% include Configuring-Alluxio-with-OSS/underfs-address.md %}
    
Next you need to specify the Aliyun credentials for OSS access. In `conf/alluxio-site.properties`, add:

{% include Configuring-Alluxio-with-OSS/oss-access.md %}
    
Alternatively, these configuration settings can be set in the `conf/alluxio-env.sh` file. More
details about setting configuration parameters can be found in
[Configuration Settings](Configuration-Settings.html#environment-variables).

After these changes, Alluxio should be configured to work with OSS as its under storage system, and you can try to run alluxio locally with OSS.

## Configuring Distributed Applications

If you are using an Alluxio client that is running separately from the Alluxio Master and Workers (in a separate JVM), then you need to make sure that your Aliyun credentials are provided to the application JVM processes as well. The easiest way to do this is to add them as command line options when starting your client JVM process. For example:

{% include Configuring-Alluxio-with-OSS/java-bash.md %}

Alternatively, you may copy `conf/alluxio-site.properties` (having the properties setting credentials) to the classpath
of your application runtime (e.g., `$SPARK_CLASSPATH` for Spark), or append the path to this site properties file to
the classpath.

## Running Alluxio Locally with OSS

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}
    
This should start an Alluxio master and an Alluxio worker. You can see the master UI at http://localhost:19999.

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}
    
After this succeeds, you can visit your OSS directory OSS_BUCKET/OSS_DIRECTORY to verify the files and directories created by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-OSS/oss-file.md %}

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}
