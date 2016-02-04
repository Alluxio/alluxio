---
layout: global
title: Configuring Alluxio with OSS
nickname: Alluxio with OSS
group: Under Store
priority: 4
---

This guide describes how to configure Alluxio with [Aliyun OSS](http://www.aliyun.com/product/oss/?lang=en) as the under storage system. Object Storage Service (OSS) is a massive, secure and highly reliable cloud storage service provided by Aliyun.

## Initial Setup

To run a Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these machines.You can either [compile the binaries from Alluxio source code](http://tachyon-project.org/documentation/master/Building-Alluxio-Master-Branch.html), or [download the precompiled binaries directly](http://tachyon-project.org/documentation/master/Running-Alluxio-Locally.html).

Then if you haven't already done so, create your configuration file from the template:

{% include Configuring-Alluxio-with-OSS/copy-tachyon-env.md %}

Also, in preparation for using OSS with tachyon, create a bucket or use an existing bucket. You should also note that the directory you want to use in that bucket, either by creating a new directory in the bucket, or using an existing one. For the purposes of this guide, the OSS bucket name is called OSS_BUCKET, and the directory in that bucket is called OSS_DIRECTORY. Also, for using the OSS Service, you should provide an oss endpoint to specify which range your bucket is on. The endpoint here is called OSS_ENDPOINT, and to learn more about the endpoints for special range you can see [here](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region). For more information about OSS Bucket, Please see [here](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/function&bucket)

## Confugurating Alluxio

To configure Alluxio to use OSS as its under storage system, modifications to the `conf/tachyon-env.sh` file must be made. The first modification is to specify an existing OSS bucket and directory as the under storage system. You can specify it by modifying `conf/tachyon-env.sh` to include:

{% include Configuring-Alluxio-with-OSS/underfs-address.md %}
    
Next you need to specify the Aliyun credentials for OSS access. In the TACHYON_JAVA_OPTS section of the `conf/tachyon-env.sh` file, add:

{% include Configuring-Alluxio-with-OSS/oss-access.md %}
    
Here, `<OSS_ACCESS_KEY_ID>` ,`<OSS_SECRET_ACCESS_KEY>` should be replaced with your actual [Aliyun keys](https://ak-console.aliyun.com/#/accesskey), or other environment variables that contain your credentials. The `<OSS_ENDPOINT>` for your OSS range you can get [here](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region). 

If you feel not sure about how to change the `conf/tachyon-env.sh`, there is another way to provide these configurations. You can provide a properties config file named : `tachyon-site.properties` in the `conf/` directory, and edit it as below:

{% include Configuring-Alluxio-with-OSS/properties.md %}

After these changes, Alluxio should be configured to work with OSS as its under storage system, and you can try to run tachyon locally with OSS.

## Configuring Distributed Applications

If you are using a Alluxio client that is running separately from the Alluxio Master and Workers (in a separate JVM), then you need to make sure that your Aliyun credentials are provided to the application JVM processes as well. The easiest way to do this is to add them as command line options when starting your client JVM process. For example:

{% include Configuring-Alluxio-with-OSS/java-bash.md %}

## Running Alluxio Locally with OSS

After everythin is configured, you can start up Alluxio locally to see that everything works.

{% include Configuring-Alluxio-with-OSS/start-tachyon.md %}
    
This should start a Alluxio master and a Alluxio worker. You can see the master UI at http://localhost:19999.

Next, you can run a simple example program:

{% include Configuring-Alluxio-with-OSS/runTests.md %}
    
After this succeeds, you can visit your OSS directory OSS_BUCKET/OSS_DIRECTORY to verify the files and directories created by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-OSS/oss-file.md %}

To stop Alluxio, you can run:

{% include Configuring-Alluxio-with-OSS/stop-tachyon.md %}
