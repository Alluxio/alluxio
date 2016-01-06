---
layout: global
title: Configuring Tachyon with OSS
nickname: Tachyon with OSS
group: Under Stores
priority: 4
---

This guide describes how to configure Tachyon with [Aliyun OSS](http://www.aliyun.com/product/oss/?lang=en) as the under storage system. Object Storage Service (OSS) is a massive, secure and highly reliable cloud storage service provided by Aliyun.

## Initial Setup

To run a Tachyon cluster on a set of machines, you must deploy Tachyon binaries to each of these machines.You can either [compile the binaries from Tachyon source code](http://tachyon-project.org/documentation/master/Building-Tachyon-Master-Branch.html), or [download the precompiled binaries directly](http://tachyon-project.org/documentation/master/Running-Tachyon-Locally.html).

Then if you haven't already done so, create your configuration file from the template:

    cp conf/tachyon-env.sh.template conf/tachyon-env.sh

Also, in preparation for using OSS with tachyon, create a bucket or use an existing bucket. You should also note that the directory you want to use in that bucket, either by creating a new directory in the bucket, or using an existing one. For the purposes of this guide, the OSS bucket name is called OSS_BUCKET, and the directory in that bucket is called OSS_DIRECTORY. Also, for using the OSS Service, you should provide an oss endpoint to specify which range your bucket is on. The endpoint here is called OSS_ENDPOINT, and to learn more about the endpoints for special range you can see [here](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region). For more information about OSS Bucket, Please see [here](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/function&bucket)

## Confugurating Tachyon

To configure Tachyon to use OSS as its under storage system, modifications to the `conf/tachyon-env.sh` file must be made. The first modification is to specify an existing OSS bucket and directory as the under storage system. You can specify it by modifying `conf/tachyon-env.sh` to include:

    export TACHYON_UNDERFS_ADDRESS=oss://OSS_BUCKET/OSS_DIRECTORY/
    
Next you need to specify the Aliyun credentials for OSS access. In the TACHYON_JAVA_OPTS section of the `conf/tachyon-env.sh` file, add:

    -Dfs.oss.accessKeyId=<OSS_ACCESS_KEY_ID>
    -Dfs.oss.accessKeySecret=<OSS_SECRET_ACCESS_KEY>
    -Dfs.oss.endpoint=<OSS_ENDPOINT>
    
Here, `<OSS_ACCESS_KEY_ID>` ,`<OSS_SECRET_ACCESS_KEY>` should be replaced with your actual [Aliyun keys](https://ak-console.aliyun.com/#/accesskey), or other environment variables that contain your credentials. The `<OSS_ENDPOINT>` for your OSS range you can get [here](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region). 

If you feel not sure about how to change the `conf/tachyon-env.sh`, there is another way to provide these configurations. You can provide a properties config file named : `tachyon-site.properties` in the `conf/` directory, and edit it as below:

    # Config the OSS credentials
    fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID>
    fs.oss.accessKeySecret=<OSS_SECRET_ACCESS_KEY>
    fs.oss.endpoint=<OSS_ENDPOINT>

After these changes, Tachyon should be configured to work with OSS as its under storage system, and you can try to run tachyon locally with OSS.

## Configuring Distributed Applications

If you are using a Tachyon client that is running separately from the Tachyon Master and Workers (in a separate JVM), then you need to make sure that your Aliyun credentials are provided to the application JVM processes as well. The easiest way to do this is to add them as command line options when starting your client JVM process. For example:

    $ java -Xmx3g -Dfs.oss.accessKeyId=<OSS_ACCESS_KEY_ID> -Dfs.oss.accessKeySecret=<OSS_SECRET_ACCESS_KEY> -Dfs.oss.endpoint=<OSS_ENDPOINT> -cp my_application.jar com.MyApplicationClass myArgs

## Running Tachyon Locally with OSS

After everythin is configured, you can start up Tachyon locally to see that everything works.

    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh local
    
This should start a Tachyon master and a Tachyon worker. You can see the master UI at http://localhost:19999.

Next, you can run a simple example program:

    $ ./bin/tachyon runTests
    
After this succeeds, you can visit your OSS directory OSS_BUCKET/OSS_DIRECTORY to verify the files and directories created by Tachyon exist. For this test, you should see files named like:

    OSS_BUCKET/OSS_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE

To stop Tachyon, you can run:

    $ ./bin/tachyon-stop.sh
