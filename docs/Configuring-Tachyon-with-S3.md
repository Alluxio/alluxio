---
layout: global
title: Configuring Tachyon with S3
nickname: Tachyon with S3
group: Under Stores
priority: 1
---

This guide describes how to configure Tachyon with [Amazon S3](https://aws.amazon.com/s3/) as the under storage system.

# Getting Started

First, the Tachyon binaries must be on your machine. You can either [compile Tachyon](Building-Tachyon-Master-Branch.html), or [download the binaries locally](Running-Tachyon-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

    $ cp conf/tachyon-env.sh.template conf/tachyon-env.sh

Also, in preparation for using S3 with Tachyon, create a bucket (or use an existing bucket). You should also note the directory you want to use in that bucket, either by creating a new directory in the bucket, or using an existing one. For the purposes of this guide, the S3 bucket name is called `S3_BUCKET`, and the directory in that bucket is called `S3_DIRECTORY`. 

# Configuration

To configure Tachyon to use S3 as its under storage system, modifications to the `conf/tachyon-env.sh` file must be made. The first modification is to specify the *existing* S3 bucket and directory as the under storage system. You specify it by modifying `conf/tachyon-env.sh` to include:

    export TACHYON_UNDERFS_ADDRESS=s3n://S3_BUCKET/S3_DIRECTORY

Next, you need to specify the AWS credentials for S3 access. In the `TACHYON_JAVA_OPTS` section of the `conf/tachyon-env.sh` file, add:

    -Dfs.s3n.awsAccessKeyId=<AWS_ACCESS_KEY_ID>
    -Dfs.s3n.awsSecretAccessKey=<AWS_SECRET_ACCESS_KEY>

Here, `<AWS_ACCESS_KEY_ID>` and `<AWS_SECRET_ACCESS_KEY>` should be replaced with your actual [AWS keys](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html), or other environment variables that represent your credentials.

