---
layout: global
title: MinIO
nickname: MinIO
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [MinIO](https://min.io/) as the
under storage system.
Alluxio natively provides the `s3://` scheme (recommended for better performance).
You can use this scheme to connect Alluxio with a MinIO server.

## Prerequisites

The Alluxio binaries must be on your machine to proceed.
You can either
[compile Alluxio from source]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}),
or [download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

## Setup MinIO

MinIO is an object storage server built for cloud applications and DevOps. MinIO provides an open
source alternative to AWS S3.

Launch a MinIO server instance using the steps mentioned
[here](http://docs.min.io/docs/minio-quickstart-guide).
Then, either create a new bucket or use an existing one.
Once the MinIO server is launched, keep a note of the server endpoint, accessKey and secretKey.

You should also note the directory you want to use in that bucket, either by creating
a new directory in the bucket or using an existing one.
For the purposes of this guide, the MinIO bucket name is called `MINIO_BUCKET`, and the directory in
that bucket is called `MINIO_DIRECTORY`.

## Configuring Alluxio

You need to configure Alluxio to use MinIO as its under storage system by modifying
`conf/alluxio-site.properties`. The first modification is to specify an **existing** MinIO
bucket and directory as the under storage system.
Because Minio supports the `s3` protocol, it is possible to configure Alluxio as if it were
pointing to an AWS S3 endpoint.

All the fields to be modified in `conf/alluxio-site.properties` file are listed here:

```properties
alluxio.master.mount.table.root.ufs=s3://<MINIO_BUCKET>/<MINIO_DIRECTORY>
alluxio.underfs.s3.endpoint=http://<MINIO_ENDPOINT>/
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3.inherit.acl=false
aws.accessKeyId=<MINIO_ACCESS_KEY_ID>
aws.secretKey=<MINIO_SECRET_KEY_ID>
```

For these parameters, replace `<MINIO_ENDPOINT>` with the hostname and port of your MinIO service,
e.g., `http://localhost:9000/`.
If the port value is left unset, it defaults to port 80 for `http` and 443 for `https`.

## Test the MinIO Configuration

Format and start Alluxio with

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

Verify Alluxio is running by navigating to [http://localhost:19999](http://localhost:19999) or by
examining the logs to ensure the process is running.

Then, to run tests using some basic Alluxio operations execute the following command:

```console
$ ./bin/alluxio runTests
```

If there are no errors then MinIO is configured properly!

## Troubleshooting

There are a few variations of errors which might appear if Alluxio is incorrectly configured.
See below for a few common cases and their resolutions.

### The Specified Bucket Does Not Exist

If a message like this is returned, then you'll need to double check the name of the bucket in the
`alluxio-site.properties` file and make sure that it exists in MinIO.
The property for the bucket name is controlled by [`alluxio.master.mount.table.root.ufs`]({{ '/en/reference/Properties-List.html' | relativize_url}}#alluxio.master.mount.table.root.ufs)

```
Exception in thread "main" alluxio.exception.DirectoryNotEmptyException: Failed to delete /default_tests_files (com.amazonaws.services.s3.model.AmazonS3Exception: The specified bucket does not exist (Service: Amazon S3; Status Code: 404; Error Code: NoSuchBucke
t; Request ID: 158681CA87E59BA0; S3 Extended Request ID: 2d47b54e-7dd4-4e32-bc6e-48ffb8e2265c), S3 Extended Request ID: 2d47b54e-7dd4-4e32-bc6e-48ffb8e2265c) from the under file system
        at alluxio.client.file.BaseFileSystem.delete(BaseFileSystem.java:234)
        at alluxio.cli.TestRunner.runTests(TestRunner.java:118)
        at alluxio.cli.TestRunner.main(TestRunner.java:100)
```

### DNS Resolution - Unable to execute HTTP request

If an exception like this is encountered then it may be that the Alluxio property
[`alluxio.underfs.s3.disable.dns.buckets`]({{ '/en/reference/Properties-List.html' | relativize_url}}#alluxio.underfs.s3.disable.dns.buckets)
is set to `false`.
Setting this value to `true` for MinIO will allow Alluxio to resolve the proper bucket location.

```
Exception in thread "main" alluxio.exception.DirectoryNotEmptyException: Failed to delete /default_tests_files (com.amazonaws.SdkClientException: Unable to execute HTTP request: {{BUCKET_NAME}}) from the under file system
        at alluxio.client.file.BaseFileSystem.delete(BaseFileSystem.java:234)
        at alluxio.cli.TestRunner.runTests(TestRunner.java:118)
        at alluxio.cli.TestRunner.main(TestRunner.java:100)
```

### Connection Refused - Unable to execute HTTP request

If an exception occurs where the client returns an error with information about a refused connection
then Alluxio most likely cannot contact the MinIO server.
Make sure that the value of
[`alluxio.underfs.s3.endpoint`]({{ '/en/reference/Properties-List.html' | relativize_url}}#alluxio.underfs.s3.endpoint)
is correct and that the node running the Alluxio master can reach the MinIO endpoint over the
network.

```
Exception in thread "main" alluxio.exception.DirectoryNotEmptyException: Failed to delete /default_tests_files (com.amazonaws.SdkClientException: Unable to execute HTTP request: Connect to localhost:9001 [localhost/127.0.0.1] failed: Connection refused (Connect
ion refused)) from the under file system
        at alluxio.client.file.BaseFileSystem.delete(BaseFileSystem.java:234)
        at alluxio.cli.TestRunner.runTests(TestRunner.java:118)
        at alluxio.cli.TestRunner.main(TestRunner.java:100)
```

### Request Forbidden

If an exception including a message about forbidden access is encountered, it's possible that the
Alluxio master has been configured with incorrect credentials.
Check the [`aws.accessKeyId`]({{ '/en/reference/Properties-List.html' | relativize_url}}#aws.accessKeyId)
and [`aws.secretKey`]({{ '/en/reference/Properties-List.html' | relativize_url}}#aws.secretKey).
If this error is appearing, double check that both properties are set correctly.

```
ERROR CliUtils - Exception running test: alluxio.examples.BasicNonByteBufferOperations@388526fb
alluxio.exception.AlluxioException: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403; Error Code: 403 Forbidden; Request ID: 1586BF770688AB20; S3 Extended Request ID: null), S3 Extended Request ID: null
        at alluxio.exception.status.AlluxioStatusException.toAlluxioException(AlluxioStatusException.java:111)
        at alluxio.client.file.BaseFileSystem.createFile(BaseFileSystem.java:200)
        at alluxio.examples.BasicNonByteBufferOperations.createFile(BasicNonByteBufferOperations.java:102)
        at alluxio.examples.BasicNonByteBufferOperations.write(BasicNonByteBufferOperations.java:85)
        at alluxio.examples.BasicNonByteBufferOperations.call(BasicNonByteBufferOperations.java:80)
        at alluxio.examples.BasicNonByteBufferOperations.call(BasicNonByteBufferOperations.java:49)
        at alluxio.cli.CliUtils.runExample(CliUtils.java:51)
        at alluxio.cli.TestRunner.runTest(TestRunner.java:164)
        at alluxio.cli.TestRunner.runTests(TestRunner.java:134)
        at alluxio.cli.TestRunner.main(TestRunner.java:100)
Caused by: alluxio.exception.status.UnknownException: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403; Error Code: 403 Forbidden; Request ID: 1586BF770688AB20; S3 Extended Request ID: null), S3 Extended Request ID: null
```
