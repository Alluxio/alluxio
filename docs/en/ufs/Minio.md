---
layout: global
title: MinIO
---


This guide describes how to configure Alluxio with [MinIO](https://min.io/){:target="_blank"} as the
under storage system.

MinIO is a high-performance, S3 compatible object store. It is built for large scale AI/ML, data lake and database workloads. It runs on-prem and on any cloud (public or private) and from the data center to the edge. MinIO provides an open source alternative to AWS S3.

Alluxio natively provides the `s3://` scheme (recommended for better performance). You can use this scheme to connect Alluxio with a MinIO server.

For more information about MinIO, please read its [documentation](https://min.io/docs/minio/linux/index.html){:target="_blank"}

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

Launch a MinIO server instance using the steps mentioned
[here](https://min.io/docs/minio/linux/index.html){:target="_blank"}. Once the MinIO server is launched, keep a note of the server endpoint, accessKey and secretKey.

In preparation for using MinIO with Alluxio:
<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<MINIO_BUCKET>`</td>
        <td markdown="span">[Create a new bucket](https://min.io/docs/minio/linux/reference/minio-mc/mc-mb.html#examples){:target="_blank"} or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<MINIO_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<MINIO_ENDPOINT>`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<S3_ACCESS_KEY_ID>`</td>
        <td markdown="span">Used to sign programmatic requests made to AWS. See [How to Obtain Access Key ID and Secret Access Key](https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<S3_SECRET_KEY>`</td>
        <td markdown="span">Used to sign programmatic requests made to AWS. See [How to Obtain Access Key ID and Secret Access Key](https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html){:target="_blank"}</td>
    </tr>
</table>

## Basic Setup

To use MinIO as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify an **existing** MinIO bucket and directory as the underfs address. Because Minio supports the `s3` protocol, it is possible to configure Alluxio as if it were pointing to an AWS S3 endpoint.

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=s3://<MINIO_BUCKET>/<MINIO_DIRECTORY>
alluxio.underfs.s3.endpoint=http://<MINIO_ENDPOINT>/
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3.inherit.acl=false
s3a.accessKeyId=<S3_ACCESS_KEY_ID>
s3a.secretKey=<S3_SECRET_KEY>
```

For these parameters, replace `<MINIO_ENDPOINT>` with the hostname and port of your MinIO service,
e.g., `http://localhost:9000/`.
If the port value is left unset, it defaults to port 80 for `http` and 443 for `https`.

## Running Alluxio Locally with MinIO

Once you have configured Alluxio to MinIO, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

## Troubleshooting

There are a few variations of errors which might appear if Alluxio is incorrectly configured.
See below for a few common cases and their resolutions.

### The Specified Bucket Does Not Exist

If a message like this is returned, then you'll need to double check the name of the bucket in the
`alluxio-site.properties` file and make sure that it exists in MinIO.
The property for the bucket name is controlled by [`alluxio.dora.client.ufs.root`]({{ '/en/reference/Properties-List.html' | relativize_url}}#alluxio.dora.client.ufs.root)

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
Check the [`s3a.accessKeyId`]({{ '/en/reference/Properties-List.html' | relativize_url}}#s3a.accessKeyId)
and [`s3a.secretKey`]({{ '/en/reference/Properties-List.html' | relativize_url}}#s3a.secretKey).
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
