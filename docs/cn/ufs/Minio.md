---
layout: global
title: Alluxio集成Minio作为底层存储
nickname: Alluxio集成Minio作为底层存储
group: Storage Integrations
priority: 10
---

* 目录
{:toc}

本指南介绍了如何配置Alluxio用[MinIO](https://min.io/)
做为底层存储系统。
Alluxio提供了原生s3://支持方案(建议使用以获得更好的性能)。
可以使用此方案把Alluxio与一个MinIO服务器链接起来。

## 先决条件

必须在机器上安装了Alluxio二进制文件才能继续。
可以
[从源代码编译Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url}})，
或[在本地下载二进制文件]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url}})。

## 设置MinIO

MinIO是为云应用和DevOps构建的对象存储。 MinIO提供了一个开源的AWS S3对象存储替代方案。
使用[此处](http://docs.min.io/docs/minio-quickstart-guide)步骤启动MinIO服务器实例。
然后，创建一个新的存储桶或使用现有的存储桶。
一旦启动MinIO服务器后，请记下服务器endpoint，accessKey和secretKey。

你还应该记录要在该存储桶中使用的目录，
可以在存储桶中新建一个目录或使用一个现有目录。
在本指南中，MinIO存储桶名称为`MINIO_BUCKET`，存储桶中的目录名称为`MINIO_DIRECTORY`。

## 配置Alluxio

你需要通过修改`conf/alluxio-site.properties`来配置Alluxio用MinIO做为底层存储系统。第一个修改是指定一个**现存**MinIO存储桶和目录作为底层存储系统。
由于Minio支持`s3`协议，因此可以将Alluxio配置为
仿佛指向一个AWS S3 endpoint。

此处列出了`conf/alluxio-site.properties`文件中所有要修改的字段:

```properties
alluxio.master.mount.table.root.ufs=s3://<MINIO_BUCKET>/<MINIO_DIRECTORY>
alluxio.underfs.s3.endpoint=http://<MINIO_ENDPOINT>/
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3.inherit.acl=false
aws.accessKeyId=<MINIO_ACCESS_KEY_ID>
aws.secretKey=<MINIO_SECRET_KEY_ID>
```

对于这些参数，将`<MINIO_ENDPOINT>`替换为你的MinIO服务的主机名和端口，
例如`http://localhost:9000/`。
如果未设置端口值，则默认为`http`端口为80，`https`端口为443。

## 测试MinIO配置

通过如下命令格式化并启动Alluxio

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

要确认Alluxio在运行，可以通过访问[http://localhost:19999](http://localhost:19999)网址或检查日志以确保进程正在运行。

之后，要使用一些基本的Alluxio操作运行测试，执行以下命令:

```console
$ ./bin/alluxio runTests
```
如果没有错误，则说明MinIO配置正确！

## 故障排除

如果Alluxio配置不正确，可能会出现几类不同错误。
有关一些常见错误案例及其解决方案，请参见下文。

### 指定的存储桶不存在

如果返回了类似这样的消息，则需要再次检查`alluxio-site.properties`文件中存储桶的名称，并确保该存储桶存在于MinIO中。
存储桶名称的属性由[`alluxio.master.mount.table.root.ufs`]({{ '/en/reference/Properties-List.html' | relativize_url}}#alluxio.master.mount.table.root.ufs)控制。 

```
Exception in thread "main" alluxio.exception.DirectoryNotEmptyException: Failed to delete /default_tests_files (com.amazonaws.services.s3.model.AmazonS3Exception: The specified bucket does not exist (Service: Amazon S3; Status Code: 404; Error Code: NoSuchBucke
t; Request ID: 158681CA87E59BA0; S3 Extended Request ID: 2d47b54e-7dd4-4e32-bc6e-48ffb8e2265c), S3 Extended Request ID: 2d47b54e-7dd4-4e32-bc6e-48ffb8e2265c) from the under file system
        at alluxio.client.file.BaseFileSystem.delete(BaseFileSystem.java:234)
        at alluxio.cli.TestRunner.runTests(TestRunner.java:118)
        at alluxio.cli.TestRunner.main(TestRunner.java:100)
```

### DNS解析-无法执行HTTP请求

如果遇到这样的异常，则可能是Alluxio属性
[`alluxio.underfs.s3.disable.dns.buckets`]({{ '/en/reference/Properties-List.html' | relativize_url}}#alluxio.underfs.s3.disable.dns.buckets)
设置为`false`。
为MinIO将此值设置为`true`将允许Alluxio解析正确的存储桶位置。

```
Exception in thread "main" alluxio.exception.DirectoryNotEmptyException: Failed to delete /default_tests_files (com.amazonaws.SdkClientException: Unable to execute HTTP request: {{BUCKET_NAME}}) from the under file system
        at alluxio.client.file.BaseFileSystem.delete(BaseFileSystem.java:234)
        at alluxio.cli.TestRunner.runTests(TestRunner.java:118)
        at alluxio.cli.TestRunner.main(TestRunner.java:100)
```
### 连接被拒绝-无法执行HTTP请求

如果发生客户端返回连接被拒绝错误的异常，
那么很可能是Alluxio无法连接MinIO服务器。
确保
[`alluxio.underfs.s3.endpoint`]({{ '/en/reference/Properties-List.html' | relativize_url}}#alluxio.underfs.s3.endpoint)
设值是正确的，并且运行Alluxio的master节点可以通过网络连接到MinIO endpoint。

```
Exception in thread "main" alluxio.exception.DirectoryNotEmptyException: Failed to delete /default_tests_files (com.amazonaws.SdkClientException: Unable to execute HTTP request: Connect to localhost:9001 [localhost/127.0.0.1] failed: Connection refused (Connect
ion refused)) from the under file system
        at alluxio.client.file.BaseFileSystem.delete(BaseFileSystem.java:234)
        at alluxio.cli.TestRunner.runTests(TestRunner.java:118)
        at alluxio.cli.TestRunner.main(TestRunner.java:100)
```

### 请求禁止

如果遇到包含有关禁止访问的消息的异常，则可能
Alluxio master凭证配置不正确。
检查[`aws.accessKeyId`]({{ '/en/reference/Properties-List.html' | relativize_url}}#aws.accessKeyId)
和[`aws.secretKey`]({{ '/en/reference/Properties-List.html' | relativize_url}}#aws.secretKey)。
如果出现此错误，请仔细检查这两个属性的设置是否正确。

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
