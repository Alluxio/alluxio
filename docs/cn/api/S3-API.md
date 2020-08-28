---
layout: global
title: S3 Client
nickname: S3 Client
group: Client APIs
priority: 1
---

* 内容列表
{:toc}

Alluxio支持RESTful API，兼容[Amazon S3 API](http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) 的基本操作。

[REST API 手册]({{ '/cn/api/Clients-Rest.html' | relativize_url }})会在Alluxio构建时生成并且可以通过`${ALLUXIO_HOME}/core/server/proxy/target/miredot/index.html`获得。

使用HTTP代理会带来一些性能的影响，尤其是在使用代理的时候会增加一个额外的跳计数。为了达到最优的性能，推荐代理服务和一个Alluxio worker运行在一个计算节点上。或者，推荐将所有的代理服务器放到load balancer之后。

# 特性支持
下表描述了对当前Amazon S3基础特性的支持情况：

<table class="table table-striped">
 <tr><th>S3 Feature</th><th>Status</th></tr>
 {% for item in site.data.table.s3-api-supported-operations %}
   <tr>
     <td>{{ item.S3Feature }}</td>
     <td>{{ item.Status }}</td>
   </tr>
 {% endfor %}
 </table>

# 语言支持
Alluxio S3 客户端支持各种编程语言，比如C++、Java、Python、Golang、Ruby等。在这个文档中，我们使用curl REST调用和python S3 client作为使用示例。

# 使用示例

## REST API
举个例子，你可以使用如下的RESTful API调用方式在本地运行一个Alluxio集群。Alluxio代理会默认在39999端口监听。

### 创建bucket

```console
$ curl -i -X PUT http://localhost:39999/api/v1/s3/testbucket
HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:34:41 GMT
Content-Length: 0
Server: Jetty(9.2.z-SNAPSHOT)
```

### 获取bucket(objects列表)

```console
$ curl -i -X GET http://localhost:39999/api/v1/s3/testbucket
HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:35:00 GMT
Content-Type: application/xml
Content-Length: 200
Server: Jetty(9.2.z-SNAPSHOT)

<ListBucketResult xmlns=""><Name>/testbucket</Name><Prefix/><ContinuationToken/><NextContinuationToken/><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>
```

### 加入object
假定本地现存一个文件`LICENSE`。

```console
$ curl -i -X PUT -T "LICENSE" http://localhost:39999/api/v1/s3/testbucket/testobject
HTTP/1.1 100 Continue

HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:36:03 GMT
ETag: "9347237b67b0be183499e5893128704e"
Content-Length: 0
Server: Jetty(9.2.z-SNAPSHOT)

```

### 获取object

```console
$ curl -i -X GET http://localhost:39999/api/v1/s3/testbucket/testobject
HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:37:34 GMT
Last-Modified: Tue, 29 Aug 2017 22:36:03 GMT
Content-Type: application/xml
Content-Length: 26847
Server: Jetty(9.2.z-SNAPSHOT)

.................. Content of the test file ...................
```

### 列出含有单个object的bucket

```console
$ curl -i -X GET http://localhost:39999/api/v1/s3/testbucket
HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:38:48 GMT
Content-Type: application/xml
Content-Length: 363
Server: Jetty(9.2.z-SNAPSHOT)

<ListBucketResult xmlns=""><Name>/testbucket</Name><Prefix/><ContinuationToken/><NextContinuationToken/><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>testobject</Key><LastModified>2017-08-29T15:36:03.613Z</LastModified><ETag></ETag><Size>26847</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>
```

### 列出含有多个objects的bucket
你可以上传更多的文件并且使用`max-keys`和`continuation-token`作为GET bucket request参数，比如：

```console
$ curl -i -X PUT -T "LICENSE" http://localhost:39999/api/v1/s3/testbucket/key1
$ curl -i -X PUT -T "LICENSE" http://localhost:39999/api/v1/s3/testbucket/key2
$ curl -i -X PUT -T "LICENSE" http://localhost:39999/api/v1/s3/testbucket/key3
$ curl -i -X GET http://localhost:39999/api/v1/s3/testbucket\?max-keys\=2
HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:40:45 GMT
Content-Type: application/xml
Content-Length: 537
Server: Jetty(9.2.z-SNAPSHOT)

<ListBucketResult xmlns=""><Name>/testbucket</Name><Prefix/><ContinuationToken/><NextContinuationToken>key3</NextContinuationToken><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>key1</Key><LastModified>2017-08-29T15:40:42.213Z</LastModified><ETag></ETag><Size>26847</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>key2</Key><LastModified>2017-08-29T15:40:43.269Z</LastModified><ETag></ETag><Size>26847</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>

# curl -i -X GET http://localhost:39999/api/v1/s3/testbucket\?max-keys\=2\&continuation-token\=key3
HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:41:18 GMT
Content-Type: application/xml
Content-Length: 540
Server: Jetty(9.2.z-SNAPSHOT)

<ListBucketResult xmlns=""><Name>/testbucket</Name><Prefix/><ContinuationToken>key3</ContinuationToken><NextContinuationToken/><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>key3</Key><LastModified>2017-08-29T15:40:44.002Z</LastModified><ETag></ETag><Size>26847</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>testobject</Key><LastModified>2017-08-29T15:36:03.613Z</LastModified><ETag></ETag><Size>26847</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>
```

你还可以验证这些对象是否为Alluxio文件，在`/testbucket`目录下。

```console
$ ./bin/alluxio fs ls -R /testbucket
```

### 删除objects

```console
$ curl -i -X DELETE http://localhost:39999/api/v1/s3/testbucket/key1
HTTP/1.1 204 No Content
Date: Tue, 29 Aug 2017 22:43:22 GMT
Server: Jetty(9.2.z-SNAPSHOT)
```

```console
$ curl -i -X DELETE http://localhost:39999/api/v1/s3/testbucket/key2
$ curl -i -X DELETE http://localhost:39999/api/v1/s3/testbucket/key3
$ curl -i -X DELETE http://localhost:39999/api/v1/s3/testbucket/testobject
```

### 初始化multipart upload

```console
$ curl -i -X POST http://localhost:39999/api/v1/s3/testbucket/testobject?uploads
HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:43:22 GMT
Content-Length: 197
Server: Jetty(9.2.z-SNAPSHOT)

<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="">
  <Bucket>testbucket</Bucket>
  <Key>testobject</Key>
  <UploadId>2</UploadId>
</InitiateMultipartUploadResult>
```

### 上传分块

```console
$ curl -i -X PUT http://localhost:39999/api/v1/s3/testbucket/testobject?partNumber=1&uploadId=2
HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:43:22 GMT
ETag: "b54357faf0632cce46e942fa68356b38"
Server: Jetty(9.2.z-SNAPSHOT)
```

### 罗列已上传的分块

```console
$ curl -i -X GET http://localhost:39999/api/v1/s3/testbucket/testobject?uploadId=2
HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:43:22 GMT
Content-Length: 985
Server: Jetty(9.2.z-SNAPSHOT)

<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="">
  <Bucket>testbucket</Bucket>
  <Key>testobject</Key>
  <UploadId>2</UploadId>
  <StorageClass>STANDARD</StorageClass>
  <IsTruncated>false</IsTruncated>
  <Part>
    <PartNumber>1</PartNumber>
    <LastModified>2017-08-29T20:48:34.000Z</LastModified>
    <ETag>"b54357faf0632cce46e942fa68356b38"</ETag>
    <Size>10485760</Size>
  </Part>
</ListPartsResult>
```

### 完成multipart upload

```console
$ curl -i -X POST http://localhost:39999/api/v1/s3/testbucket/testobject?uploadId=2 -d '
<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>"b54357faf0632cce46e942fa68356b38"</ETag>
  </Part>
</CompleteMultipartUpload>'

HTTP/1.1 200 OK
Date: Tue, 29 Aug 2017 22:43:22 GMT
Server: Jetty(9.2.z-SNAPSHOT)

<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="">
  <Location>/testbucket/testobjectLocation>
  <Bucket>testbucket</Bucket>
  <Key>testobject</Key>
  <ETag>"b54357faf0632cce46e942fa68356b38"</ETag>
</CompleteMultipartUploadResult>
```

### 中止multipart upload

```console
$ curl -i -X DELETE http://localhost:39999/api/v1/s3/testbucket/testobject?uploadId=2
HTTP/1.1 204 OK
Date: Tue, 29 Aug 2017 22:43:22 GMT
Content-Length: 0
Server: Jetty(9.2.z-SNAPSHOT)
```

### 删除空bucket

```console
$ curl -i -X DELETE http://localhost:39999/api/v1/s3/testbucket
HTTP/1.1 204 No Content
Date: Tue, 29 Aug 2017 22:45:19 GMT
```

## Python S3 Client

### 创建连接

```python
import boto
import boto.s3.connection

conn = boto.connect_s3(
    aws_access_key_id = '',
    aws_secret_access_key = '',
    host = 'localhost',
    port = 39999,
    path = '/api/v1/s3',
    is_secure=False,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
)
```

### 创建bucket

```python
bucketName = 'bucket-for-testing'
bucket = conn.create_bucket(bucketName)
```

### 加入small object

```python
smallObjectKey = 'small.txt'
smallObjectContent = 'Hello World!'

key = bucket.new_key(smallObjectKey)
key.set_contents_from_string(smallObjectContent)
```

### 获取small object

```python
assert smallObjectContent == key.get_contents_as_string()
```

### 上传large object
在本地文件系统创建一个8MB文件

```console
$ dd if=/dev/zero of=8mb.data bs=1048576 count=8
```

使用python S3 client把它作为object上传

```python
largeObjectKey = 'large.txt'
largeObjectFile = '8mb.data'

key = bucket.new_key(largeObjectKey)
with open(largeObjectFile, 'rb') as f:
    key.set_contents_from_file(f)
with open(largeObjectFile, 'rb') as f:
    largeObject = f.read()
```

### 获取large objecy

```python
assert largeObject == key.get_contents_as_string()
```

### 删除objects

```python
bucket.delete_key(smallObjectKey)
bucket.delete_key(largeObjectKey)
```

### 初始化multipart upload

```python
mp = bucket.initiate_multipart_upload(largeObjectFile)
```

### 上传分块

```python
import math, os

from filechunkio import FileChunkIO

# Use a chunk size of 1MB (feel free to change this)
sourceSize = os.stat(largeObjectFile).st_size
chunkSize = 1048576
chunkCount = int(math.ceil(sourceSize / float(chunkSize)))

for i in range(chunkCount):
    offset = chunkSize * i
    bytes = min(chunkSize, sourceSize - offset)
    with FileChunkIO(largeObjectFile, 'r', offset=offset, bytes=bytes) as fp:
        mp.upload_part_from_file(fp, part_num=i + 1)
```

### 完成multipart upload

```python
mp.complete_upload()
```

### 中止multipart upload

```python
mp.cancel_upload()
```

### 删除bucket

```python
conn.delete_bucket(bucketName)
```

