---
layout: global
title: 命名空间管理
nickname: 命名空间管理
group: Advanced
priority: 0
---

* 内容列表
{:toc}

通过使用其透明命名机制以及挂载API，Alluxio支持在不同存储系统之间对数据进行高效的管理。

## 透明命名机制

透明命名机制保证了Alluxio和底层存储系统的命名空间是一致的。

![transparent]({{ site.baseurl }}/img/screenshot_transparent.png)

当在Alluxio文件系统中创建对象时，可以选择这些对象是否要在底层存储系统中进行持久化。对于需要持久化的对象，
Alluxio会保存底层文件系统存储这些对象的文件夹的路径。例如，一个用户在根目录下创建了一个`Users`目录，其中
包含`Alice`和`Bob`两个子目录，底层文件系统（如HDFS或S3）也会保存相同的目录结构和命名。类似地，当用户在
Alluxio文件系统中对一个持久化的对象进行重命名或者删除操作时，底层文件系统中对应的对象也会被执行相同的操作。

另外，Alluxio能够搜索到底层文件系统中并非通过Alluxio创建的对象。例如，底层文件系统中包含一个`Data`文件夹，
其中包含`Reports`和`Sales`两个文件，它们都不是通过Alluxio创建的，当它们第一次被访问时（例如用户请求打开文
件），Alluxio会自动加载这些对象的元数据。然而在该过程中Alluxio不会加载具体文件数据，若要将其加载到Alluxio，
可以用`FileInStream`读数据，或者通过Alluxio Shell中的`load`命令进行加载。

## 统一命名空间

Alluxio提供了一个挂载API，通过该API能够在Alluxio中访问多个数据源中的数据。

![unified]({{ site.baseurl }}/img/screenshot_unified.png)

默认情况下，Alluxio文件系统挂载到Alluxio配置中`alluxio.master.mount.table.root.ufs`指定的目录，该目录代表Alluxio
的"primary storage"。另外，用户可以通过挂载API添加和删除数据源。

```java
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath);
void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options);
void unmount(AlluxioURI path);
void unmount(AlluxioURI path, UnmountOptions options);
```

例如，主存储（"primary storage"）可以是HDFS，其中可以包含用户的文件夹；`Data`文件夹可能存储在S3文件系统
下，这可以通过以下命令实现。
```java
mount(new AlluxioURI("alluxio://host:port/Data"), new AlluxioURI("s3a://bucket/directory"));
```

## 示例

以下示例假设Alluxio源代码在 `${ALLUXIO_HOME}` 文件夹下，并且有一个本地运行的Alluxio进程。

### 透明命名

在这个例子中，我们将展示Alluxio提供Aluxio空间和底层存储系统的透明命名机制。

先在本地文件系统中创建一个临时目录：

```bash
cd /tmp
mkdir alluxio-demo
touch alluxio-demo/hello
```

将该目录挂载到Alluxio中，并确认挂载后的目录在Alluxio中存在：

```bash
cd ${ALLUXIO_HOME}
./bin/alluxio fs mount /demo file:///tmp/alluxio-demo
Mounted file:///tmp/alluxio-demo at /demo
./bin/alluxio fs ls -R /
... # should contain /demo but not /demo/hello
```

验证对于不是通过Alluxio创建的对象，当第一次访问它们时，其元数据被加载进入了Alluxio中：

```bash
./bin/alluxio fs ls /demo/hello
... # should contain /demo/hello
```

在挂载目录下创建一个文件，并确认该文件也被创建在底层文件系统中：

```bash
./bin/alluxio fs touch /demo/hello2
/demo/hello2 has been created
./bin/alluxio fs persist /demo/hello2
persisted file /demo/hello2 with size 0
ls /tmp/alluxio-demo
hello hello2
```

在Alluxio中重命名一个文件，并验证在底层文件系统中该文件也被重命名了：

```bash
./bin/alluxio fs mv /demo/hello2 /demo/world
Renamed /demo/hello2 to /demo/world
ls /tmp/alluxio-demo
hello world
```

在Alluxio中将该文件删除，然后检查底层文件系统中该文件是否也被删除：

```bash
./bin/alluxio fs rm /demo/world
/demo/world has been removed
ls /tmp/alluxio-demo
hello
```

最后卸载该挂载目录，并确认该目录已经在Alluxio文件系统中被移除，但原先的数据依然保存在底层文件系统中。

```bash
./bin/alluxio fs unmount /demo
Unmounted /demo
./bin/alluxio fs ls -R /
... # should not contain /demo
ls /tmp/alluxio-demo
hello
```

### 统一命名空间

在这个例子中，我们将展示如何在不同类型的存储下提供一个统一的文件系统名称空间抽象。
特别的，我们有一个HDFS服务和来自不同AWS帐户的两个S3存储桶。

首先，使用凭据`<accessKeyId1>`和`<secretKey1>`将第一个S3桶挂装到Alluxio中:

```java
./bin/alluxio fs mkdir /mnt
./bin/alluxio fs mount --option aws.accessKeyId=<accessKeyId1> --option aws.secretKey=<secretKey1>  /mnt/s3bucket1 s3a://data-bucket1/
```

接下来，使用可能不同的凭据`<accessKeyId2>`和`<secretKey2>`将第二个S3 bucket挂载到Alluxio中:

```java
./bin/alluxio fs mount --option aws.accessKeyId=<accessKeyId2> --option aws.secretKey=<secretKey2>  /mnt/s3bucket2 s3a://data-bucket2/
```

最后，将HDFS存储也挂载到Alluxio中:

```java
./bin/alluxio fs mount /mnt/hdfs hdfs://<NAMENODE>:<PORT>/
```

现在这些不同的目录都包含在一个Alluxio的空间中:

```bash
./bin/alluxio fs ls -R /
... # should contain /mnt/s3bucket1, /mnt/s3bucket2, /mnt/hdfs
```

## 资源

[统一命名空间博客](http://www.alluxio.com/2016/04/unified-namespace-allowing-applications-to-access-data-anywhere/)
