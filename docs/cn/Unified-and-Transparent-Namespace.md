---
layout: global
title: 统一透明命名空间
nickname: 统一命名空间
group: Features
priority: 5
---

* 内容列表
{:toc}

通过使用其透明命名机制以及挂载API，Alluxio支持在不同存储系统之间对数据进行高效的管理。

## 透明命名机制

透明命名机制保证了Alluxio和底层存储系统的命名空间是一致的。

![transparent]({{site.data.img.screenshot_transparent}})

当在Alluxio文件系统中创建对象时，可以选择这些对象是否要在底层存储系统中进行持久化。对于需要持久化的对象，
Alluxio会保存底层文件系统存储这些对象的文件夹的路径。例如，一个用户在根目录下创建了一个`Users`目录，其中
包含`Alice`和`Bob`两个子目录，底层文件系统（如HDFS或S3）也会保存相同的目录结构和命名。类似地，当用户在
Alluxio文件系统中对一个持久化的对象进行重命名或者删除操作时，底层文件系统中对应的对象也会被执行相同的操作。

另外，Alluxio能够搜索到底层文件系统中并非通过Alluxio创建的对象。例如，底层文件系统中包含一个`Data`文件夹，
其中包含`Reports`和`Sales`两个文件，它们都不是通过Alluxio创建的，当它们第一次被访问时（例如用户请求打开文
件），Alluxio会自动加载这些对象的元数据。然而在该过程中Alluxio不会加载具体文件数据，若要将其加载到Alluxio，
可以在第一次读取数据时将`AlluxioStorageType`设置成`STORE`，或者通过Alluxio Shell中的`load`命令进行加载。

## 统一命名空间

Alluxio提供了一个挂载API，通过该API能够在Alluxio中访问多个数据源中的数据。

![unified]({{site.data.img.screenshot_unified}})

默认情况下，Alluxio文件系统挂载到Alluxio配置中`alluxio.underfs.address`指定的目录，该目录代表Alluxio
的"primary storage"。另外，用户可以通过挂载API添加和删除数据源。

{% include Unified-and-Transparent-Namespace/mounting-API.md %}

例如，主存储（"primary storage"）可以是HDFS，其中可以包含用户的文件夹；`Data`文件夹可能存储在S3文件系统
下，这可以通过`mount(alluxio://host:port/Data, s3://bucket/directory)`命令实现。

## 示例

在该示例中，我们将演示以上提到的特性。该示例假定Alluxio的源代码在`${ALLUXIO_HOME}`目录中，并且Alluxio正在
本地运行。

先在本地文件系统中创建一个临时目录：

{% include Unified-and-Transparent-Namespace/mkdir.md %}

将该目录挂载到Alluxio中，并确认挂载后的目录在Alluxio中存在：

{% include Unified-and-Transparent-Namespace/mount-demo.md %}

验证对于不是通过Alluxio创建的对象，当第一次访问它们时，其元数据被加载进入了Alluxio中：

{% include Unified-and-Transparent-Namespace/ls-demo-hello.md %}

在挂载目录下创建一个文件，并确认该文件也被创建在底层文件系统中：

{% include Unified-and-Transparent-Namespace/create-file.md %}

在Alluxio中重命名一个文件，并验证在底层文件系统中该文件也被重命名了：

{% include Unified-and-Transparent-Namespace/rename.md %}

在Alluxio中将该文件删除，然后检查底层文件系统中该文件是否也被删除：

{% include Unified-and-Transparent-Namespace/delete.md %}

最后卸载该挂载目录，并确认该目录已经在Alluxio文件系统中被移除，但原先的数据依然保存在底层文件系统中。

{% include Unified-and-Transparent-Namespace/unmount.md %}
