---
layout: global
title: 在Alluxio-FUSE上运行Tensorflow
nickname: Tensorflow
group: Compute Integrations
priority: 6
---
本指南介绍如何在Alluxio POSIX API上运行[Tensorflow](https://www.tensorflow.org/)。

* 目录
{:toc}

## 概述

Tensorflow使开发人员可以快速轻松地开始使用深度学习。 
[深度学习]({{ '/en/compute/Deep-Learning.html' | relativize_url}})部分介绍了深度学习的数据挑战 
以及Alluxio如何帮助解决这些挑战。 
本教程针对在Alluxio POSIX API之上运行Tensorflow提供一些动手实例和技巧。

## 先决条件

* 安装或刷新Java为Java 8 Update 60或更高版本(8u60 +)，64位。
* Alluxio已设置并好并正常运行。
* 已安装[Tensorflow](https://www.tensorflow.org/install/pip)。本指南使用Tensorflow **v1.15**。

## 设置Alluxio POSIX API

在本节中，我们将按照
[POSIX API部分]({{ '/en/api/POSIX-API.html' | relativize_url}})中的介绍来设置Alluxio POSIX API
和允许Tensorflow应用程序通过Alluxio POSIX API来访问数据。

运行以下命令在Linux上安装FUSE

```console
$ yum install fuse fuse-devel
```

在MacOS上下载[osxfuse dmg文件](https://github.com/osxfuse/osxfuse/releases/download/ osxfuse-3.8.3 / osxfuse-3.8.3.dmg)，然后按照安装说明进行操作。

在Alluxio根目录下建立一个文件夹 

```console
$ ./bin/alluxio fs mkdir /training-data
```

创建一个文件夹`/mnt/fuse`，改变文件夹的所有者为当前用户(`$(WHOAMI )`)， 
并改变其权限允许读取和写入

```console
$ sudo mkdir -p /mnt/fuse
$ sudo chown $(whoami) /mnt/fuse
$ chmod 755 /mnt/fuse
```

运行Alluxio-FUSE shell来挂载Alluxio文件夹`training-data` 到刚刚创建本地空文件夹：

```console
$ ./integration/fuse/bin/alluxio-fuse mount /mnt/fuse /training-data
```

以上CLI命令产生了一个后台用户空间Java进程(`AlluxioFuse`)，该进程将在指定挂载点`/mnt/alluxio`把在`/training-data`中指定的Alluxio路径挂载到本地文件系统。 
请参阅[POSIX API文档]({{ '/en/api/POSIX-API.html' | relativize_url}})， 
以获取有关如何安装Alluxio-FUSE以及设置fuse相关选项的详细信息。 

检查FUSE进程的状态

```console
$ ./integration/fuse/bin/alluxio-fuse stat
```

挂载的`/mnt/fuse`文件夹已经准备好可以为深度学习框架所用，深度学习框架可以视Alluxio存储为本地文件夹。 
下一部分中Tensorflow将使用此文件夹进行训练。

## 示例:图像识别

### 准备训练数据

如果训练数据已经在远程数据存储中，则可以将其挂载为Alluxio`/training-data`目录下的一个文件夹。 这些数据对于在本地`/mnt/fuse/`上运行的应用程序是可见的。

假设ImageNet数据存储在S3存储桶`s3://alluxio-tensorflow-imagenet/`。
运行以下命令将该S3存储桶挂载到Alluxio路径`/training-data/imagenet`:

```console
$ ./bin/alluxio fs mount /training-data/imagenet/ s3://alluxio-tensorflow-imagenet/ \
  --option aws.accessKeyID=<ACCESS_KEY_ID> --option aws.secretKey=<SECRET_KEY>
```

注意，此命令可以接受选项来传递存储桶的S3凭证。 
一旦把这些凭证与挂载点关联起来，将来的访问将不再需要这些凭证。

如果数据不在远程数据存储中，则可以将其复制到Alluxio命名空间:

```console
$ wget http://download.tensorflow.org/models/image/imagenet/inception-2015-12-05.tgz
$ ./bin/alluxio fs mkdir /training-data/imagenet 
$ ./bin/alluxio fs copyFromLocal inception-2015-12-05.tgz /training-data/imagenet 
```

假设ImageNet数据存储在一个S3桶`s3://alluxio-tensorflow-imagenet/`， 
以下三个命令将在两个挂载进程运行之后显示完全相同的数据:

```
aws s3 ls s3://alluxio-tensorflow-imagenet/
# 2019-02-07 03:51:15          0 
# 2019-02-07 03:56:09   88931400 inception-2015-12-05.tgz
bin/alluxio fs ls /training-data/imagenet/
# -rwx---rwx ec2-user       ec2-user              88931400       PERSISTED 02-07-2019 03:56:09:000   0% /training-data/imagenet/inception-2015-12-05.tgz
ls -l /mnt/fuse/imagenet/
# total 0
# -rwx---rwx 0 ec2-user ec2-user 88931400 Feb  7 03:56 inception-2015-12-05.tgz
```

### 运行图像识别测试

下载[图像识别脚本](HTTPS://生.githubus ercontent.com/tensorflow/models/v1.11/tutorials/image/imagenet/classify_image.py)
，然后使用包含训练数据的本地文件夹运行脚本。

```console
$ curl -o classify_image.py -L https://raw.githubusercontent.com/tensorflow/models/v1.11/tutorials/image/imagenet/classify_image.py
$ python classify_image.py --model_dir /mnt/fuse/imagenet/
```

这将使用`/mnt/fuse/imagenet/inception-2015-12-05.tgz`中的输入数据来识别图像，并将一些中间数据写入`/mnt/fuse/imagenet` 
如果都成功运行，你将在命令提示符下看到:

```
giant panda, panda, panda bear, coon bear, Ailuropoda melanoleuca (score = 0.89107)
indri, indris, Indri indri, Indri brevicaudatus (score = 0.00779)
lesser panda, red panda, panda, bear cat, cat bear, Ailurus fulgens (score = 0.00296)
custard apple (score = 0.00147)
earthstar (score = 0.00117)
```

## 示例:Tensorflow基准

假设数据位于S3路径`s3://alluxio-tensorflow-imagenet/`，将S3存储桶中的ImageNet数据挂载到路径`/training-data/imagenet`。

```console
$ ./bin/alluxio fs mount /training-data/imagenet/ \
  s3://alluxio-tensorflow-imagenet/ \
  --option aws.accessKeyID=<ACCESS_KEY_ID> \
  --option aws.secretKey=<SECRET_KEY>
```

要使用Alluxio POSIX API通过Alluxio访问S3中的训练数据，
可以将路径`/mnt/fuse/imagenet`传递给基准脚本的参数`data_dir`
[tf_cnn_benchmarsk.py](https://github.com/tensorflow/benchmarks/blob/master/scripts/tf_cnn_benchmarks/tf_cnn_benchmarks.py)。

## 提示

### 使用数据位置参数编写Tensorflow应用程序

在HDFS，S3和其他底层存储之上运行Tensorflow可能需要不同的配置，进而
使管理和集成Tensorflow应用和不同底层存储很难。 
通过Alluxio POSIX API，用户只需要将底层存储挂载到Aluxio一次和将包含培训数据底层存储父文件夹挂载到本地文件系统。
初始挂载后，就可通过Alluxio fuse挂载点立即访问数据，并可 
在Tensorflow应用程序中透明地访问该数据。
如果Tensorflow应用程序已设置了数据位置参数，则只需要将fuse挂载点内的数据位置传递 
点给Tensorflow应用程序，而无需对其进行修改。
这大大简化了应用程序开发，避免了使用不同的集成设置和 
为每个底层存储做凭证配置。

### 共置Tensorflow和Alluxio worker

通过共置Tensorflow应用程序和Alluxio worker，Alluxio在本地缓存远程数据以供将来访问， 
从而提供很好的数据局部性。 
如果没有Alluxio，缓慢的远程存储可能会导致I/O瓶颈，并导致GPU资源利用不足。 
在同时写入或读取大文件时，在Alluxio worker节点上运行，Alluxio POSIX API可以提供明显更好的性能。 
通过Alluxio POSIX API并设置具有内存空间的worker节点来承载所有培训数据可以使 
性能提高近2倍。

### 配置Alluxio写入类型和读取类型

许多Tensorflow应用程序在其工作流程期间会生成许多小的临时中间文件。 
这些临时中间文件仅在短时间内有用，不需要持久存储在底层存储中。 
如果我们直接将Tensorflow与远程存储链接，则所有文件(无论类型-数据文件，临时中间文件， 
结果等)都将写入并持久保存到远程存储中。 
借助Alluxio-Tensorflow应用程序和远程存储之间的缓存层，用户可以减少不必要的远程 
持久性存储操作并缩短写入/读取时间。

将`alluxio.user.file.writetype.default`设置为`MUST_CACHE`后，可以写入Alluxio worker存储顶层(通常是内存层) 。 
将`alluxio.user.file.readtype.default`设置为`CACHE_PROMOTE`后，可以将读取的数据缓存在Alluxio中以供将来访问。 
通过从内存写入和读取将加速我们的Tensorflow工作流程。 
如果远程存储是像S3这样的云存储，则优势将更加明显。
