---
layout: global
title: 整合底层存储系统
group: Resources
---

Alluxio提供了[UnderFileSystem](https://github.com/alluxio/alluxio/blob/master/common/src/main/java/alluxio/underfs/UnderFileSystem.java)接口来将各种底层存储与Alluxio整合到一起。现在已经被整合的底层存储都作为`underfs`模块的子模块。要想创建新模块，只需实现以下几个组件：

* 实现UnderFileSystem接口，从而使Alluxio可以与该底层存储进行通信。

* 实现相应的[UnderFileSystemFactory](https://github.com/alluxio/alluxio/blob/master/common/src/main/java/alluxio/underfs/UnderFileSystemFactory.java)接口，其可以让Alluxio完成路径URI与实现的UnderFileSystem的匹配。

* 一个在META-INF/services目录下的文件，用于注册新整合的存储系统。

关于如何将一个存储系统作为Alluxio的底层存储，可以参考[HDFS子模块](https://github.com/alluxio/alluxio/tree/master/underfs/hdfs)和[S3子模块](https://github.com/alluxio/alluxio/tree/master/underfs/s3)这两个例子。
