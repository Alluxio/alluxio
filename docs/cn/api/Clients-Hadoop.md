---
layout: global
title: 兼容Hadoop的Java客户端
nickname: 兼容Hadoop的Java
group: Client APIs
priority: 7
---

* 内容列表
{:toc}

Alluxio通过提供文件系统接口提供对文件的访问方式。Alluxio中的文件提供一次性写的语义：文件在全部写完之后不可修改，并且在全部
写完之前不可以读。Alluxio提供两种不同的文件系统API，Alluxio文件系统API以及兼容Hadoop的API。Alluxio API提供额外的功能性，而用户可以直接使用兼容Hadoop的API获取Hadoop原生API的功能，不用修改已有代码。

Alluxio有[原生文件系统Java客户端]({{ '/cn/api/FS-API.html' | relativize_url }})的包装类，可以提供兼容Hadoop的`FileSystem`接口。使用这个客户端时，
Hadoop文件操作会被转换为文件系统操作。最新的关于`文件系统`接口的文档可以在[这里](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html)找到。

兼容Hadoop的接口是为了方便而提供的，这样可以使用户重新利用之前为Hadoop写的代码。
