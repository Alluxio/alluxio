---
layout: global
title: 文件系统客户端API
nickname: 文件系统API
group: Features
priority: 1
---

* Table of Contents
{:toc}

Tachyon提供了访问数据的文件系统接口。Tachyon上的文件提供了一次写入的语义：文件全部被写入之后就不会改变，而且文件在写操作完成之前不能进行读操作。Tachyon提供了两种不同的文件系统API，本地API和兼容Hadoop的API。本地API具有更好的性能，而兼容Hadoop的API使用户可以灵活利用Tachyon，但没必要修改用Hadoop API写的代码。

# 本地API

Tachyon提供了Java版的API来访问和修改Tachyon文件系统命名空间内的文件。所有资源都可以通过代表资源路径的`TachyonURI`来访问。

### 获取文件系统客户端

要想用Java代码获取一个Tachyon文件系统实例，可使用：

{% include File-System-API/get-fileSystem.md %}

### 创建文件

所有的元数据操作，以及用于读文件的打开文件操作或用于写文件的创建文件操作，都会通过FileSystem对象来执行。因Tachyon文件一旦写入就不会改变，惯用的创建文件的方式是使用`FileSystem#createFile(TachyonURI)`，这条语句会返回一个用于写文件的流对象，例如：

{% include File-System-API/write-file.md %}

### 指定操作选项

对于所有FileSystem的操作，可能要指定额外的`options`域，该域允许用户指定该操作的非默认设置。例如：

{% include File-System-API/specify-options.md %}

### IO选项

Tachyon有两种存储类型：Tachyon管理的存储和底层存储。Tachyon管理的存储是分配给Worker的内存、SSD、和（或）HDD。底层存储是由底层存储系统管理的存储资源，如S3，Swift或HDFS。用户可以通过指定`ReadType`和`WriteType`来与Tachyon本地存储或底层存储进行交互。在读文件的时候，`ReadType`给该数据指定了读行为，例如：该数据是否应该被保留在Tachyon存储内。在写新文件的时候，`WriteType`给该数据指定了写行为，例如：该数据是否应该被写到Tachyon存储内。

下表描述了`ReadType`不同类型对应的不同行为。相较于底层存储系统，优先从Tachyon存储上进行读取。

<table class="table table-striped">
<tr><th>读类型</th><th>行为</th>
</tr>
{% for readtype in site.data.table.ReadType %}
<tr>
  <td>{{readtype.readtype}}</td>
  <td>{{site.data.table.cn.ReadType.[readtype.readtype]}}</td>
</tr>
{% endfor %}
</table>

下表描述了`WriteType`不同类型对应的不同行为。

<table class="table table-striped">
<tr><th>写类型</th><th>行为</th>
</tr>
{% for writetype in site.data.table.WriteType %}
<tr>
  <td>{{writetype.writetype}}</td>
  <td>{{site.data.table.cn.WriteType.[writetype.writetype]}}</td>
</tr>
{% endfor %}
</table>

### 定位策略

Tachyon提供定位策略，用于确定应该选择哪个Worker来存储文件数据块。用户可以在`CreateFileOptions`中设置该策略以用于写文件，也可在`OpenFileOptions`中设置该策略用于向Tachyon中读文件。Tachyon支持自定义定位策略，内置策略包括：

* **LocalFirstPolicy**

    首先返回本地主机，如果本地Worker没有足够的容量容纳一个数据块，那么就会从有效的Worker列表中随机选择一个Worker。这是默认策略。

* **MostAvailableFirstPolicy**

    返回拥有最多可用容量的Worker。

* **RoundRobinPolicy**

    以循环的方式选取存储下一个数据块的Worker，如果该Worker没有足够的容量，就将其跳过。

* **SpecificHostPolicy**

    返回指定主机名的Worker。该策略不能被设置为默认策略。

Tachyon支持自定义策略，所以你可以开发自己的策略来迎合应用需求。注意默认策略必须要有一个空构造函数。要想使用ASYNC_THROUGH写类型，所有的文件数据块必须被写到相同的Worker上。

### 访问Tachyon上一个存在的文件

对存在的文件和目录进行的所有操作都需要用户来指定`TachyonURI`。利用`TachyonURI`，用户可以使用`FileSystem`的方法来访问资源。

### 读数据

`TachyonURI`可被用于执行Tachyon FileSystem的操作，例如：修改文件元数据，如ttl或pin状态，或者通过获取一个输入流来读取文件。

例如，读文件：

{% include File-System-API/read-file.md %}

# Hadoop API

Tachyon有一个本地客户端的包装，其提供了兼容Hadoop的`FileSystem`接口。利用该客户端实例，Hadoop的文件操作将被转换为FileSystem操作。最新的`FileSystem`接口的文档可以在[这里](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html)找到。

兼容Hadoop的接口被作为一个通用类来使用，其允许用户保留之前所写的用于Hadoop的代码。
