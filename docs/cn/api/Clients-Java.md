---
layout: global
title: Alluxio Java客户端
nickname: Alluxio Java客户端
group: Client APIs
priority: 1
---

* 内容列表
{:toc}

Alluxio通过文件系统接口提供对数据的访问。Alluxio中的文件提供一次写入语义：它们在被完整写下之后不可变，在完成之前不可读。Alluxio提供了两种不同的文件系统API，Alluxio API和Hadoop兼容的API。AlluxioAPI提供了额外的功能，而Hadoop兼容的API为用户提供了无需修改现有代码使用Hadoop的API利用Alluxio的灵活性。

所有使用Alluxio Java API的资源都是通过`AlluxioURI`指定的路径实现。

### 获取文件系统客户端

要使用Java代码获取Alluxio文件系统客户端，请使用：

```java
FileSystem fs = FileSystem.Factory.get();
```

### 创建一个文件

所有的元数据操作，以及打开一个文件读取或创建一个文件写入都通过FileSystem对象执行。由于Alluxio文件一旦写入就不可改变，
创建文件的惯用方法是使用`FileSystem#createFile(AlluxioURI)`，它返回一个可用于写入文件的流对象。例如：

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Create a file and get its output stream
FileOutStream out = fs.createFile(path);
// Write data
out.write(...);
// Close and complete file
out.close();
```

### 指定操作选项

对于所有的文件系统操作，可以指定一个额外的`options`字段，它允许用户可以指定操作的非默认设置。例如：

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Generate options to set a custom blocksize of 128 MB
CreateFileOptions options = CreateFileOptions.defaults().setBlockSize(128 * Constants.MB);
FileOutStream out = fs.createFile(path, options);
```

### IO选项

Alluxio使用两种不同的存储类型：Alluxio管理存储和底层存储。 Alluxio管理存储是分配给Alluxio worker的内存，SSD和/或HDD。底层存储是由在最下层的存储系统（如S3，Swift或HDFS）管理的资源。用户可以指定通过`ReadType`和`WriteType`与Alluxio管理的存储交互。`ReadType`指定读取文件时的数据读取行为。`WriteType`指定数据编写新文件时写入行为，例如数据是否应该写入Alluxio Storage。

下面是`ReadType`的预期行为表。读取总是偏好Alluxio存储优先于底层存储。

<table class="table table-striped">
<tr><th>Read Type</th><th>Behavior</th>
</tr>
{% for readtype in site.data.table.ReadType %}
<tr>
  <td>{{readtype.readtype}}</td>
  <td>{{site.data.table.cn.ReadType[readtype.readtype]}}</td>
</tr>
{% endfor %}
</table>

下面是`WriteType`的预期行为表

<table class="table table-striped">
<tr><th>Write Type</th><th>Behavior</th>
</tr>
{% for writetype in site.data.table.WriteType %}
<tr>
  <td>{{writetype.writetype}}</td>
  <td>{{site.data.table.cn.WriteType[writetype.writetype]}}</td>
</tr>
{% endfor %}
</table>

### 位置策略

Alluxio提供了位置策略来选择要存储文件块到哪一个worker。

使用Alluxio的Java API，用户可以设置策略在`CreateFileOptions`中向Alluxio写入文件和在`OpenFileOptions`中读取文件。

用户可以轻松地覆盖默认的策略类[配置文件](Configuration-Settings.html)中的属性`alluxio.user.file.write.location.policy.class`。内置的策略包括：

* **LocalFirstPolicy (alluxio.client.file.policy.LocalFirstPolicy)**

    首先返回本地主机，如果本地worker没有足够的块容量，它从活动worker列表中随机选择一名worker。这是默认的策略。

* **MostAvailableFirstPolicy (alluxio.client.file.policy.MostAvailableFirstPolicy)**

    返回具有最多可用字节的worker。

* **RoundRobinPolicy (alluxio.client.file.policy.RoundRobinPolicy)**

    以循环方式选择下一个worker，跳过没有足够容量的worker。

* **SpecificHostPolicy (alluxio.client.file.policy.SpecificHostPolicy)**

    返回具有指定主机名的worker。此策略不能设置为默认策略。

Alluxio支持自定义策略，所以你也可以通过实现接口`alluxio.client.file.policyFileWriteLocationPolicy`制定适合自己的策略。注意
默认策略必须有一个空的构造函数。并使用ASYNC_THROUGH写入类型，所有块的文件必须写入同一个worker。

### 写入层

Alluxio允许客户在向本地worker写入数据块时选择一个层级偏好。目前
这种策略偏好只适用于本地worker而不是远程worker;远程worker会写到最高层。

默认情况下，数据被写入顶层。用户可以通过`alluxio.user.file.write.tier.default` [配置项](Configuration-Settings.html)修改默认设置，
或通过`FileSystem#createFile(AlluxioURI)`API调用覆盖它。

### 访问Alluxio中的现有文件

对现有文件或目录的所有操作都要求用户指定`AlluxioURI`。使用AlluxioURI，用户可以使用`FileSystem`中的任何方法来访问资源。

### 读取数据

`AlluxioURI`可用于执行Alluxio FileSystem操作，例如修改文件元数据，例如ttl或pin状态，或者获取输入流来读取文件。

例如，要读取一个文件：

```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI path = new AlluxioURI("/myFile");
// Open the file for reading
FileInStream in = fs.openFile(path);
// Read data
in.read(...);
// Close file relinquishing the lock
in.close();
```

### Javadoc

有关其他API信息，请参阅[Alluxio javadocs](http://www.alluxio.org/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/index.html).
