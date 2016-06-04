---
layout: global
title: 键值存储库（Key Value Store）客户端API（内测版）
nickname: 键值存储库（Key Value Store) API
group: Features
priority: 4
---

* Table of Contents
{:toc}

# 综述
Alluxio除了提供[Filesystem API](File-System-API.html) 让应用程序来读写和管理文件，还在文件系统
之上提供了键值（key-value）存储。
就像Alluxio文件系统中的文件一样，键值存储的语义是write-once。

* 用户可以创建一个键值存储库并且把键值对放入其中。键值对放入存储后是不可变的。
* 键值存储库完整保存后，用户可以打开并使用该键值存储库。

键值存储库可以用AlluxioURI来表示路径，比如`alluxio://path/my-kvstore`.
根据总容量和用户指定的数据块大小，单个键值存储库可能有一个以上的分区，分区是由Alluxio内部来管理，对用户透明。

# 键值存储库配置参数

Alluxio默认配置是禁用键值存储库的，可以通过配置`alluxio.keyvalue.enabled`为true来启用 (see
[configuration parameters](Configuration-Settings.html))

以下是键值存储库的配置参数：

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.key-value-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.key-value-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

# 快速测试

当启动Alluxio键值存储库后，可以运行`./bin/alluxio runKVTest`来快速测试键值存储库是否正常运行，如果运行
正常，应该在最后的输出结果中看到`Passed the test!`。

# 通过Java应用程序来访问键值存储库

### 获取一个键值存储库的客户端

要想用Java代码获取一个Alluxio键值存储系统客户端实例，可以使用:

{% include Key-Value-Store-API/get-key-value-system.md %}

## 创建一个新的键值存储库

可以通过调用`KeyValueSystem#createStore(AlluxioURI)`来创建一个新的键值存储库。将返回一个writer用于后续
加入键值对。可以参照下面的例子：

{% include Key-Value-Store-API/create-new-key-value.md %}

需要注意的是,

* 在writer关闭之前，该键值存储库是不完整并且不可用的;
* 在某些情况下，该键值存储库的空间会大于一个分区的最大容许容量。在这种情况下，writer会把键值对存于多个分区，
分区的切换对于用户是透明的。
* 写入的键应该是排了序的并且没有重复的键值。

## 从存储库中读取值

可以通过调用`KeyValueSystem#openStore(AlluxioURI)`来读取一个完整的键值存储库。将返回一个reader用于后续
基于键的值读取。可以参照下面的例子：

{% include Key-Value-Store-API/read-value.md %}

## 通过迭代器遍历存储中的键值对

可以参照下面的例子：

{% include Key-Value-Store-API/iterate-key-values.md %}

## 样例

从代码库中的[样例](https://github.com/Alluxio/alluxio/tree/master/examples/src/main/java/alluxio/examples/keyvalue)
 可以了解更多。

# 在Hadoop MapReduce内访问键值存储库

## MapReduce InputFormat

Alluxio提供了一种`InputFormat`的实现使得Hadoop MapReduce程序可以访问键值存储库。它使用一个key-value
URI作为参数，把键值对放入键值存储库内。

{% include Key-Value-Store-API/set-input-format.md %}


## MapReduce OutputFormat
Alluxio同时提供了一种`OutputFormat`的实现使得Hadoop MapReduce程序可以创建一个键值存储库。它使用一个
key-value URI作为参数把键值对放入键值存储库内。

{% include Key-Value-Store-API/set-output-format.md %}


## 样例

从代码库中的[样例](https://github.com/Alluxio/alluxio/blob/master/examples/src/main/java/alluxio/examples/keyvalue/hadoop/CloneStoreMapReduce.java)
 可以了解更多。

如果你已经[将HDFS配置为Alluxio的底层存储](Configuring-Alluxio-with-HDFS.md), 并且已经启用了键值存储，
那么可以按照如下方式运行上面的样例:

{% include Key-Value-Store-API/run-mapreduce-example.md %}
