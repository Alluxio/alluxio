---
layout: global
title: Key Value Store Client API
nickname: Key Value Store API
group: Features
priority: 4
---

* Table of Contents
{:toc}

# 综述
Tachyon除了提供[Filesystem API](File-System-API.html) 让应用程序来读，写和管理文件，还在文件系统
之上提供了键值（key-value）存储。
就像Tachyon文件系统中的文件一样，键值存储的语义是write-once。

* 用户可以创建一个键值存储并且把键值对放入其中。键值对放入存储后是不可变的。 
* 键值对放入键值存储后，用户可以打开并使用该键值存储。？

单个键值存储可以用TachyonURI来表示路径，比如`tachyon://path/my-kvstore`.
取决于用户指定的总容量和数据块大小，单个键值存储可能有一个以上的分区，分区是Tachyon内部来管理，对用户透明。

# 通过Java应用程序来访问键值存储

### 获取一个键值存储的客户端

要想用Java代码获取一个Tachyon键值存储客户端实例，可以使用:

```java
KeyValueSystem kvs = KeyValueSystem.Factory().get();
```

## 创建一个新的键值存储

可以通过调用`KeyValueSystem#createStore(TachyonURI)`来创建一个新的键值存储。将返回一个writer用于后续
加入键值对。可以参照下面的例子：

```java
KeyValueStoreWriter writer = kvs.createStore(new TachyonURI("tachyon://path/my-kvstore"));
// Insert key-value pair ("100", "foo")
writer.put("100", "foo");
// Insert key-value pair ("200", "bar")
writer.put("200", "bar");
// Close and complete the store
writer.close();
```
需要注意的是, 

* 在writer关闭之前，该键值存储是不完整的并且不可用;
* 在某些情况下，该键值存储的空间会大于一个分区的最大容许容量。在这种情况下，writer会把键值对卸乳多个分区，
分区的切换对于用户是透明的。
* 写入的键应该是排了序的并且没有重复的键。

## 从存储中读取值

可以通过调用`KeyValueSystem#openStore(TachyonURI)`来读取一个完整的键值存储。将返回一个reader用于后续
基于键的值读取。可以参照下面的例子：

```java
KeyValueStoreReader reader = kvs.openStore(new TachyonURI("tachyon://path/kvstore/"));
// Return "foo"
reader.get("100"); 
// Return null as no value associated with "300"
reader.get("300");
// Close the reader on the store
reader.close();
```
## 通过迭代器遍历存储中的键值对

可以参照下面的例子：

```java
KeyValueStoreReader reader = kvs.openStore(new TachyonURI("tachyon://path/kvstore/"));
KeyValueIterator iterator = reader.iterator();
while (iterator.hasNext()) {
  KeyValuePair pair = iterator.next();
  ByteBuffer key = pair.getkKey();
  ByteBuffer value = pair.getValue();
}
// Close the reader on the store
reader.close()
```

# 在Hadoop MapReduce内访问键值存储
 
## MapReduce InputFormat

Tachyon提供了一种`InputFormat`的实现使得Hadoop MapReduce程序可以访问键值存储。它使用一个key-value 
URI作为参数，把键值对放入键值存储内。
 
```java
conf.setInputFormat(KeyValueInputFormat.class);
FileInputFormat.setInputPaths(conf, new Path("tachyon://input-store"));
```


## MapReduce OutputFormat
Tachyon同时提供了一种`OutputFormat`的实现使得Hadoop MapReduce程序可以创建一个键值存储。它使用一个
key-value URI作为参数把键值对放入键值存储内。
 
```java
conf.setOutputKeyClass(BytesWritable.class);
conf.setOutputValueClass(BytesWritable.class);
conf.setOutputFormat(KeyValueOutputFormat.class);
conf.setOutputCommitter(KeyValueOutputCommitter.class);
FileOutputFormat.setOutputPath(conf, new Path("tachyon://output-store"));
```