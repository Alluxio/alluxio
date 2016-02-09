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
就像Tachyon文件系统中的文件一样，键值存储的语义是只写一次。

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