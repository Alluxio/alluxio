---
layout: global
title: 如何开发底层存储扩展
group: Resources
priority: 3
---

* 内容列表
{:toc}

该页是面向底层存储扩展的开发者的。请浏览[managing extensions](UFSExtensions.html)来获取使用已有扩展的指导。

底层存储扩展被构建为jar，并包含在一个特定的扩展位置，由Alluxio core提取。这一页描述了在Alluxio中的扩展如何工作的机制，并提供了开发底层存储扩展的详细说明。扩展提供了一个框架，使更多的存储库能够使用Alluxio，并使开发Alluxio不支持的模块变得方便。

## 工作原理

### 发现服务

扩展jar包在Alluxio server运行时动态加载，使得Alluxio能与新的底层存储扩展交互而不用重启。
Alluxio server使用Java [ServiceLoader](https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html)来发现底层存储API。
提供者要把`alluxio.underfs.UnderFileSystemFactory`的接口的实现包含在内。
实现的宣传方式是在`META_INF/services`中包含一个文本文件，其中只有一行指向实现该接口的类。

### 依赖管理

实现者需要将传递的依赖包含在扩展jar包内。Alluxio为每个扩展jar包实行独立的类加载来避免Alluxio server和扩展之间的冲突。

## 实现一个底层存储扩展

建立一个新的底层存储连接涉及:

-实现所需的存储接口

-声明服务实现

-将实现和传递依赖打包到一个uber JAR中

参考的实现可以在[alluxio-extensions](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/tutorial)找到。
本章剩余部分将介绍写一个新底层存储扩展的步骤。名为`DummyUnderFileSystem`的样例，使用maven作为build和依赖管理工具，并将所有操作转发到本地文件系统。

### 实现底层存储接口

[HDFS Submodule](https://github.com/alluxio/alluxio/tree/master/underfs/hdfs)和[S3A Submodule](https://github.com/alluxio/alluxio/tree/master/underfs/s3a)是两个将存储系统对接Alluxio好的样例。

步骤1：实现接口`UnderFileSystem`

`UnderFileSystem`接口定义在模块`org.alluxio:alluxio-core-common`中。选择扩展`BaseUnderFileSystem`或`ObjectUnderFileSystem`来完成`UnderFileSystem`接口。
`ObjectUnderFileSystem`适合连接到对象存储，并将文件系统操作抽象为对象存储。

```java
public class DummyUnderFileSystem extends BaseUnderFileSystem {
	// Implement filesystem operations
	...
}
```

或是，

```java
public class DummyUnderFileSystem extends ObjectUnderFileSystem {
	// Implement object store operations
	...
}
```

步骤2：完成接口`UnderFileSystemFactory`

底层存储factory定义了`UnderFileSystem`实现支持的路径，以及如何创建`UnderFileSystem`实现。

```java
public class DummyUnderFileSystemFactory implements UnderFileSystemFactory {
	...

        @Override
        public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
                // Create the under storage instance
        }

        @Override
	public boolean supportsPath(String path) {
		// Choose which schemes to support, e.g., dummy://
	}
}
```

### 声明该服务

在`src/main/resources/META_INF/services/alluxio.underfs.UnderFileSystemFactory`创建文件来告知ServiceLoader完成的`UnderFileSystemFactory`。

```
alluxio.underfs.dummy.DummyUnderFileSystemFactory
```

### 编译

使用`maven-shade-plugin`或`maven-assembly`将扩展工程的所有传递依赖包含在built jar中。
此外，为了避免冲突，指定依赖`alluxio-core-common`的范围为`provided`。maven定义像下面这样：

```xml
<dependencies>
    <!-- Core Alluxio dependencies -->
    <dependency>
      <groupId>org.alluxio</groupId>
      <artifactId>alluxio-core-common</artifactId>
      <scope>provided</scope>
    </dependency>
    ...
</dependencies>
```

### 测试

扩展`AbstractUnderFileSystemContractTest`来测试定义的`UnderFileSystem`符合Alluxio与底层存储模块的约定。
看下面的参考实现来将测试参数包含。

```java
public final class DummyUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
    ...
}
```

恭喜！你已经开发了一个Alluxio的新底层存储扩展。
向Alluxio [repository](https://github.com/Alluxio/alluxio/tree/master/docs/en/UFSExtensions.md)提交一个pull request来让社区知道它，在[documentation page](UFSExtensions.html)章节编辑扩展列表。
