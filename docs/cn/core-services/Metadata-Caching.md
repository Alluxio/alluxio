---
layout: global
title: Metadata Cache and Invalidation
---

在Dora中，Alluxio缓存了worker节点的元数据和数据。 本文档介绍了如何缓存元数据以及元数据如何失效和刷新。 随着元数据失效，与元数据关联的数据也失效。

## 两种不同级别的元数据缓存

在 Dora 中，元数据被缓存在worker节点上。 元数据缓存有两级：内存级别的缓存和RocksDB缓存。
RocksDB 中的元数据可以被视为堆外 (off-heap)持久缓存。 它被存储在持久化存储中，例如本地文件系统。如果worker节点重启，缓存在内存中的元数据将会丢失。 相反，RocksDB中缓存的元数据是持久的，并且可以在worker节点重启时加载回来。Alluxio使用配置键来控制缓存的容量和缓存的寿命。这些配置值则应根据可用内存资源来调整。

### 内存缓存的属性键
以下的key用于控制 getStatus() 返回结果中内存元数据缓存的大小，以及此类元数据的生命周期（生存时间，TTL）。
- `DORA_UFS_FILE_STATUS_CACHE_SIZE (alluxio.dora.ufs.file.status.cache.size)`：内存中缓存的元数据项的最大数量。 元数据项的平均大小约为 2KB。 所以如果这个属性值被设置为1M（1024 * 1024），那么缓存需要的总内存大约是2GB。
- `DORA_UFS_FILE_STATUS_CACHE_TTL (alluxio.dora.ufs.file.status.cache.ttl)` ：缓存元数据的最长有效时间。 当元数据加载到内存缓存中时，其 TTL 开始倒计时。 TTL 达到零后，元数据将失效且不可访问。 后续访问必须再次加载元数据。

### RocksDB 缓存的属性键
- `DORA_WORKER_METASTORE_ROCKSDB_TTL (alluxio.dora.worker.metastore.rocksdb.ttl)`：RocksDB 中缓存的元数据的最长有效时间。 当元数据加载到 RocksDB 缓存中时，其 TTL 开始倒计时。 TTL 达到零后，元数据将失效且不可访问。 缓存的元数据将从RocksDB中删除，后续访问必须重新加载元数据。

### 内存中 ListStatus 缓存的属性键
Alluxio worker节点还将ListStatus()请求的结果缓存在内存中, 从而加速此操作。
- `DORA_UFS_LIST_STATUS_CACHE_TTL (alluxio.dora.ufs.list.status.cache.ttl)`：ListStatus()结果被缓存的最长时间。
- `DORA_UFS_LIST_STATUS_CACHE_NR_DIRS (alluxio.dora.ufs.list.status.cache.nr.dirs)`：内存中缓存的 ListStatus() 结果的最大数量。

### 客户端元数据失效
在一些面向元数据的操作中，Alluxio客户端还可以将以下配置传递给worker节点来控制元数据失效。
- `USER_FILE_METADATA_SYNC_INTERVAL(alluxio.user.file.metadata.sync.interval)` 是在对路径/文件执行调用操作之前同步 UFS 元数据的时间间隔。
“同步UFS元数据”是指如果需要的话使缓存的元数据失效，然后从UFS重新加载元数据。 该键可以在`conf/alluxio-site.properties`配置文件中配置，或者在命令行上通过`-Dalluxio.user.file.metadata.sync.interval=<integer_value>`定义为JVM的系统属性。
```shell
$ bin/alluxio fs -Dalluxio.user.file.metadata.sync.interval=0 ls "some/path/and/file"
```
此配置将对所有操作和所有路径全局生效。

使用 Alluxio 客户端 jar 的应用程序也可以在 java 程序中执行该操作，如下所示：
```java
AlluxioURI path = new AlluxioURI("/some_file");
URIStatus st;
int syncIntervalMs = 2000; // two seconds. Set to 0 to always sync metadata
GetStatusPOptions option = GetStatusPOptions.newBuilder()
    .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                    .setSyncIntervalMs(syncIntervalMs)
                    .build())
    .build();
st = fs.getStatus(path, option);
```

## PageStore中缓存数据失效
当文件/对象元数据失效时，PageStore 中与同一文件/对象关联的数据也将失效。
将来，我们需要通过更细粒度的方法来使文件/对象的缓存数据失效，例如基于文件/对象的校验和。
但截至目前，PageStore 尚未将此类信息与其页面一起存储。 后续我们需要对此进行一些优化。
