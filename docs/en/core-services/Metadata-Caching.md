---
layout: global
title: Metadata Cache and Invalidation
---

In Dora, Alluxio caches metadata and data on workers. This document explains how the metadata is cached and how the metadata is invalidated and refreshed. Along with the metadata invalidation, the data that is associated with the metadata is also invalidated.

## Two Different Levels of Metadata Cache

In Dora, metadata is cached on workers. There are two levels of metadata cache: in-memory cache and in-RocksDB cache.
Metadata in RocksDB can be considered as off-heap and persistent cache. It is stored in persistent storage, e.g. local file system.
The metadata cached in memory will be lost if worker restarts. On the contrary, the metadata cached in RocksDB is persistent, and can be loaded
back when worker restarts. Alluxio uses config keys to control the capacity of the cache and the life of the cache. These config values should
be tuned according to available memory resources.

### Property Keys for In-memory Cache

The following keys are used to control the size of in-memory metadata cache from getStatus() result, and the life (Time To Live, TTL) of such metadata.
- `DORA_UFS_FILE_STATUS_CACHE_SIZE (alluxio.dora.ufs.file.status.cache.size)`: the maximum number of metadata items cached in memory. The average size of a metadata item is about 2K bytes. So if this property key is set to 1M (1024 * 1024), the total memoey required by cache is about 2G bytes.
- `DORA_UFS_FILE_STATUS_CACHE_TTL (alluxio.dora.ufs.file.status.cache.ttl)`  : the maximum time that a cached metadata is valid. When a metadata is loaded into memory cache, its TTL starts to tick. After the TTL reaches zero, the metadata is invalidated and will not be accessible. A subsequent access has to load the metadata again.

### Property Keys for In-RocksDB Cache

- `DORA_WORKER_METASTORE_ROCKSDB_TTL (alluxio.dora.worker.metastore.rocksdb.ttl)`: the maximum time that a cached metadata in RocksDB is valid. When a metadata is loaded into RocksDB cache, its TTL starts to tick. After the TTL reaches zero, the metadata is invalidated and will not be accessible. The cached metadata will be removed from RocksDB, and subsequent access has to load the metadata again.

### Property Keys for In-memory ListStatus Cache

Alluxio workers also cache the result of a ListStatus() request in memory to speed up this operation.
- `DORA_UFS_LIST_STATUS_CACHE_TTL (alluxio.dora.ufs.list.status.cache.ttl)`: the maximum time that a ListStatus() result is cached.
- `DORA_UFS_LIST_STATUS_CACHE_NR_DIRS (alluxio.dora.ufs.list.status.cache.nr.dirs)`: the maximum number of ListStatus() results cached in memory.

## Metadata Invalidation by Client

Alluxio client can also pass the following config to workers in some metadata oriented operations to control the metadata invalidation.
- `USER_FILE_METADATA_SYNC_INTERVAL(alluxio.user.file.metadata.sync.interval)` is the interval for syncing UFS metadata before invoking an operation on a path/file.

"Syncing UFS metadata" means to invalidate the cached metadata if needed and then to reload the metadata from UFS. This key can be configured in `conf/alluxio-site.properties`, or defined as a JVM system property on the command line by `-Dalluxio.user.file.metadata.sync.interval=<integer_value>`.
```shell
$ bin/alluxio fs -Dalluxio.user.file.metadata.sync.interval=0 ls "some/path/and/file"
```
This configuration will be globally effective for all operations and for all paths.

Applications using Alluxio client jar can also do this in a java program like this:
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

## Invalidation of Data Cached in PageStore

When file/object metadata is invalidated, the data associated with the same file/object will also be invalidated from PageStore.
In future, we need a finer-grained way to invalidate the cached data of a file/object, e.g. based on checksum of a file/object.
But as of now, PageStore does not have such information stored with its pages. There will need some optimization.
