---
layout: global
title: Metastore Management
nickname: Metastore Management
group: Operations
priority: 6
---

* Table of Contents
{:toc}

Alluxio stores most of its metadata on the master node. The metadata includes the
filesystem tree, file permissions, and block locations. Alluxio provides two ways
to store the metadata:
  * `ROCKS`: an on-disk, RocksDB-based metastore
  * `HEAP`: an on-heap metastore

The default metastore is the `ROCKS` metastore.

## RocksDB Metastore

The ROCKS metastore uses a mixture of memory and disk. Metadata eventually gets
written to disk, but a large, in-memory cache stores recently-accessed metadata. The
cache buffers writes, asynchronously flushing them to RocksDB as the cache fills up.
The default cache size is dynamically set to be (JVM Heap size / 4KB).
In the default configuration of 8GB heap size, Alluxio master will cache 2 million inodes.
The cache performs LRU-style eviction, with an asynchronous evictor evicting from a
high water mark (default 85%) down to a low water mark (default 80%).

You can explicitly configure Alluxio to use the ROCKS metastore by adding this setting
in `conf/alluxio-site.properties` for the master nodes.

```properties
alluxio.master.metastore=ROCKS
```

### Configuration Properties

* `alluxio.master.metastore.dir`: A local directory for writing RocksDB data.
Default: `${work_dir}/metastore`
* `alluxio.master.metastore.inode.cache.max.size`: A hard limit on the number of entries in the on-heap inode cache.
Increase this to improve performance if you have spare master memory. 
The default value for this configuration is dynamically set to be (JVM Heap size / 4KB).
For example, when xmx setting is set to the default 8GB, Alluxio master will cache 2 million inodes.

### Advanced Tuning Properties

These tuning parameters primarily affect the behavior of the cache.

* `alluxio.master.metastore.inode.cache.evict.batch.size`: Batch size for flushing cache
  modifications to RocksDB. Default: `1000`
* `alluxio.master.metastore.inode.cache.high.water.mark.ratio`: Ratio of the maximum cache size
  where the cache begins evicting. Default: `0.85`
* `alluxio.master.metastore.inode.cache.low.water.mark.ratio`: Ratio of the maximum cache size
  that eviction will evict down to. Default: `0.8`

## Heap Metastore

The heap metastore is simple: it stores all metadata on the heap. This gives consistent,
fast performance, but the required memory scales with the number of files in the
filesystem. With 64GB of Alluxio master memory, Alluxio can support around 30 million files.

To configure Alluxio to use the on-heap metastore, set the following in
`conf/alluxio-site.properties` for the master nodes:

```properties
alluxio.master.metastore=HEAP
```

## Switching between RocksDB MetaStore and Heap MetaStore

Alluxio master stores different journal information for RocksDB metastore and heap metastore.
Switching the metastore type of an existing Alluxio requires formatting Alluxio journal which will wipe
all Alluxio metadata. If you would like to keep the existing data in Alluxio cluster after the switch,
you will need to perform a journal backup and restore:

First, run the journal backup command while Alluxio master is running.

```console
$ ./bin/alluxio fsadmin backup
```

This will print something like

```
Backup Host        : ${HOST_NAME}
Backup URI         : ${BACKUP_PATH}
Backup Entry Count : ${ENTRY_COUNT}
```

By default, this will write a backup named
`alluxio-backup-YYYY-MM-DD-timestamp.gz` to the `/alluxio_backups` directory of
the root under storage system, e.g. `hdfs://cluster/alluxio_backups`. This default
backup directory can be configured by setting `alluxio.master.backup.directory`

Alternatively, you may use the `--local <DIRECTORY>` flag to
specify a path to write the backup to on the local disk of the primary master.
For example:

```console
$ ./bin/alluxio fsadmin backup --local /tmp/alluxio_backup
```

Then stop Alluxio masters:

```console
$ ./bin/alluxio-stop.sh masters
```

Update the metastore type in `conf/alluxio-site.properties` for the master nodes:

```properties
alluxio.master.metastore=<new value>
```

Format the masters with the following command:

```console
$ ./bin/alluxio formatMasters
```

Then restart the masters with the `-i ${BACKUP_PATH}` argument, replacing
`${BACKUP_PATH}` with your specific backup path.

```console
$ ./bin/alluxio-start.sh -i ${BACKUP_PATH} masters
```

See [here]({{ '/en/operation/Journal.html' | relativize_url }}#backing-up-the-journal)
for more information regarding journal backup.
