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
Default: `{alluxio.work.dir}/metastore`, e.g. `/opt/alluxio/metastore`
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

### Metrics and memory usage
Alluxio exposes all RocksDB metrics found [here](https://github.com/facebook/rocksdb/blob/2b5c29f9f3a5c622031368bf3bf4566f5c590ce5/include/rocksdb/db.h#L1104-L1136).
Alluxio uses one RocksDB database for blocks and one for inodes. Both databases have their own set of metrics. The naming of
such metrics follows the pattern of `rocksdb.name-of-metric > Master.Rocks<Block | Inode>NameOfMetric`. All metrics are 
aggregated across all columns present in RocksDB.

The metrics that concern memory usage are explored in the [RocksDB wiki](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB).
These are important as RocksDB is written in C++ and used through the JNI in Java. This means that RocksDB's memory usage is not
governed by the JVM arguments `-Xmx` and `-Xms`. Here's how they are exposed in Alluxio:
- `Master.RocksBlockBlockCacheUsage` and `Master.RocksInodeBlockCacheUsage` (derived from `rocksdb.block-cache-usage`).
- `Master.RocksBlockEstimateTableReadersMem` and `Master.RocksInodeEstimateTableReadersMem` (derived from `rocksdb.estimate-table-readers-mem`).
- `Master.RocksBlockCurSizeAllMemTables` and `Master.RocksInodeCurSizeAllMemTables` (derived from `rocksdb.cur-size-all-mem-tables`).
- `Master.RocksBlockBlockCachePinnedUsage` and `Master.RocksInodeBlockCachePinnedUsage` (derived from `rocksdb.block-cache-pinned-usage`).

These four metrics are aggregated on a blocks and inodes basis to estimate total memory usage. `Master.RocksBlockEstimatedMemUsage`
and `Master.RocksInodeEstimatedMemUsage` estimate the total memory usage for the blocks table and the inodes table, respectively.
These two metrics are further combined in `Master.RocksTotalEstimatedMemUsage` to estimate the total memory usage of RocksDB across
all of Alluxio.

### RocksDB configuration

RocksDB is highly tunable.
Users familiar with the internals of RocksDB can configure the inode and edge tables through a configuration
file by setting `{alluxio.site.conf.rocks.inode.file}` with the file path. The block metadata and block locations
tables can be configured by setting `{alluxio.site.conf.rocks.block.file}` with the configuration file path.
Template configuration files with default options can be found at `conf/rocks-inode.ini.template` and `conf/rocks-block.ini.template`.
Additional template configuration files can be found at `conf/rocks-inode-bloom.ini.template` and `conf/rocks-block-bloom.ini.template`,
these files have additional options set, such as using bloom filters to improve the speed of lookups,
these options are described below.
A RocksDB example configuration file can be found [here](https://github.com/facebook/rocksdb/blob/main/examples/rocksdb_option_file_example.ini).
Along with the RocksDB metrics described above, the [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
can assist users with designing an appropriate configuration for their workload.
The current defaults are set using the following options in java
```java
new DBOptions()
        .setAllowConcurrentMemtableWrite(false)
        .setCreateMissingColumnFamilies(true)
        .setCreateIfMissing(true)
        .setMaxOpenFiles(-1);
new ColumnFamilyOptions() // for each table (inode, edge, block metadata and block location)
        .useFixedLengthPrefixExtractor(Longs.BYTES) // allows memtable hash buckets by inode id or block id
        .setMemTableConfig(new HashLinkedListMemTableConfig()) // each bucket is a linked list
        .setCompressionType(CompressionType.NO_COMPRESSION)
```

[Not all](https://github.com/facebook/rocksdb/blob/7.2.fb/include/rocksdb/utilities/options_util.h#L22-L72)
RocksDB configuration options are tunable via configuration files, these
must be set using the property keys with the prefix `alluxio.master.metastore.rocks.inode`
and `alluxio.master.metastore.rocks.edge` for the inode tables and `alluxio.master.metastore.rocks.block.meta`
and `alluxio.master.metastore.rocks.block.location` for the block tables. For any property key
that is not set, the default RocksDB value will be used. The different options are
described in the following section.

### Some Configuration Options
The best way to tune RockDB is by running benchmarks and tuning parameters individually for your workload,
but here are some options to get started with.

In the RocksDB SST files the key/value pairs are stored in blocks.
Each time a key or value is read the entire block is loaded from disk, thus
decreasing block size will decrease disk IO, but will also increase
[memory usage](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB#indexes-and-filter-blocks)
as each block has index information stored in memory.
The default block size is 4KB and can be changed for each column family
using the `block_size` configuration option in the configuration files.

Enabling [Bloom filters](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter)
in the blocks can increase the speed of point lookups
in the tables, but will also incur increased CPU and memory usage.
Point lookups are performed when traversing the inode tree, looking up inode metadata,
and looking up block metadata information,
but not when listing directories, or listing the workers for a block id.
Bloom filters can be enabled or disabled using the property keys
`alluxio.master.metastore.rocks.*.bloom.filter`
where `*` is one of `block.location`, `block.meta`, `inode`, or `edge`.

Data blocks can also be configured to use a [hash index](https://github.com/facebook/rocksdb/wiki/Data-Block-Hash-Index)
to improve the speed of point lookups at the cost of increase space usage.
These can be enabled by setting the property key
`alluxio.master.metastore.rocks.*.block.index` to `kDataBlockBinaryAndHash`
where `*` is one of `block.location`, `block.meta`, `inode`, or `edge`.

Alluxio uses a fixed length [prefix extractor](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#prefix-databases)
for each RocksDB column family. The prefix is either the inode ID or the block ID.
To improve the speed of prefix lookups we can include a hash index in the table files
at the cost of increased memory usage.
This can be done by setting `alluxio.master.metastore.rocks.*.index` to
`kHashSearch` where `*` is one of `block.location`, `block.meta`, `inode`, or `edge`.
Furthermore, Bloom filters can be added to the memtable to increase the speed
of prefix lookups by setting the `memtable_prefix_bloom_size_ratio` to a value larger
than 0 and less than 0.25 in the configuration files
(see [the option](https://github.com/facebook/rocksdb/blob/master/java/src/main/java/org/rocksdb/AdvancedMutableColumnFamilyOptionsInterface.java#L57-L67)
for more details).
The memtable Bloom filters can be per key or per prefix by setting
`memtable_whole_key_filtering` to `true` or `false` respectively
in the configuration file.
This can also be configured for the block based Bloom filters
using the `whole_key_filtering` option.


RocksDB will cache some uncompressed blocks in memory, by
default this will use an 8MB LRU cache. Increasing the size of this
cache may increase read performance. Note that changing the size
of this cache may have unexpected impacts on total memory usage and IO
as page cache is also used
([see memory usage in RocksDB](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB)).
The size of the block cache can be changed using the property keys
`alluxio.master.metastore.rocks.*.cache.size`
where `*` is one of `block.location`, `block.meta`, `inode`, or `edge`.

Alluxio has disabled compression in RockDB, this reduces CPU overhead
at the cost of additional space amplification and IO. Compression can
be enabled through the `compression` and `compression_per_level` options
in the RocksDB configuration files. See the [tuning](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
and [compression](https://github.com/facebook/rocksdb/wiki/Compression)
wikis for more information.

RocksDB stores recently modified key-value pairs in memory in a memtable for each column family
(block metadata, block locations, inodes, and inode edges) before flushing them to disk.
By default, this is a 64MB hashtable with linked lists as buckets.
Increasing the memtable size may increase read performance and decrease write amplification,
but the L1 size should also be increased in this case.
Memtable configuration can be done through the RocksDB configuration files.
See [MemTable](https://github.com/facebook/rocksdb/wiki/MemTable) and
[memory usage](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB) for more information
on tuning memtables.

The RocksDB metastore edges column family maps an inode to its children.
By default, the memtable for this is a hashtable with the key being
an inode and the value being a linked list of its children.
If the filesystem creates directories with many children
then lookup performance may be increased by using skip lists instead of linked lists.
This can be enabled by setting the `memtable_factory` option in the RocksDB
configuration file to use the `HashSkipListRepFactory`.

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
Note that backup directory paths must be absolute paths.
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
