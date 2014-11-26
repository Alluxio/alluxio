---
layout: global
title: Hierarchy storage on Tachyon
---

The hierarchy storage introduces several more storage layers besides the existing single memory 
cache layr. currently the newly coming data is always cached onto top level storage for high speed.
if upper level storage runs out of space, the block files will be evicted to successor storage layer
by certain strategy.

## Configuring hierarchy storage

Use tachyon-env.sh to configure the hierarchy storage by adding properties into Tachyon_JAVA_OPTS,
there are six configurations for hierarchy storage:

    $ tachyon.worker.hierarchystore.level.max: the maximum level of storage tier, it sets the
number of storage layers in Tachyon, the default number is 1, which means there is only one storage
layer in Tachyon.

    $ tachyon.worker.hierarchystore.level{x}.alias: the alias of each storage tier, x here means
the number of the storage layer(starts from 0). There are pre-defined alias names in
StorageLevelAlias, such as MEM SSD HDD etc, currently only local file system is supported, more
types of storage layer will be added later.

    $ tachyon.worker.hierarchystore.level{x}.dirs.path: the paths of storage directories in each
storage layer, which are delimited by comma. x means the number of the storage layer. it is
suggested to set more than one storage directories in one layer.

    $ tachyon.worker.hierarchystore.level{x}.dirs.quota: the quotas for storage directories in each
storage layer, which are also delimited by comma. x means the number of the storage layer. there are
default quotas for storage directories in certain storage layer(512MB for level0, 64GB for level1
and 1TB for level2 and next levels), if the quota configuration for some storage layer is not set,
default value will be used. 

    $ tachyon.worker.allocate.strategy: the space allocation strategy which defines how worker
allocates space in storage directories in certain storage layer, there are three strategies
pre-defined: RANDOM ROUND_ROBIN MAX_FREE. RANDOM means worker allocates space randomly among storage
directories; ROUND_ROBIN means worker allocates space by round robin among storage directories.
MAX_FREE means worker allocates space in storage directory which has maximum free space, and it is
the default strategy used. 

    $ tachyon.worker.evict.strategy: the block file eviction strategy which defines how worker
evicts block files when storage layer runs out of space. currently LRU based strategies are
realized: LRU PARTIAL_LRU, LRU means worker evict blocks by LRU among storage directories in certain
layer, and it is the default strategy used. PARTIAL_LUR means worker evict blocks by LRU in some
storage directory selected. more strategies will be introduced in future.

For example:
-Dtachyon.worker.hierarchystore.level.max=2
-Dtachyon.worker.hierarchystore.level0.alias=MEM
-Dtachyon.worker.hierarchystore.level0.dirs.path=/mnt/ramdisk
-Dtachyon.worker.hierarchystore.level0.dirs.quota=10G
-Dtachyon.worker.hierarchystore.level1.alias=SSD
-Dtachyon.worker.hierarchystore.level1.dirs.path=/mnt/ssd1,/mnt/ssd2
-Dtachyon.worker.hierarchystore.level1.dirs.quota=60G,80G
-Dtachyon.worker.allocate.strategy=MAX_FREE
-Dtachyon.worker.evict.strategy=LRU

It means two storage layers are set. the alias of first layer is MEM, one directory is set. the path
of the directory is /mnt/ramdisk, and the quota is 10G. the alias of second layer is SSD, two
directories are set. the paths of two directories are /mnt/ssd1 and /mnt/ssd2. and quotas for two
directories are 60G and 80G. the space allocation strategy is MAX_FREE and the block eviction
strategy is LRU.

Currently only synchronous eviction is supported by hiearchy storage, it is recommanded to use
small block size(less than 64MB), to reduce latency of block eviction. this restraition will not
exist when asynchronus eviction is introduced.
