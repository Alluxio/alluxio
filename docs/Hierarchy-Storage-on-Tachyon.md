---
layout: global
title: Hierarchical storage on Tachyon
---

The hierarchical storage introduces more storage layers besides the existing memory layer. Currently
the newly coming data is always stored onto top level storage for high speed. If the upper level
storage runs out of space, the block files will be evicted to successor storage layer by a
configurable strategy.

## Configuring hierarchical storage

Use tachyon-env.sh to configure the hierarchy storage by adding properties into Tachyon_JAVA_OPTS,
there are six configurations for hierarchy storage:

    $ tachyon.worker.hierarchystore.level.max: the maximum level of storage tier, it sets the
number of storage layers in Tachyon, the default number is 1, which means there is only one storage
layer in Tachyon.

    $ tachyon.worker.hierarchystore.level{x}.alias: the alias of each storage tier, x represents the
number of storage layers (starts from 0). There are pre-defined alias names in StorageLevelAlias,
such as MEM, SSD, and HDD etc. Currently only local file system is supported, more types of storage
layer will be added later.

    $ tachyon.worker.hierarchystore.level{x}.dirs.path: the paths of storage directories in each
storage layer, which are delimited by comma. x means the number of the storage layer. it is
suggested to set more than one storage directories in one layer.

    $ tachyon.worker.hierarchystore.level{x}.dirs.quota: the quotas for storage directories in each
storage layer, which are also delimited by comma. x means the number of the storage layer. there are
default quotas for storage directories in certain storage layer(512MB for level0, 64GB for level1
and 1TB for level2 and next levels), if the quota configuration for some storage layer is not set,
default value will be used.

    $ tachyon.worker.allocate.strategy: space allocation strategy defines how workers allocate space
in storage directories in certain storage layer. There are three pre-defined strategies: RANDOM,
ROUND_ROBIN, and MAX_FREE. RANDOM means that workers allocate space randomly among storage
directories; ROUND_ROBIN means worker allocates space by round robin among storage directories.
MAX_FREE means worker allocates space in storage directory which has maximum free space, and it is
the default strategy used.

    $ tachyon.worker.evict.strategy: block file eviction strategy defines how workers evict block
files when a storage layer runs out of space. Supported strategies are LRU and PARTIAL_LRU. LRU
means workers evict blocks by LRU among storage directories in certain layer, and it is the default
strategy. PARTIAL_LRU means workers evict blocks by LRU in some storage directory selected. More
strategies will be introduced in future.

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

In this example, there are two storage layers. The alias of the first layer is MEM with one
directory. The folder path is /mnt/ramdisk with 10GB quota. The alias of the second layer is SSD
with two directories. The paths are /mnt/ssd1 and /mnt/ssd2, with 60GB and 80GB quotas respectively.
The space allocation strategy is MAX_FREE and the block eviction strategy is LRU.

Currently only synchronous eviction is supported by hierarchical storage, it is recommended to use
small block size (less than 64MB), to reduce the latency of block eviction. this restriction will
not exist when asynchronous eviction is introduced.
