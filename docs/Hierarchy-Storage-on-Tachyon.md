---
layout: global
title: Hierarchical storage on Tachyon
---

The hierarchical storage introduces more storage layers besides the existing memory layer, and
there are serveral storage directories in one storage layer, which store block files.

When writing data into Tachyon, client needs to request space from Worker, and Worker will allocate
space on hierarchical storage, Currently the newly coming data is always stored onto top level
storage layer for high speed, and the space is allocated among storage directories in one storage
layer by configurable strategy, If the upper level storage runs out of space, block files will be
evicted to successor storage layer by a configurable strategy to get enough space, if the storage
layer is the last tier, the block files will be deleted. When reading data from Tachyon, client
first get information of the block from master, then lock the block and get path of the block file,
after data is read, updates the access time of the block and unlock the block.

## Configuring hierarchical storage

Use tachyon-env.sh to configure the hierarchy storage by adding properties into Tachyon_JAVA_OPTS,
there are six configurations for hierarchy storage:

    $ tachyon.worker.hierarchystore.level.max
The maximum level of storage tier, it sets the number of storage layers in Tachyon, the default
number is 1, which means there is only one storage layer in Tachyon.

    $ tachyon.worker.hierarchystore.level{x}.alias
The alias of each storage tier, x represents the number of storage layers (starts from 0). There
are pre-defined alias names in StorageLevelAlias, such as MEM, SSD, and HDD etc. Currently only
local file system is supported, more types of storage layer will be added later.

    $ tachyon.worker.hierarchystore.level{x}.dirs.path
The paths of storage directories in each storage layer, which are delimited by comma. x means the
number of the storage layer. it is suggested to set more than one storage directories in one layer.

    $ tachyon.worker.hierarchystore.level{x}.dirs.quota
The quotas for storage directories in each storage layer, which are also delimited by comma. x
means the number of the storage layer. there are default quotas for storage directories in certain
storage layer(512MB for level0, 64GB for level1 and 1TB for level2 and next levels), if the quota
configuration for some storage layer is not set, default value will be used.

    $ tachyon.worker.allocate.strategy
Space allocation strategy, it defines how workers allocate space in storage directories in certain
storage layer. There are three pre-defined strategies: RANDOM, ROUND_ROBIN, and MAX_FREE. RANDOM
means that workers allocate space randomly among storage directories; ROUND_ROBIN means workers
allocate space by round robin among storage directories. MAX_FREE means workers allocate space
in storage directory which has maximum free space, and it is the default strategy used.

    $ tachyon.worker.evict.strategy
Block file eviction strategy, it defines how workers evict block files when a storage layer runs
out of space. Supported strategies are LRU and PARTIAL_LRU. LRU means workers evict blocks by LRU
among storage directories in certain layer, and it is the default strategy. PARTIAL_LRU means
workers evict blocks by LRU in some storage directory selected. More strategies will be added in
future.

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
directory. The path is /mnt/ramdisk with 10GB quota. The alias of the second layer is SSD with two
directories. The paths are /mnt/ssd1 and /mnt/ssd2, with 60GB and 80GB quotas respectively. The
space allocation strategy is MAX_FREE and the block eviction strategy is LRU.

Currently only synchronous eviction is supported by hierarchical storage, it is recommended to use
small block size (less than 64MB), to reduce the latency of block eviction. this restriction will
not exist when asynchronous eviction is introduced.
