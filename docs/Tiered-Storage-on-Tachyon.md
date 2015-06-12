---
layout: global
title: Tiered storage on Tachyon (Alpha)
---

Tiered storage introduces additional storage layers besides the existing memory layer. Each
layer contains one or more storage directories, where each directory stores block files.

When writing data into Tachyon, a client needs to request space from a worker, and the worker
allocates space on tiered storage. Currently, the newly incoming data is always stored in the top
level storage layer for high speed, and the space is allocated among storage directories in one
storage layer by a configured strategy. If the upper level storage runs out of space, its block
files will be evicted to the lower storage layer by a configured strategy to free space. If
the storage layer is the bottom tier, the block files will be deleted. The location change and
deletion information will be sent to the master with the heartbeat. After a block file is
written, the client requests the worker to cache the block. Then, the worker will update the block
caching information to the master after it is cached. When reading data from Tachyon, a client first
gets the block information from the master. If the block exists on the local worker, the client
requests the worker to lock the block and get the path of the block file. After reading the data,
the client requests the worker to update the block access time and unlock it. If the block does not
exist on local worker, the client will try to get the block data from remote workers which store the
block.

## Configuring tiered storage

Use tachyon-env.sh to configure the tiered storage by adding properties into
Tachyon_JAVA_OPTS, there are six configuration parameters for tiered storage:

    $ tachyon.worker.tieredstore.level.max
The maximum number of storage layers in Tachyon. Its default value is 1, which means there is only
one storage layer.

    $ tachyon.worker.tieredstore.level{x}.alias
The alias of each storage tier, where x represents storage tier number (starting from 0). There
are pre-defined alias names in StorageLevelAlias, such as MEM, SSD, and HDD etc. Currently only
the local file system is supported. More types of storage tiers will be added later.

    $ tachyon.worker.tieredstore.level{x}.dirs.path
The paths of storage directories in each storage tier, which are delimited by comma. x represents
the storage tier number (starting from 0). It is okay to have one directory for the memory layer. It
is suggested to have one storage directory per hardware device for SSD and HDD.

    $ tachyon.worker.tieredstore.level{x}.dirs.quota
The quotas for all storage directories in a storage tier, which are delimited by comma. x
represents the storage tier number (starting from 0). Workers use the corresponding quota in the
configuration for storage directories. If the quota for some storage directories are not set, the
last quota will be used. There is a default quota(128MB) for the storage tier with alias MEM, if the
quota for any other storage layer is not set, the system will report the error and exit the
initialization.

    $ tachyon.worker.allocate.strategy
Space allocation strategy defines how workers allocate space in storage directories in certain
storage layers. There are three pre-defined strategies: RANDOM, ROUND_ROBIN, and MAX_FREE. RANDOM
means that workers allocate space randomly among storage directories; ROUND_ROBIN means workers
allocate space by round robin among storage directories. MAX_FREE means workers allocate space
in storage directory which has maximum free space, and it is the default strategy used.

    $ tachyon.worker.evict.strategy
Block file eviction strategy defines how workers evict block files when a storage layer runs
out of space. Supported strategies are LRU and PARTIAL_LRU. LRU means workers evict blocks by LRU
among storage directories in a certain layer, and it is the default strategy. PARTIAL_LRU means
workers evict blocks by LRU in some storage directory selected. More strategies will be added in
future.

For example:

    -Dtachyon.worker.tieredstore.level.max=2
    -Dtachyon.worker.tieredstore.level0.alias=MEM
    -Dtachyon.worker.tieredstore.level0.dirs.path=/mnt/ramdisk
    -Dtachyon.worker.tieredstore.level0.dirs.quota=10GB
    -Dtachyon.worker.tieredstore.level1.alias=SSD
    -Dtachyon.worker.tieredstore.level1.dirs.path=/mnt/ssd1,/mnt/ssd2
    -Dtachyon.worker.tieredstore.level1.dirs.quota=60GB,80GB
    -Dtachyon.worker.allocate.strategy=MAX_FREE
    -Dtachyon.worker.evict.strategy=LRU

In this example, there are two storage layers. The alias of the first layer is MEM with one
directory. The path is /mnt/ramdisk with 10GB quota. The alias of the second layer is SSD with two
directories. The paths are /mnt/ssd1 and /mnt/ssd2, with 60GB and 80GB quotas respectively. The
space allocation strategy is MAX_FREE and the block eviction strategy is LRU.

Currently only synchronous eviction is supported by tiered storage. It is recommended to use
small block size (less than 64MB), to reduce the latency of block eviction. This restriction will
not exist when asynchronous eviction is introduced.
