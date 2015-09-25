---
layout: global
title: Tiered Storage on Tachyon (Beta)
nickname: Tiered Storage
group: More
---

Tachyon supports tiered storage, which allows Tachyon to manage other storage types besides memory.
Currently, Tachyon Tiered Storage supports these storage types or tiers:

* MEM (Memory)
* SSD (Solid State Drives)
* HDD (Hard Drives)

Using Tachyon with tiered storage allows Tachyon to manage more space in the Tachyon namespace,
since MEM capacity may still be limited in some deployments. With tiered storage, Tachyon 
automatically manages blocks between all the configured tiers, so users and administrators do not
have to manually manage the locations of the data.

# Using Tiered Storage

With the introduction of tiers, the data blocks managed by Tachyon are not necessarily in memory;
blocks can be in any of the available tiers. To manage the placement and movement of the blocks,
Tachyon uses *allocators* and *evictors* to place and re-arrange blocks between the tiers. In
Tachyon, the tiers are ordered from fastest to slowest, from top to bottom. Therefore, the typical
tiered storage configuration defines the top tier to be MEM, followed by SSD, and finally HDD.

## Storage Directories

A tier is made up of at least 1 storage directory. This directory is a file path where the Tachyon
blocks should be stored. Tachyon supports configuring multiple directories for a single tier,
allowing multiple mount points or storage devices for a particular tier. For example, if you have
five SSD devices on your Tachyon worker, you can configure Tachyon to use all five devices for the
SSD tier. Configuration for this is described below. Choosing which directory the data should placed
is determined by the allocator, described later.

## Writing Data

When a user writes a new block, it is written to the MEM tier by default (a custom allocator can be
used if the default behavior is not desired). If there is not enough space for the block in the MEM
tier, then the evictor is triggered in order to free space for the new block.

## Reading Data

Reading a data block with tiered storage is similar to standard Tachyon. Tachyon will simply read
the block from where it is already stored. If Tachyon is configured with multiple tiers, then the
block will not be necessarily read from the MEM tier, since it could have been moved to a lower tier
transparently.

### Promoting Blocks

Since the movement of blocks is transparent to the user (via allocators and evictors), Tachyon
provides client APIs to *force* a block to be moved to the top tier. These APIs are:

    TachyonFS.promoteBlock(long blockId)
    TachyonFile.promoteBlock(int blockIndex)

Using these methods, a user/client can force a block to be placed in the top layer of the Tachyon
worker. Please note, promoting a block may trigger an eviction if there is insufficient space in the
top tier.

### Pinning Files

Another way for a user to control the placement and movement of their files is to *pin* and *unpin*
files. When a file is pinned, its blocks will not be evicted or moved by the evictor. However, users
can still promote blocks of pinned files to move blocks to the top tier.

The API to pin a file is:

    TachyonFS.pinFile(int fid)

and the API to unpin a file is:

    TachyonFS.unpinFile(int fid)

Since blocks of pinned files are no longer candidates for eviction, clients should make sure to
unpin files when appropriate.

## Allocators

Tachyon uses allocators for choosing locations for writing new blocks. Tachyon has a framework for
customized allocators, but there are a few default implementations of allocators. Here are the
existing allocators in Tachyon:

* GreedyAllocator: allocates the new block to the first storage directory that has sufficient space
* MaxFreeAllocator: allocates the block in the storage directory with most free space.

In the future, additional allocators will be available. Since Tachyon supports custom allocators,
you can also develop your own allocator appropriate for your workload.

## Evictors

Tachyon uses evictors for deciding which blocks to move to a lower tier, when space needs to be
freed. Tachyon supports custom evictors, and implementations include:

* GreedyEvictor: evicts arbitrary blocks until the required size is freed
* LRUEvictor: evicts the least-recently-used blocks until the required size is freed

In the future, additional evictors will be available. Since Tachyon supports custom evictors,
you can also develop your own evictor appropriate for your workload.

Currently only synchronous eviction is supported by tiered storage. It is recommended to use
small block size (less than 64MB), to reduce the latency of block eviction. This restriction will
not exist when asynchronous eviction is introduced.

## Space Reserver

Space reserver makes tiered storage try to reserve certain portion of space on each storage layer
before all space on some layer is consumed. It will be good for burst write performance, but for
continous write, since the bottleneck is eviction speed, it will get very little performance gain. 

# Enabling and Configuring Tiered Storage

Tiered storage can be enabled in Tachyon with some
[configuration parameters](Configuration-Settings.html). By default, Tachyon only enables a single,
MEM tier. To specify additional tiers for Tachyon, use the following configuration parameters:

    tachyon.worker.tieredstore.level.max
    tachyon.worker.tieredstore.level{x}.alias
    tachyon.worker.tieredstore.level{x}.dirs.quota
    tachyon.worker.tieredstore.level{x}.dirs.path
    tachyon.worker.tieredstore.level{x}.reserved.ratio

For example, if you wanted to configure Tachyon to have two tiers, of MEM and HDD, it would be
configured something like:

    tachyon.worker.tieredstore.level.max=2
    tachyon.worker.tieredstore.level0.alias=MEM
    tachyon.worker.tieredstore.level0.dirs.path=/mnt/ramdisk
    tachyon.worker.tieredstore.level0.dirs.quota=100GB
    tachyon.worker.tieredstore.level0.reserved.ratio=0.2
    tachyon.worker.tieredstore.level1.alias=HDD
    tachyon.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3
    tachyon.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB
    tachyon.worker.tieredstore.level1.reserved.ratio=0.1

Here is the explanation of the example configuration:

* `tachyon.worker.tieredstore.level.max=2` configures 2 tiers in Tachyon.
* `tachyon.worker.tieredstore.level0.alias=MEM` configures the first (top) tier to be the MEM tier.
* `tachyon.worker.tieredstore.level0.dirs.path=/mnt/ramdisk` defines `/mnt/ramdisk` to be the file
path to the ramdisk.
* `tachyon.worker.tieredstore.level0.dirs.quota=100GB` sets the quota for the ramdisk to be `100GB`.
* `tachyon.worker.tieredstore.level0.reserved.ratio=0.2` sets the ratio of space to be reserved on
top layer to be 0.1.
* `tachyon.worker.tieredstore.level1.alias=HDD` configures the second tier to be HDD.
* `tachyon.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3` configures 3 separate
file paths for the HDD tier.
* `tachyon.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB` defines the quota for each of the 3
file paths of the HDD tier.
* `tachyon.worker.tieredstore.level1.reserved.ratio=0.1` sets the ratio of space to be reserved on
the second layer to be 0.1.

There are a few restrictions to defining the tiers. First of all, there can be at most 3 tiers.
Also, at most 1 tier can refer to a specific alias. For example, at most 1 tier can have the alias
HDD. If you want Tachyon to use multiple hard drives for the HDD tier, you can configure that as
multiple storage directories in the HDD tier.

Additionally, the specific evictor and allocator strategies can be configured. Those configuration
parameters are:

    tachyon.worker.allocate.strategy.class=tachyon.worker.block.allocator.MaxFreeAllocator
    tachyon.worker.evict.strategy.class=tachyon.worker.block.evictor.LRUEvictor

Space reserver can be configured to be enabled or disabled, the configuration parameter is:

    tachyon.worker.space.reserver.enable=false

# Configuration Parameters For Tiered Storage

These are the configuration parameters for tiered storage.

<table class="table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
<tr>
  <td>tachyon.worker.tieredstore.level.max</td>
  <td>1</td>
  <td>
  The maximum number of storage tiers in Tachyon. Currently, Tachyon supports 1, 2, or 3 tiers.
  </td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.level{x}.alias</td>
  <td>MEM (for tachyon.worker.tieredstore.level0.alias)</td>
  <td>
  The alias of each storage tier, where x represents storage tier number (top tier is 0). Currently,
  there are 3 aliases, MEM, SSD, and HDD.
  </td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.level{x}.dirs.path</td>
  <td>/mnt/ramdisk (for tachyon.worker.tieredstore.level0.dirs.path)</td>
  <td>
  The paths of storage directories in storage tier x, delimited by comma. x represents the storage
  tier number (top tier is 0). It is suggested to have one storage directory per hardware device for
  the SSD and HDD tiers.
  </td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.level{x}.dirs.quota</td>
  <td>128MB (for tachyon.worker.tieredstore.level0.dirs.quota)</td>
  <td>
  The quotas for all storage directories in storage tier x, delimited by comma. x represents the
  storage tier number (starting from 0). For a particular storage tier, if the list of quotas is
  shorter than the list of directories of that tier, then the quotas for the remaining directories
  will just use the last-defined quota. Quota definitions use these suffixes: KB, MB, GB, TB, PB.
  </td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.level{x}.reserved.ratio</td>
  <td>0.1</td>
  <td>
  The portion of space reserved on storage tier x.
  </td>
</tr>
<tr>
  <td>tachyon.worker.space.reserver.enable</td>
  <td>false</td>
  <td>
  Whether enabling space reserver service.
  </td>
</tr>
<tr>
  <td>tachyon.worker.space.reserver.interval.ms</td>
  <td>1000</td>
  <td>
  The period of space reserver service.
  </td>
</tr>
<tr>
  <td>tachyon.worker.allocate.strategy.class</td>
  <td>tachyon.worker.block.allocator.MaxFreeAllocator</td>
  <td>
  The class name of the allocation strategy to use for new blocks in Tachyon. Currently, the
  supported allocators are GreedyAllocator and MaxFreeAllocator.
  </td>
</tr>
<tr>
  <td>tachyon.worker.evict.strategy.class</td>
  <td>tachyon.worker.block.evictor.LRUEvictor</td>
  <td>
  The class name of the block eviction strategy to use when a storage layer runs out of space.
  Currently, the supported evictors are GreedyEvictor and LRUEvictor.
  </td>
</tr>
</table>
