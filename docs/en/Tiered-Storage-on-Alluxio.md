---
layout: global
title: Tiered Storage on Alluxio
nickname: Tiered Storage
group: Features
priority: 4
---

* Table of Contents
{:toc}

Alluxio supports tiered storage, which allows Alluxio to manage other storage types in addition to
memory. Currently, Alluxio Tiered Storage supports these storage types or tiers:

* MEM (Memory)
* SSD (Solid State Drives)
* HDD (Hard Disk Drives)

Using Alluxio with tiered storage allows Alluxio to store more data in the system at once,
since memory capacity may be limited in some deployments. With tiered storage, Alluxio
automatically manages blocks between all the configured tiers, so users and administrators do not
have to manually manage the locations of the data. Users may specify their own data management
strategies by implementing [allocators](#allocators) and [evictors](#evictors). In addition, manual
control over tier storage is possible, see [pinning files](#pinning-files).

# Using Tiered Storage

With the introduction of tiers, the data blocks managed by Alluxio are not necessarily in memory;
blocks can be in any of the available tiers. To manage the placement and movement of the blocks,
Alluxio uses *allocators* and *evictors* to place and re-arrange blocks between the tiers. Alluxio
assumes that tiers are ordered from top to bottom based on I/O performance. Therefore, the typical
tiered storage configuration defines the top tier to be MEM, followed by SSD, and finally HDD.

## Storage Directories

A tier is made up of at least one storage directory. This directory is a file path where the Alluxio
blocks should be stored. Alluxio supports configuring multiple directories for a single tier,
allowing multiple mount points or storage devices for a particular tier. For example, if you have
five SSD devices on your Alluxio worker, you can configure Alluxio to use all five devices for the
SSD tier. Configuration for this is described [below](#enabling-and-configuring-tiered-storage).
Choosing which directory the data should placed is determined by the [allocators](#allocators).

## Writing Data

When a user writes a new block, it is written to the top tier by default (a custom allocator can be
used if the default behavior is not desired). If there is not enough space for the block in the top
tier, then the evictor is triggered in order to free space for the new block.

## Reading Data

Reading a data block with tiered storage is similar to standard Alluxio. Alluxio will simply read
the block from where it is already stored. If Alluxio is configured with multiple tiers, then the
block will not be necessarily read from the top tier, since it could have been moved to a lower tier
transparently.

Reading data with the AlluxioStorageType.PROMOTE will ensure the data is first transferred to the
top tier before it is read from the worker. This can also be used as a data management strategy by
explicitly moving hot data to higher tiers.

### Pinning Files

Another way for a user to control the placement and movement of their files is to *pin* and *unpin*
files. When a file is pinned, its blocks will not be evicted. However, users can still promote
blocks of pinned files to move blocks to the top tier.

An example of how to pin a file:

{% include Tiered-Storage-on-Alluxio/pin-file.md %}

Similarly, the file can be unpinned through:

{% include Tiered-Storage-on-Alluxio/unpin-file.md %}

Since blocks of pinned files are no longer candidates for eviction, clients should make sure to
unpin files when appropriate.

## Allocators

Alluxio uses allocators for choosing locations for writing new blocks. Alluxio has a framework for
customized allocators, but there are a few default implementations of allocators. Here are the
existing allocators in Alluxio:

* **GreedyAllocator**

    Allocates the new block to the first storage directory that has sufficient space.

* **MaxFreeAllocator**

    Allocates the block in the storage directory with most free space.

* **RoundRobinAllocator**

    Allocates the block in the highest tier with space, the storage directory is chosen through
    round robin.

In the future, additional allocators will be available. Since Alluxio supports custom allocators,
you can also develop your own allocator appropriate for your workload.

## Evictors

Alluxio uses evictors for deciding which blocks to move to a lower tier, when space needs to be
freed. Alluxio supports custom evictors, and implementations include:

* **GreedyEvictor**

    Evicts arbitrary blocks until the required size is freed.

* **LRUEvictor**

    Evicts the least-recently-used blocks until the required size is freed.

* **LRFUEvictor**

    Evicts blocks based on least-recently-used and least-frequently-used with a configurable weight.
    If the weight is completely biased toward least-recently-used, the behavior will be the same as
    the LRUEvictor.

* **PartialLRUEvictor**

    Evicts based on least-recently-used but will choose StorageDir with maximum free space and
    only evict from that StorageDir.

In the future, additional evictors will be available. Since Alluxio supports custom evictors,
you can also develop your own evictor appropriate for your workload.

When using synchronous eviction, it is recommended to use small block size (around 64MB),
to reduce the latency of block eviction. When using the [space reserver](#space-reserver), block
size does not affect eviction latency.

## Space Reserver

Space reserver makes tiered storage try to reserve certain portion of space on each storage layer
before all space on any given layer is consumed. It will improve the performance of bursty write,
but may also provide marginal performance gain for continuous writes when eviction is continually
running. See the [configuration section](#enabling-and-configuring-tiered-storage) for how to enable
and configure the space reserver.

# Enabling and Configuring Tiered Storage

Tiered storage can be enabled in Alluxio using
[configuration parameters](Configuration-Settings.html). By default, Alluxio only enables a single,
memory tier. To specify additional tiers for Alluxio, use the following configuration parameters:

{% include Tiered-Storage-on-Alluxio/configuration-parameters.md %}

For example, if you wanted to configure Alluxio to have two tiers -- memory and hard disk drive --
you could use a configuration similar to:

{% include Tiered-Storage-on-Alluxio/two-tiers.md %}

Here is the explanation of the example configuration:

* `alluxio.worker.tieredstore.levels=2` configures 2 tiers in Alluxio
* `alluxio.worker.tieredstore.level0.alias=MEM` configures the first (top) tier to be a memory tier
* `alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk` defines `/mnt/ramdisk` to be the file
path to the first tier
* `alluxio.worker.tieredstore.level0.dirs.quota=100GB` sets the quota for the ramdisk to be `100GB`
* `alluxio.worker.tieredstore.level0.reserved.ratio=0.2` sets the ratio of space to be reserved on
top layer to be 0.2
* `alluxio.worker.tieredstore.level1.alias=HDD` configures the second tier to be a hard disk drive tier
* `alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3` configures 3 separate
file paths for the second tier
* `alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB` defines the quota for each of the 3
file paths of the second tier
* `alluxio.worker.tieredstore.level1.reserved.ratio=0.1` sets the ratio of space to be reserved on
the second layer to be 0.1

There are a few restrictions to defining the tiers. First of all, there can be at most 3 tiers.
Also, at most 1 tier can refer to a specific alias. For example, at most 1 tier can have the
alias HDD. If you want Alluxio to use multiple hard drives for the HDD tier, you can configure that
by using multiple paths for `alluxio.worker.tieredstore.level{x}.dirs.path`.

Additionally, the specific evictor and allocator strategies can be configured. Those configuration
parameters are:

{% include Tiered-Storage-on-Alluxio/evictor-allocator.md %}

Space reserver can be configured to be enabled or disabled through:

    alluxio.worker.tieredstore.reserver.enabled=false

# Configuration Parameters For Tiered Storage

These are the configuration parameters for tiered storage.

<table class="table table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
{% for item in site.data.table.tiered-storage-configuration-parameters %}
<tr>
<td>{{ item.parameter }}</td>
<td>{{ item.defaultValue }}</td>
<td>{{ site.data.table.en.tiered-storage-configuration-parameters.[item.parameter] }}</td>
</tr>
{% endfor %}
</table>
