---
layout: global
title: Alluxio Storage
nickname: Alluxio Storage
group: Features
priority: 1
---

* Table of Contents
{:toc}

Alluxio manages the local storage, including memory, of Alluxio workers to act as a distributed
buffer cache. This fast data layer between user applications and the various under storages results
in vastly improved I/O performance.

The amount and type of storage for each Alluxio node to manage is determined by user configuration.
Alluxio also supports tiered storage, which makes the system storage media aware, enabling data
storage optimizations similar to L1/L2 cpu caches.

## Configuring Alluxio Storage

The easiest way to configure Alluxio storage is to use the default single-tier mode.

Note that this doc refers to local storage and terms like `mount` refer to mounting in the local
filesystem, not to be confused with Alluxio's `mount` concept for under storages.

Out-of-the-box Alluxio will provision a ramdisk on every worker and take a percentage of the
system's total memory. This ramdisk will be used as the only storage medium allocated to each
Alluxio worker.

Alluxio storage is configured through Alluxio's configuration in `alluxio-site.properties`. See the
[configuration docs](Configuration-Settings.html) for detailed information.

A common modification to the default is to explicitly set the ramdisk size. For example, to set the
ramdisk size to be 16GB on each worker:

```
alluxio.worker.memory.size=16GB
```

Another common change is to specify multiple storage media, such as ramdisk and SSDs. We will need
to update `alluxio.worker.tieredstore.level0.dirs.path` to take specify each storage medium we want
to use as a storage directory. For example, to use the ramdisk (mounted at `/mnt/ramdisk`) and two
SSDs (mounted at `/mnt/ssd1` and `/mnt/ssd2`):

```
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk,/mnt/ssd1,/mnt/ssd2
```

The paths provided should point to paths in the local filesystem mounting the appropriate storage
media. To enable short circuit operations, the permissions of these paths should be permissive for the
client user to read, write and execute on the path. For example, 770 is needed for the client user who is
among the same group of the user that starts the Alluxio service.

After updating the storage media, we need to indicate how much storage is allocated for each storage
directory. For example, if we wanted to use 16 GB on the ramdisk and 100 GB on each SSD:

```
alluxio.worker.tieredstore.level0.dirs.quota=16GB,100GB,100GB
```

Note that the ordering of the quotas must match with the ordering of the paths.

There is a subtle difference between `alluxio.worker.memory.size` and
`alluxio.worker.tieredstore.level0.dirs.quota` (which defaults to the former). Alluxio will
provision and mount a ramdisk when started with the `Mount` or `SudoMount` options. This ramdisk
will have its size determined by `alluxio.worker.memory.size`, regardless of the value set in
`alluxio.worker.tieredstore.level0.dirs.quota`. Similarly, the quota should be set independently
of the memory size if devices other than the default Alluxio provisioned ramdisk are to be used.

## Eviction

Because Alluxio storage is designed to be volatile, there must be a mechanism to make space for new
data when Alluxio storage is full. This is termed eviction.

There are two modes of eviction in Alluxio, asynchronous (default) and synchronous. You can switch
between the two by enabling and disabling the space reserver which handles asynchronous eviction.
For example, to turn off asynchronous eviction:

```
alluxio.worker.tieredstore.reserver.enabled=false
```

Asynchronous eviction is the default implementation of eviction. It relies on a periodic space reserver 
thread in each worker to evict data. It waits until the worker storage utilization reaches a configurable
 high watermark. Then it evicts data based on the eviction policy until it reaches the configurable low
  watermark. For example, if we had the same 16+100+100=216GB storage configured, we can set eviction to
  kick in at around 200GB and stop at around 160GB:

```
alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9 # 216GB * 0.9 ~ 200GB
alluxio.worker.tieredstore.level0.watermark.low.ratio=0.75 # 216GB * 0.75 ~ 160GB
```

In write or read-cache heavy workloads, asynchronous eviction can improve performance.

Synchronous eviction waits for a client to request more space than is currently available on the worker 
and then kicks off the eviction process to free up enough space to serve that request. This leads to many 
small eviction attempts, which is less efficient but maximizes the utilization of available Alluxio space.

In addition, if you wish files not be evicted, [`pin`](Command-Line-Interface.html#pin) command can be
used. Once a file is pinned, any blocks belonging to the file will never be evicted from an Alluxio
worker. Conversely, you can invoke [`unpin`](Command-Line-Interface.html#unpin) command to make its data
blocks evicted from the various Alluxio workers containing the block.

### Evictors

Alluxio uses evictors for deciding which blocks to evict, when space needs to be
freed. Users can specify the Alluxio evictor to achieve fine grained control over the eviction
process.

Alluxio supports custom evictors. Out-of-the-box implementations include:

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

When using synchronous eviction, it is recommended to use smaller block sizes (around 64-128MB),
to reduce the latency of block eviction. When using the space reserver, block size does not affect
eviction latency.

## Using Tiered Storage

For typical deployments, it is recommended to use a single storage tier with heterogeneous storage
media. However, in certain environments, workloads will benefit from explicit ordering of storage
media based on I/O speed. In this case, tiered storage should be used. When tiered storage is
enabled, the eviction process intelligently accounts for the tier concept. Alluxio assumes that
tiers are ordered from top to bottom based on I/O performance. For example, users often specify the
following tiers:

 * MEM (Memory)
 * SSD (Solid State Drives)
 * HDD (Hard Disk Drives)

### Writing Data

When a user writes a new block, it is written to the top tier by default. If eviction cannot free up
space in the top tier, the write will fail. If the file size exceeds the size of the top tier, the
write will also fail.

The user can also specify the tier that the data will be written to via
[configuration settings](#configuration-parameters-for-tiered-storage).

Reading data with the ReadType.CACHE or ReadType.CACHE_PROMOTE will also result in the data being
written into Alluxio. In this case, the data is always written to the top tier.

Finally, data in written into Alluxio via the load command. In this case also, the data is always
written to the top tier.

### Reading Data

Reading a data block with tiered storage is similar to standard Alluxio. If the data is already in
Alluxio, the client will simply read the block from where it is already stored. If Alluxio is
configured with multiple tiers, then the block will not be necessarily read from the top tier, since
it could have been moved to a lower tier transparently.

Reading data with the ReadType.CACHE_PROMOTE will ensure the data is first transferred to the
top tier before it is read from the worker. This can also be used as a data management strategy by
explicitly moving hot data to higher tiers.

### Enabling and Configuring Tiered Storage

Tiered storage can be enabled in Alluxio using
[configuration parameters](Configuration-Settings.html). To specify additional tiers for Alluxio,
use the following configuration parameters:

```
alluxio.worker.tieredstore.levels
alluxio.worker.tieredstore.level{x}.alias
alluxio.worker.tieredstore.level{x}.dirs.quota
alluxio.worker.tieredstore.level{x}.dirs.path
alluxio.worker.tieredstore.level{x}.watermark.high.ratio
alluxio.worker.tieredstore.level{x}.watermark.low.ratio
```

For example, if you wanted to configure Alluxio to have two tiers -- memory and hard disk drive --
you could use a configuration similar to:

```
alluxio.worker.tieredstore.levels=2
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk
alluxio.worker.tieredstore.level0.dirs.quota=100GB
alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9
alluxio.worker.tieredstore.level0.watermark.low.ratio=0.7
alluxio.worker.tieredstore.level1.alias=HDD
alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3
alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB
alluxio.worker.tieredstore.level1.watermark.high.ratio=0.9
alluxio.worker.tieredstore.level1.watermark.low.ratio=0.7
```

Here is the explanation of the example configuration:

* `alluxio.worker.tieredstore.levels=2` configures 2 tiers in Alluxio
* `alluxio.worker.tieredstore.level0.alias=MEM` configures the first (top) tier to be a memory tier
* `alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk` defines `/mnt/ramdisk` to be the file
path to the first tier
* `alluxio.worker.tieredstore.level0.dirs.quota=100GB` sets the quota for the ramdisk to be `100GB`
* `alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9` sets the ratio of high watermark on
top layer to be 0.9
* `alluxio.worker.tieredstore.level0.watermark.low.ratio=0.7` sets the ratio of low watermark on
top layer to be 0.7
* `alluxio.worker.tieredstore.level1.alias=HDD` configures the second tier to be a hard disk tier
* `alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3` configures 3 separate
file paths for the second tier
* `alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB` defines the quota for each of the 3
file paths of the second tier
* `alluxio.worker.tieredstore.level1.watermark.high.ratio=0.9` sets the ratio of high watermark on
the second layer to be 0.9
* `alluxio.worker.tieredstore.level1.watermark.low.ratio=0.7` sets the ratio of low watermark on
the second layer to be 0.7

There are a few restrictions to defining the tiers. There is no restriction on the number of tiers,
however, a common configuration has 3 tiers - Memory, HDD and SSD. At most 1 tier can refer to a
specific alias. For example, at most 1 tier can have the alias HDD. If you want Alluxio to use
multiple hard drives for the HDD tier, you can configure that by using multiple paths for
`alluxio.worker.tieredstore.level{x}.dirs.path`.

### Configuration Parameters For Tiered Storage

These are the configuration parameters for tiered storage.

<table class="table table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
{% for item in site.data.table.tiered-storage-configuration-parameters %}
<tr>
<td>{{ item.parameter }}</td>
<td>{{ item.defaultValue }}</td>
<td>{{ site.data.table.en.tiered-storage-configuration-parameters[item.parameter] }}</td>
</tr>
{% endfor %}
</table>
