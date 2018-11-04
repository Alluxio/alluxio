---
layout: global
title: Alluxio Storage Management
nickname: Alluxio Storage Management
group: Advanced
priority: 0
---

* Table of Contents
{:toc}

## Alluxio Storage Overview

The purpose of this documentation is to introduce users to the concepts behind Alluxio storage and
the operations that can be performed within Alluxio storage space. For metadata related operations
such as syncing and namespaces, refer to the
[page on namespace management]({{ '/en/advanced/Namespace-Management.html' | relativize_url }})

Alluxio helps unify users' data across a variety of platforms while also helping to increase
overall I/O throughput from a user's perspective. Alluxio accomplishes this by splitting storage
into two distinct categories.

- **UFS (Under File Storage, also referred to as the under storage)**
    - This storage is represented as the space which is not managed by Alluxio.
    UFS storage may come from an external file system, including HDFS or S3.
    Alluxio may connect to one or more of these UFSs and expose them within a single namespace.
    - Typically, UFS storage is aimed at storing large amounts of data persistently for extended
    periods of time.
- **Alluxio storage**
    - Alluxio manages the local storage, including memory, of Alluxio workers to act as a
    distributed buffer cache. This fast data layer between user applications and the various under
    storages results in vastly improved I/O performance.
    - Alluxio storage is mainly aimed at storing hot, transient data and is not focused on long term
    persistence.
    - The amount and type of storage for each Alluxio node to manage is determined by user
    configuration.
    - Even if data is not currently within Alluxio storage, files within a connected UFS are still
    visible to Alluxio clients. The data is copied into Alluxio storage when a client attempts to
    read a file that is only available from a UFS.


![Alluxio storage diagram]({{ '/img/stack.png' | relativize_url }})

Alluxio storage improves performance by storing data in memory co-located with compute nodes.
Data in Alluxio storage can be replicated to make "hot" data more readily available for
I/O operations to consume in parallel.

Replicas of data within Alluxio are independent of the replicas that may exist within a UFS.
The number of data replicas within Alluxio storage is determined dynamically by cluster activity.
Due to the fact that Alluxio relies on the under file storage for a majority of data storage,
Alluxio does not need to keep copies of data that are not being used.

Alluxio also supports tiered storage, which makes the system storage media aware, enabling data
storage optimizations similar to L1/L2 cpu caches.

## Configuring Alluxio Storage

### Single-Tier Storage

The easiest way to configure Alluxio storage is to use the default single-tier mode.

Note that this page refers to local storage and terms like `mount` refer to mounting in the local
filesystem, not to be confused with Alluxio's `mount` concept for external under storages.

On startup, Alluxio will provision a ramdisk on every worker and take a percentage of the
system's total memory. This ramdisk will be used as the only storage medium allocated to each
Alluxio worker.

Alluxio storage is configured through Alluxio's configuration in `alluxio-site.properties`. See
[configuration settings]({{ '/en/basic/Configuration-Settings.html' | relativize_url }}) for detailed
information.

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
client user to read, write, and execute on the path. For example, `770` permissions is needed for
the client user who is among the same group of the user that starts the Alluxio service.

After updating the storage media, we need to indicate how much storage is allocated for each storage
directory. For example, if we wanted to use 16 GB on the ramdisk and 100 GB on each SSD:

```
alluxio.worker.tieredstore.level0.dirs.quota=16GB,100GB,100GB
```

Note that the ordering of the quotas must match with the ordering of the paths.

There is a subtle difference between `alluxio.worker.memory.size` and
`alluxio.worker.tieredstore.level0.dirs.quota`, which defaults to the former. Alluxio will
provision and mount a ramdisk when started with the `Mount` or `SudoMount` options. This ramdisk
will have its size determined by `alluxio.worker.memory.size` regardless of the value set in
`alluxio.worker.tieredstore.level0.dirs.quota`. Similarly, the quota should be set independently
of the memory size if devices other than the default Alluxio provisioned ramdisk are to be used.

### Multiple-Tier Storage

It is typically recommended to use a single storage tier with heterogeneous storage media.
In certain environments, workloads will benefit from explicit ordering of storage media based on
I/O speed. When tiered storage is enabled, the eviction process intelligently accounts for the
ordering of tiers. Alluxio assumes that tiers are ordered from top to bottom based on I/O performance.
For example, users often specify the following tiers:

 * MEM (Memory)
 * SSD (Solid State Drives)
 * HDD (Hard Disk Drives)

#### Writing Data

When a user writes a new block, it is written to the top tier by default. If eviction cannot free up
space in the top tier, the write will fail. If the file size exceeds the size of the top tier, the
write will also fail.

The user can also specify the tier that the data will be written to via
[configuration settings](#configure-tiered-storage).

When reading data with the `ReadType.CACHE` or `ReadType.CACHE_PROMOTE`,
the data written into Alluxio is always written to the top tier.
When writing data using the load command, the data is also written to the top tier.

#### Reading Data

If the data is already in Alluxio, the client will simply read the block from where it is already stored.
If Alluxio is configured with multiple tiers, the block may not be necessarily read from the top tier,
since it could have been moved to a lower tier transparently.

Reading data with the `ReadType.CACHE_PROMOTE` will ensure the data is first transferred to the
top tier before it is read from the worker. This can also be used as a data management strategy by
explicitly moving hot data to higher tiers.

#### Configuring Tiered Storage

Tiered storage can be enabled in Alluxio using
[configuration parameters]({{ '/en/basic/Configuration-Settings.html' | relativize_url }}).
To specify additional tiers for Alluxio, use the following configuration parameters:

```
alluxio.worker.tieredstore.levels
alluxio.worker.tieredstore.level{x}.alias
alluxio.worker.tieredstore.level{x}.dirs.quota
alluxio.worker.tieredstore.level{x}.dirs.path
alluxio.worker.tieredstore.level{x}.watermark.high.ratio
alluxio.worker.tieredstore.level{x}.watermark.low.ratio
```

For example, if you wanted to configure Alluxio to have two tiers, memory and hard disk drive,
you could use a configuration similar to:

```
# configure 2 tiers in Alluxio
alluxio.worker.tieredstore.levels=2
# the first (top) tier to be a memory tier
alluxio.worker.tieredstore.level0.alias=MEM
# defined `/mnt/ramdisk` to be the file path to the first tier
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk
# set the quota for the ramdisk to be `100GB`
alluxio.worker.tieredstore.level0.dirs.quota=100GB
# set the ratio of high watermark on top layer to be 90%
alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9
# set the ratio of low watermark on top layer to be 70%
alluxio.worker.tieredstore.level0.watermark.low.ratio=0.7
# configure the second tier to be a hard disk tier
alluxio.worker.tieredstore.level1.alias=HDD
# configured 3 separate file paths for the second tier
alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3
# define the quota for each of the 3 file paths of the second tier
alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB
# set the ratio of high watermark on the second layer to be 90%
alluxio.worker.tieredstore.level1.watermark.high.ratio=0.9
# set the ratio of low watermark on the second layer to be 70%
alluxio.worker.tieredstore.level1.watermark.low.ratio=0.7
```

There are no restrictions to how many tiers can be configured
but each tier must be identified with a unique alias.
A typical configuration will have three tiers for Memory, SSD, and HDD.
To use multiple hard drives in the HDD tier, specify multiple paths when configuring
`alluxio.worker.tieredstore.level{x}.dirs.path`.

## Evicting Stale Data

When Alluxio storage is full, Alluxio frees up space for new data as its storage is designed to be
volatile. Eviction is the mechanism that removes old data.

There are two modes of eviction in Alluxio: asynchronous and synchronous.

Asynchronous eviction is the default implementation of eviction. It relies on a periodic space
reserver thread in each worker to evict data. When the worker storage utilization reaches a
maximum threshold, it evicts data until the usage reaches a minimum threshold. The two thresholds
are configurable, labeled as the high and low watermarks respectively. In the case where we have
216 GB of storage on a worker, eviction can be triggered at about 200 GB to reduce storage
utilization to 160 GB by setting the high watermark to 90% and the low watermark to 75%.

```properties
alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9
alluxio.worker.tieredstore.level0.watermark.low.ratio=0.75
```

Asynchronous eviction is particularly useful for write or read-cache heavy workloads.

Synchronous eviction waits for a client to request more space than is currently available on the
worker and then kicks off the eviction process to free up enough space to serve that request. This
leads to many small eviction attempts, which is less efficient, but maximizes the utilization of
available Alluxio space. Synchronous eviction is enabled by configuring the setting:

```
alluxio.worker.tieredstore.reserver.enabled=false
```

In general, it is recommended to use the default asynchronous eviction.

### Eviction Policies

Alluxio uses evictors to decide which blocks to remove from Alluxio storage.
Users can specify different Alluxio evictors to better control the eviction process.

Out-of-the-box evictor implementations include:

- **LRUEvictor**: Evicts the least-recently-used blocks until the required space is freed.
**This is Alluxio's default evictor**.
- **GreedyEvictor**: Evicts arbitrary blocks until the required space is freed.
- **LRFUEvictor**: Evicts blocks based on least-recently-used and least-frequently-used with a
configurable weight.
    - If the weight is completely biased toward least-recently-used, the behavior will be the same
    as the LRUEvictor.
    - The applicable configuration properties are `alluxio.worker.evictor.lrfu.step.factor` and
    `alluxio.worker.evictor.lrfu.attenuation.factor` which can be found in
    [configuration properties]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.worker.evictor.lrfu.step.factor)
- **PartialLRUEvictor** : Evicts based on a least-recently-used policy but will choose one configured
storage directory on the local worker with maximum free space and only evict from that directory.

The evictor utilized by workers is determined by the Alluxio property
[`alluxio.worker.evictor.class`]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.worker.evictor.class).
The property should specify the fully qualified class name within the configuration. Currently
available options are:

- `alluxio.worker.block.evictor.LRUEvictor`
- `alluxio.worker.block.evictor.GreedyEvictor`
- `alluxio.worker.block.evictor.LRFUEvictor`
- `alluxio.worker.block.evictor.PartialLRUEvictor`

When using synchronous eviction, it is recommended to use smaller block sizes (around 64-128MB),
to reduce the latency of block eviction. The block size has no effect for asynchronous eviction.

Alluxio also supports custom evictors for more fine-grained control over the eviction behavior.
Evictors must implement the
[Evictor interface](https://github.com/Alluxio/alluxio/blob/master/core/server/worker/src/main/java/alluxio/worker/block/evictor/Evictor.java).
Sample evictor implementations can be found under the `alluxio.worker.block.evictor` package.
For Alluxio to use the custom evictor, the fully qualified class name must be specified in the
`alluxio.worker.evictor.class` property. After compiling the class to a JAR file, the JAR file
needs to be accessible and added to the Alluxio worker's java classpath.

## Managing Data in Alluxio

Users should understanding the following concepts to properly utilize available resources:

- **free**: Freeing data means removing data from the Alluxio cache, but not the
underlying UFS. Data is still available to users after a free operation, but performance may be
degraded for clients who attempt to access a file after it has been freed from Alluxio.
- **load**: Loading data means copying it from a UFS into Alluxio cache. If Alluxio is using
memory-based storage, users will likely see I/O performance improvements after loading.
- **persist**: Persisting data means writing data which may or may not have
been modified within Alluxio storage back to a UFS. By writing data back to a UFS, data is
guaranteed to be recoverable if an Alluxio node goes down.
- **TTL (Time to Live)**: The TTL property sets an expiration duration on files and directories to
remove them from Alluxio space when the data exceeds its lifetime. It is also possible to configure
TTL to remove the corresponding data stored in a UFS.

### Freeing Data from Alluxio Storage

In order to manually free data in Alluxio, you can use the `./bin/alluxio` file system command
line interface.

```bash
$ ./bin/alluxio fs free ${PATH_TO_UNUSED_DATA}
```

This will remove the data at the given path from Alluxio storage. The data is still accessible if
it is persisted to a UFS. For more information refer to the
[command line interface documentation]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#free)

Note that a user typically should not need to manually free data from Alluxio as the
configured [eviction policy](#eviction-policies) will take care of removing unused or old data.

### Loading Data into Alluxio Storage

If the data is already in a UFS, use
[`alluxio fs load`]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#load)

```bash
$ ./bin/alluxio fs load ${PATH_TO_FILE}
```

To load data from the local file system, use the
[command `copyFromLocal`]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#copyfromlocal).
This will only load the file into Alluxio storage, but may not persist the data to a UFS.
Setting the write type to `MUST_CACHE` write type will _not_ persist data to a UFS,
whereas `CACHE` and `CACHE_THROUGH` will. Manually loading data is not recommended as Alluxio
will automatically load data into the Alluxio cache when a file is used for the first time.

### Persisting Data in Alluxio

[The `alluxio fs persist`]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#persist)
command allows a user to push data from the Alluxio cache to a UFS.

```bash
$ ./bin/alluxio fs persist ${PATH_TO_FILE}
```

This command is useful if you have data which you loaded into Alluxio which did not originate from
a configured UFS. Users should not need to worry about manually persisting data in most cases.

### Setting Time to Live (TTL)

Alluxio supports a `Time to Live (TTL)` setting on each file and directory in its namespace. This
feature can be used to effectively manage the Alluxio cache, especially in environments with strict
guarantees on the data access patterns. For example, if analytics is run on the last week of
ingested data, the TTL feature can be used to explicitly flush old data to free the cache for new
files.

Alluxio has TTL attributes associated with each file or directory. These attributes are saved as
part of the journal and persist across cluster restarts. The active master node is responsible for
holding the metadata in memory when Alluxio is serving. Internally, the master runs a background
thread which periodically checks if files have reached their TTL expiration.

Note that the background thread runs on a configurable period, which is set to an hour by default.
Data that has reached its TTL expiration immediately after a check will not be removed until the
next check interval an hour later.

To set the interval to 10 minutes, add the following to `alluxio-site.properties`:

```
alluxio.master.ttl.checker.interval=10m
```

Refer to the [configuration page]({{ '/en/basic/Configuration-Settings.html' | relativize_url }})
for more details on setting Alluxio configurations.

#### APIs

There are two ways to set the TTL attributes of a path.

1. Through the Alluxio shell command line
1. Passively on each load metadata or create file

The TTL API is as follows:

```
SetTTL(path, duration, action)
`path`          the path in the Alluxio namespace
`duration`      the number of milliseconds before the TTL action goes into effect, this overrides
                any previous value
`action`        the action to take when duration has elapsed. `FREE` will cause the file to be
                evicted from Alluxio storage, regardless of the pin status. `DELETE` will cause the
                file to be deleted from the Alluxio namespace and under store.
                NOTE: `DELETE` is the default for certain commands and will cause the file to be
                permanently removed.
```

#### Command Line Usage

See the detailed
[command line documentation]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#setttl)
to see how to use the `setTtl` command within the Alluxio shell to modify TTL attribute.

## Checking Alluxio Cache Capacity and Usage

The Alluxio shell command `fsadmin report` provides a short summary of space availability,
along with other useful information. A sample output is shown below:

```bash
$ ./bin/alluxio fsadmin report
Alluxio cluster summary:
    Master Address: localhost/127.0.0.1:19998
    Web Port: 19999
    Rpc Port: 19998
    Started: 09-28-2018 12:52:09:486
    Uptime: 0 day(s), 0 hour(s), 0 minute(s), and 26 second(s)
    Version: 1.8.0
    Safe Mode: true
    Zookeeper Enabled: false
    Live Workers: 1
    Lost Workers: 0
    Total Capacity: 10.67GB
        Tier: MEM  Size: 10.67GB
    Used Capacity: 0B
        Tier: MEM  Size: 0B
    Free Capacity: 10.67GB
```

The Alluxio shell also allows a user to check how much space is available and in use within
the Alluxio cache.

To get the total used bytes in the Alluxio cache:

```bash
$ ./bin/alluxio fs getUsedBytes
```

To get the total capacity of the Alluxio cache in bytes:

```bash
$ ./bin/alluxio fs getCapacityBytes
```

The Alluxio master web interface gives the user a visual overview of the cluster and how much
storage space is used. It can be found at `http:/{MASTER_IP}:${alluxio.master.web.port}/`.
More detailed information about the Alluxio web interface can be
[found in our documentation]({{ '/en/basic/Web-Interface.html' | relativize_url }}).
