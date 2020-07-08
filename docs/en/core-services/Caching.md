---
layout: global
title: Caching
nickname: Caching
group: Core Services
priority: 1
---

* Table of Contents
{:toc}

## Alluxio Storage Overview

The purpose of this documentation is to introduce users to the concepts behind Alluxio storage and
the operations that can be performed within Alluxio storage space. For metadata related operations
such as syncing and namespaces, refer to the
[page on namespace management]({{ '/en/core-services/Unified-Namespace.html' | relativize_url }})

Alluxio helps unify users' data across a variety of platforms while also helping to increase
overall I/O throughput from a user's perspective. Alluxio accomplishes this by splitting storage
into two distinct categories.

- **UFS (Under File Storage, also referred to as the under storage)**
    - This storage is represented as space which is not managed by Alluxio.
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
storage optimizations similar to L1/L2 CPU caches.

## Configuring Alluxio Storage

### Single-Tier Storage

The easiest way to configure Alluxio storage is to use the default single-tier mode.

Note that this page refers to local storage and terms like `mount` refer to mounting in the local
filesystem, not to be confused with Alluxio's `mount` concept for external under storages.

On startup, Alluxio will provision a ramdisk on every worker and take a percentage of the
system's total memory. This ramdisk will be used as the only storage medium allocated to each
Alluxio worker.

Alluxio storage is configured through Alluxio's configuration in `alluxio-site.properties`. See
[configuration settings]({{ '/en/operation/Configuration.html' | relativize_url }}) for detailed
information.

A common modification to the default is to set the ramdisk size explicitly. For example, to set the
ramdisk size to be 16GB on each worker:

```properties
alluxio.worker.memory.size=16GB
```

Another common change is to specify multiple storage media, such as ramdisk and SSDs. We will need
to update `alluxio.worker.tieredstore.level0.dirs.path` to take specify each storage medium we want
to use as a storage directory. For example, to use the ramdisk (mounted at `/mnt/ramdisk`) and two
SSDs (mounted at `/mnt/ssd1` and `/mnt/ssd2`):

```properties
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk,/mnt/ssd1,/mnt/ssd2
alluxio.worker.tieredstore.level0.dirs.mediumtype=MEM,SSD,SSD
```

Note that the ordering of the medium types must match with the ordering of the paths.
Here, MEM and SSD are two preconfigured types in Alluxio.
`alluxio.master.tieredstore.global.mediumtype` is a configuration parameter that has a list of
all available medium types and by default it is set to `MEM, SSD, HDD`. This list can be modified
if the user has additional storage media types.

The paths provided should point to paths in the local filesystem mounting the appropriate storage
media. To enable short circuit operations, the permissions of these paths should be permissive for the
client user to read, write, and execute on the path. For example, `770` permissions are needed for
the client user who is among the same group of the user that starts the Alluxio service.

After updating the storage media, we need to indicate how much storage is allocated for each storage
directory. For example, if we wanted to use 16 GB on the ramdisk and 100 GB on each SSD:

```properties
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
In certain environments, workloads will benefit from the explicit ordering of storage media based on
I/O speed. Alluxio assumes that tiers are ordered from top to bottom based on I/O performance.
For example, users often specify the following tiers:

 * MEM (Memory)
 * SSD (Solid State Drives)
 * HDD (Hard Disk Drives)

#### Writing Data

When a user writes a new block, it is written to the top tier by default. If the top tier does not have enough free space,
then the next tier is tried. If no space is found on all tiers, Alluxio frees up space for new data as its storage is designed to be volatile.
Eviction will start attempting to evict blocks from the worker, based on the block annotation policy. [block annotation policies](#block-annotation-policies).
If eviction cannot free up new space, then the write will fail.

**Note:** The new eviction model is synchronous and is executed on behalf of the client that requires a free space for the block it is writing.
This synchronized mode is not expected to incur performance hit as ordered list of blocks are always available with the help of block annotation policies.
However, `alluxio.worker.tieredstore.free.ahead.bytes`(Default: 0) can be configured to free up more bytes than necessary per eviction attempt.

The user can also specify the tier that the data will be written to via
[configuration settings](#configuring-tiered-storage).

#### Reading Data

If the data is already in Alluxio, the client will simply read the block from where it is already stored.
If Alluxio is configured with multiple tiers, the block may not be necessarily read from the top tier,
since it could have been moved to a lower tier transparently.

Reading data with the `ReadType.CACHE_PROMOTE` will attempt to first transfer the block to the
top tier before it is read from the worker. This can also be used as a data management strategy by
explicitly moving hot data to higher tiers.

#### Configuring Tiered Storage

Tiered storage can be enabled in Alluxio using
[configuration parameters]({{ '/en/operation/Configuration.html' | relativize_url }}).
To specify additional tiers for Alluxio, use the following configuration parameters:

```properties
alluxio.worker.tieredstore.levels
alluxio.worker.tieredstore.level{x}.alias
alluxio.worker.tieredstore.level{x}.dirs.quota
alluxio.worker.tieredstore.level{x}.dirs.path
alluxio.worker.tieredstore.level{x}.dirs.mediumtype
```

For example, if you wanted to configure Alluxio to have two tiers, memory and hard disk drive,
you could use a configuration similar to:

```properties
# configure 2 tiers in Alluxio
alluxio.worker.tieredstore.levels=2
# the first (top) tier to be a memory tier
alluxio.worker.tieredstore.level0.alias=MEM
# defined `/mnt/ramdisk` to be the file path to the first tier
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk
# defined MEM to be the medium type of the ramdisk directory
alluxio.worker.tieredstore.level0.dirs.mediumtype=MEM
# set the quota for the ramdisk to be `100GB`
alluxio.worker.tieredstore.level0.dirs.quota=100GB
# configure the second tier to be a hard disk tier
alluxio.worker.tieredstore.level1.alias=HDD
# configured 3 separate file paths for the second tier
alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3
# defined HDD to be the medium type of the second tier
alluxio.worker.tieredstore.level1.dirs.mediumtype=HDD,HDD,HDD
# define the quota for each of the 3 file paths of the second tier
alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB
```

There are no restrictions on how many tiers can be configured
but each tier must be identified with a unique alias.
A typical configuration will have three tiers for Memory, SSD, and HDD.
To use multiple hard drives in the HDD tier, specify multiple paths when configuring
`alluxio.worker.tieredstore.level{x}.dirs.path`.

### Block Annotation Policies

Alluxio uses block annotation policies, starting v2.3, to maintain strict ordering of blocks in storage. 
Annotation policy defines an order for blocks across tiers and is consulted during:
- Eviction
- [Dynamic Block Placement](#block-aligning-dynamic-block-placement).

The eviction, that happens in-line with writes, will attempt to remove blocks based on the order enforced by the block annotation policy.
The last block in annotated order is the first candidate for eviction regardless of which tier it's sitting on.

Out-of-the-box annotation implementations include:

- **LRUAnnotator**: Annotates the blocks based on least-recently-used order.
**This is Alluxio's default annotator**.
- **LRFUAnnotator**: Annotates the blocks based on least-recently-used and least-frequently-used orders with a
configurable weight.
    - If the weight is completely biased toward least-recently-used, the behavior will be the same
    as the LRUAnnotator.
    - The applicable configuration properties are `alluxio.worker.block.annotator.lrfu.step.factor` and
    `alluxio.worker.block.annotator.lrfu.attenuation.factor`.

The annotator utilized by workers is determined by the Alluxio property
[`alluxio.worker.block.annotator.class`]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.worker.block.annotator.class).
The property should specify the fully qualified class name within the configuration. Currently
available options are:

- `alluxio.worker.block.annotator.LRUAnnotator`
- `alluxio.worker.block.annotator.LRFUAnnotator`

#### Evictor Emulation
The old eviction policies are now removed and Alluxio provided implementations are replaced with appropriate annotation policies.
Configuring old Alluxio evictors will cause worker startup failure with `java.lang.ClassNotFoundException`.
Also, the old watermark based configuration is invalidated. So below configuration options are ineffective:
- `alluxio.worker.tieredstore.levelX.watermark.low.ratio`
- `alluxio.worker.tieredstore.levelX.watermark.high.ratio`

However, Alluxio supports emulation mode which annotates blocks based on custom evictor implementation. The emulation assumes the configured eviction policy creates
an eviction plan based on some kind of order and works by regularly extracting this order to be used in annotation activities.

The old evictor configurations should be changes as below. (Failing to change the lef-over configuration will cause class load exceptions as old evictor implementations are deleted.)
- LRUEvictor -> LRUAnnotator
- GreedyEvictor -> LRUAnnotator
- PartialLRUEvictor -> LRUAnnotator
- LRFUEvictor -> LRFUAnnotator

### Tiered Storage Management
As block allocation/eviction no longer enforces a particular tier for new writes, a new block could end up in any configured tier.
This allows writing data bigger than Alluxio storage capacity. However, it also requires Alluxio to dynamically manage block placement.
To enforce the assumption that tiers are configured from fastest to slowest, Alluxio now moves blocks around tiers based on block annotation policies.

Below configuration is honoured by each individual tier management task:
- `alluxio.worker.management.task.thread.count`: How many threads to use for management tasks. (Default:`CPU core count`)
- `alluxio.worker.management.block.transfer.concurrency.limit`: How many block transfers can execute concurrently. (Default:`CPU core count`/ 2)

#### Block Aligning (Dynamic Block Placement)
Alluxio will dynamically move blocks across tiers in order to have block composition that is in line with the configured block annotation policy.

To compensate this, Alluxio watches the I/O pattern and reorganize blocks across tiers to make sure
**the lowest block of a higher tier has higher order than the highest block of a tier below**.

This is achieved by `align` management task. This task, upon detecting tiers are out of order, swaps blocks among tiers
in order to eliminate disorder among blocks and effectively align the tiers to configured annotation policy.
See [Management Task Back-Off](#management-task-back-off) section for how to control the effect of these new background tasks to user I/O.

To control tier aligning:
- `alluxio.worker.management.tier.align.enabled`: Whether tier aligning task is enabled. (Default:`true`)
- `alluxio.worker.management.tier.align.range`: How many blocks to align in a single task run. (Default:`100`)
- `alluxio.worker.management.tier.align.reserved.bytes`: The amount of space to reserve on all directories by default when multi-tier is configured. (Default:`1GB`)
This is used for internal block movements.
- `alluxio.worker.management.tier.swap.restore.enabled`: This controls a special task that is used to unblock tier alignment when internal reserved space was exhausted. (Default:`true`)
Reserved space can get exhausted due to fact that Alluxio supports variable block sizes, so swapping blocks between tiers during aligning can decrease reserved space on a directory when block sizes don't match.

#### Block Promotions
When a higher tier has free space, blocks from a lower tier is moved up in order to better utilize faster disks as it's assumed the higher tiers are configured with faster disks.

To control dynamic tier promotion:
- `alluxio.worker.management.tier.promote.enabled` : Whether tier promition task is enabled. (Default:`true`)
- `alluxio.worker.management.tier.promote.range`: How many blocks to promote in a single task run. (Default:`100`)
- `alluxio.worker.management.tier.promote.quota.percent`: The max percentage of each tier that could be used for promotions.
Promotions will be stopped to a tier once its used space go over this value. (0 means never promote, and, 100 means always promote.)

#### Management Task Back-Off
Tier management tasks (align/promote) are respectful to user I/O and backs off whenever worker/disk is under load.
This behaviour is to make sure internal management doesn't have negative effect on user I/O performance.

There are two back-off types available, `ANY` and `DIRECTORY`, that can be set in `alluxio.worker.management.backoff.strategy` property.

- `ANY`; management tasks will backoff from worker when there is any user I/O. 
This mode will ensure low management task overhead in order to favor immediate user I/O performance.
However, making progress on management tasks will require quite periods on the worker.

- `DIRECTORY`; management tasks will backoff from directories with ongoing user I/O.
This mode will give better chance of making progress on management tasks
However, immediate user I/O throughput might be decreased due to increased management task activity.

An additional property that effects both back-off strategies is `alluxio.worker.management.load.detection.cool.down.time` that controls
for how long a user I/O will be counted as a load on target directory/worker.


## Managing the Data Lifecycle in Alluxio

Users should understand the following concepts to utilize available resources properly:

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

```console
$ ./bin/alluxio fs free ${PATH_TO_UNUSED_DATA}
```

This will remove the data at the given path from Alluxio storage. The data is still accessible if
it is persisted to a UFS. For more information refer to the
[command line interface documentation]({{ '/en/operation/User-CLI.html' | relativize_url }}#free)

Note that a user typically should not need to manually free data from Alluxio as the
configured [annotation policy](#block-annotation-policies) will take care of removing unused or old data.

### Loading Data into Alluxio Storage

If the data is already in a UFS, use
[`alluxio fs load`]({{ '/en/operation/User-CLI.html' | relativize_url }}#load)

```console
$ ./bin/alluxio fs load ${PATH_TO_FILE}
```

To load data from the local file system, use the command
[`alluxio fs copyFromLocal`]({{ '/en/operation/User-CLI.html' | relativize_url
}}#copyfromlocal).
This will only load the file into Alluxio storage, but may not persist the data to a UFS.
Setting the write type to `MUST_CACHE` write type will _not_ persist data to a UFS,
whereas `CACHE` and `CACHE_THROUGH` will. Manually loading data is not recommended as Alluxio
will automatically load data into the Alluxio cache when a file is used for the first time.

### Persisting Data in Alluxio

The command [`alluxio fs persist`]({{ '/en/operation/User-CLI.html' | relativize_url
}}#persist)
allows a user to push data from the Alluxio cache to a UFS.

```console
$ ./bin/alluxio fs persist ${PATH_TO_FILE}
```

This command is useful if you have data that you loaded into Alluxio which did not originate from
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
thread that periodically checks if files have reached their TTL expiration.

Note that the background thread runs on a configurable period, which is set to an hour by default.
Data that has reached its TTL expiration immediately after a check will not be removed until the
next check interval an hour later.

To set the interval to 10 minutes, add the following to `alluxio-site.properties`:

```properties
alluxio.master.ttl.checker.interval=10m
```

Refer to the [configuration page]({{ '/en/operation/Configuration.html' | relativize_url }})
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
`action`        the action to take when the duration has elapsed. `FREE` will cause the file to be
                evicted from Alluxio storage, regardless of the pin status. `DELETE` will cause the
                file to be deleted from the Alluxio namespace and under store.
                NOTE: `DELETE` is the default for certain commands and will cause the file to be
                permanently removed.
```

#### Command Line Usage

See the detailed
[command line documentation]({{ '/en/operation/User-CLI.html' | relativize_url }}#setttl)
to see how to use the `setTtl` command within the Alluxio shell to modify TTL attribute.

#### Passive TTL on files in Alluxio

The Alluxio client can be configured to add TTL attributes whenever it adds a new file to the
Alluxio namespace. Passive TTL is useful in cases where files accessed by the user are expected to
be temporarily used, but it is not flexible because all requests from the same client will inherit
the same TTL attributes.

Passive TTL works with the following configuration options:

* `alluxio.user.file.create.ttl` - The TTL duration to set on files in Alluxio.
By default, no TTL duration is set.
* `alluxio.user.file.create.ttl.action` - The TTL action to set on files
in Alluxio. By default, this action is `DELETE`.

TTL is disabled by default and should only be enabled by clients which have strict data
access patterns.

For example, to delete the files created by the `runTests` after 3 minutes:

```console
$ ./bin/alluxio runTests -Dalluxio.user.file.create.ttl=3m \
  -Dalluxio.user.file.create.ttl.action=DELETE
```

For this example, ensure the `alluxio.master.ttl.checker.interval` is set to a short
duration, such as a minute, in order for the the master to quickly identify the expired files.

## Managing Data Replication in Alluxio

### Passive Replication

Like many distributed file systems, each file in Alluxio consists of one or multiple blocks stored
across the cluster. By default, Alluxio may adjust the replication level of different blocks
automatically based on the workload and storage capacity. For example, Alluxio may
create more replicas of a particular block when more clients request to read this block with read
type `CACHE` or `CACHE_PROMOTE`; Alluxio may also remove existing replicas when they are used less
often to reclaim the space for data that is accessed more often ([Block Annotation Policies](#block-annotation-policies)).
It is possible that in the same file different blocks have a different number
of replicas according to the popularity.

By default, this replication or eviction decision and the corresponding data transfer is completely
transparent to users and applications accessing data stored in Alluxio.

### Active Replication

In addition to the dynamic replication adjustment, Alluxio also provides APIs and command-line
interfaces for users to explicitly maintain a target range of replication levels for a file explicitly.
Particularly, a user can configure the following two properties for a file in Alluxio:

1. `alluxio.user.file.replication.min` is the minimum possible number of replicas of this file. Its
default value is 0, so in the default case Alluxio may completely evict this file from Alluxio
managed space after the file becomes cold. By setting this property to a positive integer, Alluxio
will check the replication levels of all the blocks in this file periodically. When some blocks
become under-replicated, Alluxio will not evict any of these blocks and actively create more
replicas to restore the replication level.
1. `alluxio.user.file.replication.max` is the maximum number of replicas. Once the property of this
file is set to a positive integer, Alluxio will check the replication level and remove the excessive
replicas. Set this property to -1 to make no upper limit (the default case), and to 0 to prevent
storing any data of this file in Alluxio. Note that, the value of `alluxio.user.file.replication.max`
must be no less than `alluxio.user.file.replication.min`.

For example, users can copy a local file `/path/to/file` to Alluxio with at least two replicas initially:

```console
$ ./bin/alluxio fs -Dalluxio.user.file.replication.min=2 \
copyFromLocal /path/to/file /file
```

Next, set the replication level range of `/file` between 3 and 5. Note that, this command will
return right after setting the new replication level range in a background process and achieving
the target asynchronously.

```console
$ ./bin/alluxio fs setReplication --min 3 --max 5 /file
```

Set the `alluxio.user.file.replication.max` to unlimited.

```console
$ ./bin/alluxio fs setReplication --max -1 /file
```

Recursirvely set replication level of all files inside a directory `/dir` (including its
sub-directories) using `-R`:

```console
$ ./bin/alluxio fs setReplication --min 3 --max -5 -R /dir
```

To check the target replication level of a file, run

```console
$ ./bin/alluxio fs stat /foo
```
and look for the `replicationMin` and `replicationMax` fields in the output.

## Checking Alluxio Cache Capacity and Usage

The Alluxio shell command `fsadmin report` provides a short summary of space availability,
along with other useful information. A sample output is shown below:

```console
$ ./bin/alluxio fsadmin report
Alluxio cluster summary:
    Master Address: localhost/127.0.0.1:19998
    Web Port: 19999
    Rpc Port: 19998
    Started: 09-28-2018 12:52:09:486
    Uptime: 0 day(s), 0 hour(s), 0 minute(s), and 26 second(s)
    Version: 2.0.0
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

```console
$ ./bin/alluxio fs getUsedBytes
```

To get the total capacity of the Alluxio cache in bytes:

```console
$ ./bin/alluxio fs getCapacityBytes
```

The Alluxio master web interface gives the user a visual overview of the cluster and how much
storage space is used. It can be found at `http:/{MASTER_IP}:${alluxio.master.web.port}/`.
More detailed information about the Alluxio web interface can be
[found in our documentation]({{ '/en/operation/Web-Interface.html' | relativize_url }}).
