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
[page on namespace management]({{site.baseurl}}{% link en/advanced/Namespace-Management.md %})

Alluxio helps unify users' data across a variety of platforms while also helping to increase
overall I/O throughput from a user's perspective. Alluxio accomplishes this by splitting storage
into two distinct categories.

- **UFS (Under File Storage, also sometimes called under storage)**
    - This storage is represented as space which is not managed by Alluxio. UFS storage may come
    from an external system such as HDFS, S3, or any other type of filesystem. Alluxio
    may connect to one or more of these UFSs and expose them within a single namespace.
    - Typically, UFS storage is aimed at storing large amounts of data persistently for extended
    periods of time.
- **Alluxio storage**
    - Alluxio manages the local storage, including memory, of Alluxio workers to act as a
    distributed buffer cache. This fast data layer between user applications and the various under
    storages results in vastly improved I/O performance.
    - Alluxio storage is mainly aimed at storing hot, transient data and is not focused on long-term
    persistence.
    - The amount and type of storage for each Alluxio node to manage is determined by user
    configuration.
    - Even if data is not currently within Alluxio storage, files within a connected UFS are still
    visible to Alluxio clients. When a client attempts to read a file which is only in a UFS
    the data will be copied into Alluxio storage.


![Alluxio storage diagram]({{site.baseurl}}{% link img/stack.png %})

One of the reasons that Alluxio storage improves performance is that it stores data in memory
co-located with compute nodes. Another reason is that Alluxio makes "hot" data more available to
nodes by creating more replicas of data within Alluxio storage so more I/O operations may occur in
parallel.

Replicas of data within Alluxio are independent of the replicas which may exist within a UFS. The
number of data replicas within Alluxio storage is determined dynamically by cluster activity. It is
also possible to have zero copies of a file within Alluxio, but still have access to it. Due to the
fact that Alluxio relies on UFSs for a majority of data storage, Alluxio doesn't need to always
keep copies of data that aren't currently being used.

Alluxio also supports tiered storage, which makes the system storage media aware, enabling data
storage optimizations similar to L1/L2 cpu caches.

## Configure Alluxio Storage

### Single-Tier Storage

The easiest way to configure Alluxio storage is to use the default single-tier mode.

Note that this doc refers to local storage and terms like `mount` refer to mounting in the local
filesystem, not to be confused with Alluxio's `mount` concept for under storages.

Out-of-the-box Alluxio will provision a ramdisk on every worker and take a percentage of the
system's total memory. This ramdisk will be used as the only storage medium allocated to each
Alluxio worker.

Alluxio storage is configured through Alluxio's configuration in `alluxio-site.properties`. See the
[configuration docs]({{site.baseurl}}{% link en/basic/Configuration-Settings.md %}) for detailed
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

### Multiple-Tier Storage

For typical deployments, it is recommended to use a single storage tier with heterogeneous storage
media. However, in certain environments, workloads will benefit from explicit ordering of storage
media based on I/O speed. In this case, tiered storage should be used. When tiered storage is
enabled, the eviction process intelligently accounts for the tier concept. Alluxio assumes that
tiers are ordered from top to bottom based on I/O performance. For example, users often specify the
following tiers:

 * MEM (Memory)
 * SSD (Solid State Drives)
 * HDD (Hard Disk Drives)

#### Writing Data

When a user writes a new block, it is written to the top tier by default. If eviction cannot free up
space in the top tier, the write will fail. If the file size exceeds the size of the top tier, the
write will also fail.

The user can also specify the tier that the data will be written to via
[configuration settings](#configure-tiered-storage).

Reading data with the ReadType.CACHE or ReadType.CACHE_PROMOTE will also result in the data being
written into Alluxio. In this case, the data is always written to the top tier.

Finally, data in written into Alluxio via the load command. In this case also, the data is always
written to the top tier.

#### Reading Data

Reading a data block with tiered storage is similar to standard Alluxio. If the data is already in
Alluxio, the client will simply read the block from where it is already stored. If Alluxio is
configured with multiple tiers, then the block will not be necessarily read from the top tier, since
it could have been moved to a lower tier transparently.

Reading data with the ReadType.CACHE_PROMOTE will ensure the data is first transferred to the
top tier before it is read from the worker. This can also be used as a data management strategy by
explicitly moving hot data to higher tiers.

#### Configure Tiered Storage

Tiered storage can be enabled in Alluxio using
[configuration parameters]({{site.baseurl}}{% link en/basic/Configuration-Settings.md %}). To
specify additional tiers for Alluxio, use the following configuration parameters:

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

There are a few restrictions to defining the tiers. There is no restriction on the number of tiers,
however, a common configuration has 3 tiers - Memory, HDD and SSD. At most 1 tier can refer to a
specific alias. For example, at most 1 tier can have the alias HDD. If you want Alluxio to use
multiple hard drives for the HDD tier, you can configure that by using multiple paths for
`alluxio.worker.tieredstore.level{x}.dirs.path`.

## Evict Stale Data

Because Alluxio storage is designed to be volatile, there must be a mechanism to make space for new
data when Alluxio storage is full. This is termed eviction.

There are two modes of eviction in Alluxio, asynchronous (default) and synchronous. You can switch
between the two by enabling and disabling the space reserver which handles asynchronous eviction.
For example, to turn off asynchronous eviction:

```
alluxio.worker.tieredstore.reserver.enabled=false
```

Asynchronous eviction is the default implementation of eviction. It relies on a periodic space
reserver thread in each worker to evict data. It waits until the worker storage utilization reaches
a configurable high watermark. Then it evicts data based on the eviction policy until it reaches the
configurable low watermark. For example, if we had the same 16+100+100=216GB storage configured,
we can set eviction to kick in at around 200GB and stop at around 160GB:

```properties
alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9 # 216GB * 0.9 ~ 200GB
alluxio.worker.tieredstore.level0.watermark.low.ratio=0.75 # 216GB * 0.75 ~ 160GB
```

In write or read-cache heavy workloads, asynchronous eviction can improve performance.

Synchronous eviction waits for a client to request more space than is currently available on the
worker and then kicks off the eviction process to free up enough space to serve that request. This
leads to many small eviction attempts, which is less efficient but maximizes the utilization of
available Alluxio space. In general, we recommend to use asynchronous eviction.

### Eviction Policies

Alluxio uses evictors for deciding which blocks to remove from Alluxio storage, when space needs to
be freed. Users can specify the Alluxio evictor to achieve fine grained control over the eviction
process.

Out-of-the-box evictor implementations include:

- **GreedyEvictor**: Evicts arbitrary blocks until the required space is freed.
- **LRUEvictor**: Evicts the least-recently-used blocks until the required space is freed.
**This is Alluxio's default evictor**
- **LRFUEvictor**: Evicts blocks based on least-recently-used and least-frequently-used with a
configurable weight.
    - If the weight is completely biased toward least-recently-used, the behavior will be the same
    as the LRUEvictor.
    - The applicable configuration properties are `alluxio.worker.evictor.lrfu.step.factor` and
    `alluxio.worker.evictor.lrfu.attenuation.factor.` which can be found in
    [configuration properties]({{site.baseurl}}{% link en/reference/Properties-List.md %}#alluxio.worker.evictor.lrfu.step.factor)
- **PartialLRUEvictor** : Evicts based on least-recently-used but will choose StorageDir with
maximum free space and only evict from that StorageDir.

The evictor utilized by workers is determined by the Alluxio property
[`alluxio.worker.evictor.class`]({{site.basurl}}{% link en/reference/Properties-List.md %}#alluxio.worker.evictor.class).
The property should specify the fully qualified class name within the configuration. Currently
available options are:

- `alluxio.worker.block.evictor.GreedyEvictor`
- `alluxio.worker.block.evictor.LRUEvictor`
- `alluxio.worker.block.evictor.LRFUEvictor`
- `alluxio.worker.block.evictor.PartialLRUEvictor`

In the future, additional evictors may be available.

When using synchronous eviction, it is recommended to use smaller block sizes (around 64-128MB),
to reduce the latency of block eviction. When using the space reserver, block size does not affect
eviction latency.

Alluxio also supports custom evictors so that you can have even finer-grained control over Alluxio
eviction behavior. You can create your own custom evictor by implementing the
[Evictor interface](https://github.com/Alluxio/alluxio/blob/master/core/server/worker/src/main/java/alluxio/worker/block/evictor/Evictor.java).
If you want to see sample evictor implementations, you can find them under
`alluxio.worker.block.evictor` package. In order to specify your own custom evictor in the
`alluxio.worker.evictor.class` property you must compile your evictor implementation to a JAR
file, and then add the JAR file to the java classpath when starting the Alluxio workers.


## Manage Data in Alluxio

Within an Alluxio cluster there are concepts related to storage which must be understood in order
to properly utilize the available resources. Users should understanding the following:

- **free**: Freeing data in Alluxio means removing data from the Alluxio cache, but not the
underlying UFS. Data is still available to users after a free, but performance may be degraded
for those who attempt to access a file after it has been freed from Alluxio.
- **load**: Loading data into Alluxio means copying it from a UFS into Alluxio cache. If
Alluxio is using memory-based storage, users will likely see I/O performance improvements after
loading data into Alluxio.
- **persist**: In the context of Alluxio persisting means writing data which may or may not have
been modified within Alluxio storage back to a UFS. By writing data back to a UFS data is
guaranteed not to be lost if an Alluxio node goes down.
- **TTL (Time to Live)**: Files and directories within Alluxio space have a TTL property which can
be set in order to effectively manage Alluxio space. By setting a TTL Alluxio remove old data from
the Alluxio cache after a period of time in order to make room for new data. UFS data can also be
managed by setting the `TTLAction` to "_delete_".

### Freeing Data from Alluxio Storage

In order to manually free data in Alluxio, you can use the `./bin/alluxio` filesystem command
line interface.

```bash
$ ./bin/alluxio fs free ${PATH_TO_UNUSED_DATA}
```

This will remove data from Alluxio storage. but if the data is persisted to a UFS, it will still be
accessible. For more information refer to the
[command line interface documentation]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#free)

It should be noted that a user typically should not need to manually free data from Alluxio as the
configured [eviction policy](#eviction-policies) will take care of removing unused or old data.

### Loading Data into Alluxio Storage

There are a couple ways that a user may move data into Alluxio storage. If the data is already in a
UFS, then the user may use
[`alluxio fs load`]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#load)

```bash
$ ./bin/alluxio fs load ${PATH_TO_FILE}
```

If there is data within the local filesystem, the
[command `copyFromLocal`]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#copyfromlocal)
which can be used to load data into Alluxio storage, but will not persist the data to a UFS.
However, setting the write type will also control whether or not data will be written to the UFS or
not. The `MUST_CACHE` write type will _not_ persist data to a UFS while `CACHE` and `CACHE_THROUGH`
will. Manually loading data is not recommended as Alluxio will automatically load data into the
Alluxio cache when a file is used even once.

### Persisting Data in Alluxio

[The `alluxio fs persist`]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#persist)
command allows a user to push data from the Alluxio cache to a UFS.

```bash
$ ./bin/alluxio fs persist ${PATH_TO_FILE}
```

This command is useful if you have data which you loaded into Alluxio which didn't come from a
configured UFS. Most of the time users should not need to worry about manually persisting data.

### Setting Time to Live (TTL)

Alluxio supports a `Time to Live (TTL)` setting on each file and directory in the namespace. This
feature can be used to effectively manage the Alluxio cache, especially in environments with strict
guarantees on the data access patterns. For example, if analytics is only done on the last week of
ingested data, TTL can be used to explicitly flush old data to free the cache for new
files.

Alluxio has TTL attributes associated with each file or directory. These attributes are journaled
and persist across cluster restarts. The active master node is responsible for holding the metadata
in memory when Alluxio is serving. Internally, the master runs a background thread which
periodically checks if files have reached their TTL.

Note that the background thread runs on a configurable period, by default 1 hour. This means a TTL
will not be enforced until the next check interval, and the enforcement of a TTL can be up to 1
TTL interval late. The interval length is set by the `alluxio.master.ttl.checker.interval`
property.

For example, to set the interval to 10 minutes, add the following to `alluxio-site.properties`:

```
alluxio.master.ttl.checker.interval=10m
```

Refer to the [configuration page]({{site.baseurl}}{% link en/basic/Configuration-Settings.md %})
for more details on setting Alluxio configurations.

While the master node enforces TTLs, it is up to the clients to set the appropriate TTLs.

#### APIs

There are three ways to set the TTL of a path.

1. Through the Alluxio shell command line.
1. Passively on each load metadata or create file.

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
[command line documentation]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#setttl)
to see how to use the `setTtl` command within the Alluxio shell to modify files' TTL.

#### Passively on load metadata or create file

Whenever a new file is added to the Alluxio namespace, the user has the option of passively adding
a TTL to that file. This is useful in cases where files accessed by the user are expected to be
temporarily used. Instead of calling the API many times, it is automatically set on file discovery.

Note: passive TTL is more convenient but also less flexible. The options are client level, so all
TTL requests from the client will have the same action and duration.

Passive TTL works with the following configuration options:

* `alluxio.user.file.load.ttl` - the default duration to give any file newly loaded into Alluxio
from an under store. By default this is no ttl.
* `alluxio.user.file.load.ttl.action` - the default action for any ttl set on a file newly loaded
into Alluxio from an under store. By default this is `DELETE`.
* `alluxio.user.file.create.ttl` - the default duration to give any file newly created in Alluxio.
By default this is no ttl.
* `alluxio.user.file.create.ttl.action` - the default action for any ttl set on a file newly created
in Alluxio. By default this is `DELETE`.

There are two pairs of options, one for `load` and one for `create`. `Load` refers to files which
are discovered by Alluxio from the under store. `Create` refers to new files or directories created
in Alluxio.

Both options are disabled by default and should only be enabled by clients which have strict data
access patterns.

For example, to delete the files created by the `runTests` after 1 minute:

```bash
$ bin/alluxio runTests -Dalluxio.user.file.create.ttl=1m -Dalluxio.user.file.create.ttl
.action=DELETE
```

Note, if you try this example, make sure the `alluxio.master.ttl.checker.interval` is set to a short
duration, ie. 1 minute.

## Checking Alluxio Cache Capacity and Usage

To get a report containing information about the used and available space within an Alluxio cluster
the Alluxio shell command `fsadmin report` provides a short summary of space availability along with
some other useful information. Sample output is shown below

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

The alluxio shell also exposes two commands which allow a user to check how much space is available
to the Alluxio cache as well as how much space is currently in use. The `fs getCapacityBytes` and
`fs getUsedBytes` commands show a user how many bytes of available and used storage are in the
Alluxio cache respectively.

To get the total used bytes:

```bash
$ ./bin/alluxio fs getUsedBytes
```

To get the total capacity in bytes:

```bash
$ ./bin/alluxio fs getCapacityBytes
```

To determine the percentage of used space you can do `100 * (getUsedBytes/getCapacityBytes)`.
Another option to view the storage capacity and usage of an Alluxio cluster is by using the web
interface which can be found at `http:/{MASTER_IP}:${alluxio.master.web.port}/`

The Alluxio master web interface gives the user a visual overview of the cluster and how much
storage space is used. More detailed information about the Alluxio web interface can be
[found in our documentation]({{site.baseurl}}{% link en/basic/Web-Interface.md %})
