---
layout: global
title: Scalability Tuning
nickname: Scalability Tuning
group: Administration
priority: 5
---

Alluxio is a scalable distributed file system designed to handle many workers within a single
cluster.
This page details methods to recommend Alluxio node resource sizes. The section
[Metrics to Monitor](#metrics-to-monitor) describes methods to monitor and estimate resources which
influence the system's hardware requirements. The sections
[Alluxio Master Configuration](#alluxio-master-configuration),
[Alluxio Worker Configuration](#alluxio-worker-configuration), and
[Alluxio Client Configuration](#alluxio-client-configuration) provide details on hardware and
configuration tuning for large scale deployments.

* Table of Contents
{:toc}

## Metrics To Monitor
This section discusses what metrics to monitor to understand the current cluster scale.
On how to best integrate the metrics monitoring to your architecture, please see
[Metrics System]({{ '/en/operation/Metrics-System.html' | relativize_url }}) for more information.

Other than the most important metrics mentioned below, Alluxio provides observability on
various other aspects like the cluster usage and throughput of each individual operation.
A full list can be found at 
[All Metric Keys]({{ '/en/reference/Metrics-List.html' | relativize_url }}).

### Number of Files in Alluxio

In this section "files" refers to regular files and directories.
The number of files in Alluxio can be monitored through the metric `Master.TotalPaths`.

The number of files in Alluxio impacts the following:
* Size of heap required by the master - Each file and its directory structure takes approximately 4KB. 
If RocksDB is used, most file metadata is stored off-heap, 
and the size of the heap impacts how many files’ metadata can be cached on the heap. See the
[RocksDB section]({{ '/en/operation/Metastore.html#rocksdb-metastore' | relativize_url }}) for more
information.
* Size of disk required for journal storage - At peak, there may be two snapshot of the journal during checkpointing. 
Thus, we need to reserve approximately 4KB (2x2KB) on the disk for each file. 
* Latency of journal replay - The journal replay, which is the majority of the cold startup time for
a master, takes time proportional to the number of files in the system.
* Latency of journal backup - The journal backup takes time proportional to the number of files in
the system. If delegated backups are used, the primary master will not be impacted during the entire
backup duration.
* Latency of recursive operations - Recursive operations such as `loadMetadata` and `delete` take
time proportional to the number of files in the subtree being operated on. This should not impact
user experience unless the subtree is significantly large (> 10k files).

Other metrics related to the number of files and the number of blocks
* Master.TotalPath
* Master.InodeHeapSize (Estimate only)
* Master.UniqueBlocks and Master.BlockReplicaCount
* Master.BlockHeapSize - In addition to the total path, 
the total number of blocks also affects the heap usage, also to a lesser degree. 
Be sure to monitor this metric if you expect your total data size to be very large or grow quickly.
* Cluster.CapacityTotal
* Cluster.CapacityUsed
* Cluster.CapacityFree - Monitor if worker capacity is always full, consider more workers if that is the case.

### Number of Concurrent Clients

Concurrent clients represents number of logical Alluxio clients communicating with the Alluxio
Master or Worker. Concurrency is typically considered at a per node level.

Calculating concurrent clients requires estimating the number of Alluxio clients in the deployment.
This can typically be attributed to the number of threads allowed in the compute frameworks used.
For example, the number of tasks in a Presto job or the number of slots in a MapReduce node.

#### Number of concurrent clients on the master

The number of concurrent clients can not be directly observed, but can be found by monitoring
on the following metrics.

* Master.TotalRpcs
* Master.RpcQueueLength

Client connections to the master are typically short lived.
Note that although the number of potential concurrent clients are high, it is
unlikely for all clients to simultaneously hit the master. The steady state number of concurrent
clients to the master is generally lower than the master-side thread pool size defined by
`alluxio.master.rpc.executor.max.pool.size`.

The number of concurrent clients to the master impacts the following
* Number of cores required by the master - We recommend 8 clients per core, or to determine the
number of cores based on required operation throughput.
* Number of open files allowed on the master - We recommend about 4 open files per expected
concurrent client. On Linux machines this can be set by modifying `/etc/security/limits.d` and
checked with the `ulimit` command.

It is also important to monitor key timer metrics, as an abnormally high response rate would indicate the master is under stress.
* Master.JournalFlushTimer 
If the journal can’t keep up with the flush, master might report request count per second that is higher than initiated by clients. Consider using a more powerful master node.

* Master.ListStatus Timer
Any of the RPC timer statistics would help here. If the latency is abnormally high, master might be under a lot of load. 
Consider using a more powerful master node.

#### Number of concurrent clients on a worker

The number of concurrent clients that are actively reading from/writing to a worker can be found by:

* Worker.ActiveClients

You may also find other helpful information regarding the read/write performance associated with
the current level of concurrency in metrics like:
 
 * Worker.BytesReadDirectThroughput
 * Worker.BytesReadRemoteThroughput
 * Worker.BytesReadDomainThroughput
 * Worker.BytesReadUfsThroughput
 * and so on

> Note: Which metrics best reflects the performance of your current workload heavily depends on
> the nature of your workload.

Client connections to the worker are long lived, lasting the duration of a block read. Therefore,
the number of concurrent clients should be used to estimate the resource requirements, as opposed to
converting to operations per second like the master.

Concurrent clients can be estimated as below. In a deployment with 2 users, 50 Presto worker nodes
(with 200 task concurrency), and 50 Alluxio nodes, the estimations would come out to the following
* 50 (Presto workers) x 200 (task concurrency) / (50 (workers) x 0.5 (distribution)) = 400

The distribution factor is an estimate of how well the data is distributed among the workers. It can
be thought of as the probability of a random worker being able to serve the data. If all the data is on
one node, the distribution factor is 1 / # workers, if the data is likely to be on half of the
workers, the distribution factor is 1 / 2, and if the given dataset is likely to be on all workers,
the distribution factor is 1.

The number of concurrent clients to the worker impacts the following
* Amount of memory required by the worker - We recommend about 64MB per expected concurrent client
* Number of cores required by the worker - We recommend about 1 core per 4 expected concurrent
clients.
* Amount of network bandwidth required by the worker - We recommend at least 10 MB/s per concurrent
client. This resource is less important if a majority of tasks have locality and use short circuit.

The metric Worker.BlocksEvictionRate is an important measure of how full the Alluxio cache is. 
When this rate is high, it is a warning sign that the working set is significantly larger than what we can cache, 
or the access pattern is unfriendly to caching. Consider increasing the cache size per worker or number of workers.


## Alluxio Master Configuration

### Heap Size

The Alluxio master heap size controls the total number of files that can fit into the master memory.
Each file or directory will be represented by an inode in Alluxio, containing all its metadata.
In general you should provision roughly 2 KB of space for each inode.

If using `HEAP` metastore, all the inodes will be stored in the master heap. Therefore the master heap
size must be large enough to fit ALL inodes.

If using the `ROCKS` off-heap metastore, the master heap size must be large enough to fit the inode
cache. See the [RocksDB section]({{ '/en/operation/Metastore.html#rocksdb-metastore' | relativize_url }})
for more information.

Note that the master heap memory is not only allocated to metadata storage but also RPC logic and
internal management tasks. You should leave sufficient memory to those too.
The master heap allocation can be abstracted as:
```
METADATA_STORAGE_SIZE + RPC_CONSUMPTION + INTERNAL_MGMT_TASKS
```

The RPC memory consumption varies highly to your workload pattern. In general, below are the known top memory consumers:
1. Recursive 'rm' operations
2. Large metadata sync operation, typically triggered by recursive `ls` or `loadMetadata` operations
3. Worker registration
4. Small but frequent RPC surges like scanning thousands of files

The internal management tasks typically execute at intervals. On execution, 
they will scan a number of files and perform certain management operations.
For example, the ReplicationChecker will regularly scan files in the namespace and check their replica numbers.

It is hard to estimate the memory consumption of `RPC_CONSUMPTION` and `INTERNAL_MGMT_TASKS`.
A general good start is to leave at least 30% of heap to these two factors, monitor the heap usage
for a long period of time like a week, adjust accordingly and leave enough buffer for a workload surge.

The following JVM options, set in `alluxio-env.sh`, determine the respective maximum heap sizes for
the Alluxio master and standby master processes to `256 GB`:

```bash
ALLUXIO_MASTER_JAVA_OPTS+=" -Xms256g -Xmx256g "
```

* As a rule of thumb, manually set the min and max heap size equal to avoid heap resizing.
Do not leave these blank and rely on the default values.
* Each thread spawned by the master JVM requires off heap space determined by the thread stack size.
When setting the heap size, ensure that there is enough memory allocated for off heap storage.
For example, spawning `4000` threads with a default thread stack size of `1 MB` requires at least
`4 GB` of off-heap space available.
* Network buffers are often allocated from a pool of direct memory in Java. 
The configuration controlling the maximum size of direct memory allocated defaults to the `-Xmx` setting, 
which can leave very little space for the other critical processes in the system. 
We recommend setting the direct memory to 10GB for both Alluxio Master and Alluxio Workers in a typical deployment, 
and only increase it if the number of concurrent clients/RPC threads are increased and you see failure
in allocating from direct memory.

```bash
ALLUXIO_JAVA_OPTS+=" -XX:MaxDirectMemorySize=10g "
```

### Number of Cores

The Alluxio Master’s ability to handle concurrent requests and parallelize recursive operations
(i.e. full sync, check consistency) scales with the number of cores available. In addition,
background processes of the Alluxio Master also require cores.

Alluxio microbenchmarks, show the following operation throughput on 4vCores (r5.xlarge) on the
master. There are 32 clients. The journal is on HDFS.
* Create File - 3000 ops/second
* List Status (file) - 65000 ops/second
* List Status (dir) - 9000 ops/second
* Delete File - 10000 ops/second
* List Status (file does not exist) - 15000 ops/second

Because of the sensitivity of the Alluxio Master to CPU load and network load, we recommend a
dedicated node for the Alluxio Master which does not run any other major processes aside from the
Alluxio Job Master.

The minimum number of cores supportable is 4 and the suggested minimum number of cores is 32.

### Disk

The Alluxio Master needs disk space to write logs as well as the journal if running with embedded
journal.

We recommend at least 8 GB of disk space for writing logs. The write speed of the disk should be at
least 128 MB/s.

When using embedded journal, the disk space is proportional to the namespace size and typical number
of write operations within a snapshot period. We recommend at least 8 GB of disk space plus 8 GB for
each 1 million files in the namespace. The read and write speed of the disk should be at least
512 MB/s. We recommend a dedicated SSD for the embedded journal.
See the [Journal Size Management]({{ '/en/operation/Journal.html#managing-the-journal-size' | relativize_url }})
section for more information.

When using RocksDB as the storage backend for the file system metadata, the disk space required is 
proportional to the namespace size. See the [RocksDB section]({{ '/en/operation/Metastore.html#rocksdb-metastore' | relativize_url }})
for more information. We recommend 4 GB of disk space for each 1 million files in the name space.

### Operating System Limits

An exception message like `java.lang.OutOfMemoryError: unable to create new native thread`
indicates that operating system limits may need tuning.

Several parameters in the Linux kernel limit the number of threads that a process can spawn:
- `kernel.pid_max`: Run `sysctl -w kernel.pid_max=<new value>` as root
- `kernel.thread_max`: Run `sysctl -w kernel.thread_max=<new value>` as root
- `vm.max_map_count`: Run command `sysctl -w vm.max_map_count=<new value>` as root
- Max user process limit: Update `/etc/security/limits.d` with `[domain] [type] nproc [value]` for
example, if Alluxio is run under user `alluxio`: `alluxio soft nproc 4096`.
- Max open files limit: Update `/etc/security/limits.d` with `[domain] [type] nfile [value]` for
example, if Alluxio is run under user `alluxio`: `alluxio soft nofile 4096`.
- User specific pid_max limit: Run command `sudo echo <new value> > /sys/fs/cgroup/pids/user.slice/user-<userid>.slice/pids.max` as root

These limits are often set for the particular user that launches the Alluxio process.
As a rule of thumb, `vm.max_map_count` should be at least twice the limit for master threads
as set by `alluxio.master.rpc.executor.max.pool.size`.

### Operating System Tuning

The Linux kernel has many tuning parameters.
Here are the recommended settings for Alluxio components.

#### Disable vm.zone_reclaim_mode

It is strongly recommended to disable `vm.zone_reclaim_mode` for Alluxio servers (masters, workers).
This is because zone reclaims can induce significantly high rates of memory page scans, which
can negatively affect the Alluxio server JVMs.
This can result in unexpected long pauses of the JVM (not due to garbage collection), which will
hinder the operation of the Alluxio server.
See the [kernel documentation for zone_reclaim_mode](https://www.kernel.org/doc/Documentation/sysctl/vm.txt)
which recommends keeping `vm.zone_reclaim_mode` disabled for workloads identical for Alluxio workers.

To disable this for the system, persistent across reboots, update `/etc/sysctl.conf` to include
```bash
vm.zone_reclaim_mode=0
```
and then run the following to load the settings:
```bash
sysctl -p
```

To disable this for the system temporarily, run:
```bash
sysctl -w vm.zone_reclaim_mode=0
```

### Heartbeat Intervals and Timeouts

See [below section](#master-worker-heartbeat) for a detailed explanation.

## Alluxio Worker Configuration

### Heap Size

Alluxio workers require modest amounts of memory for metadata because off-heap storage is used for data storage.
However, data transfer will create buffers that consume heap or direct memory.
We recommend about 64MB (from the heap or direct memory) per expected concurrent client.

As a beginning, you can set both to 8G and tune up when you see the worker running out of heap/direct memory. 
```properties
ALLUXIO_WORKER_JAVA_OPTS+=" -Xms8g -Xmx8g -XX:MaxDirectMemorySize=8g"
```

### Number of Cores

The Alluxio worker’s ability to handle concurrent remote I/O requests depends on the number of cores
available. We recommend 1 core for every 4 concurrent remote requests.

### Network Bandwidth to Compute Nodes

The Alluxio worker’s network bandwidth determines the rate at which it can send data to remote
clients. With 8 concurrent clients, the worker is able to saturate a 10 Gbit link. We recommend
having at least 10 Gbit connectivity to compute nodes.

### Network Bandwidth to the Under File System (UFS)

The Alluxio worker’s network bandwidth to UFS determines the rate at which it can read data to serve
or populate the cache from the underlying storage. If the network link is shared with the compute
nodes, the async caching options will need to be managed in order to ensure the appropriate ratio
between serving client requests and populating the cache is respected.
We recommend having a separate link for bandwidth to the UFS. For every 10 Gbit/s bandwidth to compute
nodes (across workers), we recommend having 1 Gbit/s bandwidth (across workers) to the UFS. This
gives a ratio of at least 10:1. The UFS link throughput can be greatly decreased based on the
expected cache hit ratio.

You may use the [UfsIOBench]({{ '/en/administration/StressBench.html#ufs-io-bench' | relativize_url }})
tool to measure the worker-UFS network bandwidth.

### Disk

The Alluxio worker needs local disk space for writing logs and temporary files to object stores.

We recommend at least 8 GB of disk space for writing logs. The write speed of the disk should be at
least 128 MB/s.

We recommend `8 GB + expected number of concurrent writers * max size of file written to object
stores disk space` for writes to an object store. This disk should be a dedicated SSD supporting
512 MB/s read and write.

### Worker Cache Storage

The Alluxio worker needs storage space (memory, SSD, or HDD) to cache files. We recommend sizing the
total aggregated worker storage to be at least 120% of the expected working set. If the expected
working set is unknown, we recommend starting with 33% of the total dataset.

Note that if you have more than one replica for each block, you should adjust the cache
size estimation accordingly. 
See [Managing Data Replication in Alluxio]({{ '/en/core-services/Caching.html#managing-data-replication-in-alluxio' | relativize_url }})
for more details.

### Heartbeat Intervals and Timeouts

See [below section](#master-worker-heartbeat) for a detailed explanation.

### Keepalive Time and Timeout

Alluxio workers are configured to check the health of connected clients by sending keepalive pings.
This is controlled by the following properties
```properties
alluxio.worker.network.keepalive.time=30s
alluxio.worker.network.keepalive.timeout=30s
```

`alluxio.worker.network.keepalive.time` controls the maximum wait time since a client sent the last
message before worker issues a keepalive request.
`alluxio.worker.network.keepalive.timeout` controls the maximum wait time after a keepalive request
is sent before the worker determines the client is no longer alive and closes the connection.

## Alluxio Client Configuration

### RPC Retry Interval

The following properties tune RPC retry intervals:

```properties
alluxio.user.rpc.retry.max.duration=2min
alluxio.user.rpc.retry.base.sleep=50ms
```

The retry duration and sleep duration should be increased if frequent timeouts are observed
when a client attempts to communicate with the Alluxio master.

### Keepalive Time and Timeout

The Alluxio client can also be configured to check the health of connected workers using keepalive
pings.
This is controlled by the following properties
```properties
alluxio.user.network.streaming.keepalive.time=Long.MAX_VALUE
alluxio.user.network.streaming.keepalive.timeout=30s
```
`alluxio.user.network.streaming.keepalive.time` controls the maximum wait time since a worker sent the last
message before client issues a keepalive request.
`alluxio.user.network.streaming.keepalive.timeout` controls the maximum wait time after a keepalive request is
sent before the client determines the worker is no longer alive and closes the connection.
This is disabled by default (the default value for `alluxio.user.network.streaming.keepalive.time` is
`Long.MAX_VALUE` which effectively disables the keepalive) to minimize unintended performance impact
to workers.
You might want to enable it if you find that the Alluxio client is waiting a long time on dead
workers.
To enable it, set the property `alluxio.user.network.streaming.keepalive.time` to a desired interval.

## Resource Sharing with Co-located Services

In many cases, Alluxio is not the only resource intensive service running on a node.
In most cases, our users choose to co-locate the computation framework such as Presto or Spark with Alluxio,
to fully take advantage of the data locality.
Allocation of limited resources to different services such as Presto, Spark and Alluxio becomes an interesting challenge,
and can have significantly impact on the performance of the tasks or queries.
Unbalanced resource allocation can even lead to query failures and processes exiting with an error.

The optimal allocation depends heavily on the use case, architecture and the workflow.
This section is not the silver bullet resource allocation manual.
However, we recommend you to start with the best practices below.

### Memory Allocation between Compute with Alluxio

When Presto or Spark is co-located with Alluxio, memory is often the most contentious resource.
Both Presto and Spark need a large amount of memory to be able to efficiently process queries.
Alluxio also needs memory for caching and metadata management.

In a typical deployment setup, the Alluxio workers are co-located with the compute worker processes for the best data locality.
Although in the ideal setup the Alluxio masters all have their dedicated nodes, 
it is also not rare that the Alluxio masters are co-located with the compute coordinator processes for centralized management. 
Since the memory usage patterns are different for these two kinds of nodes, they will be discussed separately.

#### Co-located Coordinator and Master

In many deployment settings, the Presto coordinators or the Spark drivers are running on the same node as the Alluxio masters.
They are good candidates to be co-located because the Alluxio master consumes large amount of memory due to the metadata it keeps, 
but Presto coordinator and Spark masters are often less demanding on the memory compared to their workers.

The total memory consumption can be roughly abstracted as follows: 
```
ALLUXIO_MASTER_JVM_SIZE + COMPUTE_JVM_SIZE  + SYSTEM_RESOURCE_REQUIREMENT
```
> Note: The JVM_SIZE means ALL the memory allocated to the JVM, including off-heap and metaspace.

If the sum of these values are near the system total available memory, the performance of your operating system will degrade.
To make the matter worse, in certain architectures, the system resource manager may kill running processes
to release some resources.
It may choose the process with the highest badness score (frequently the process using the most memory) and kill it.
This would likely kill the Alluxio master process and lead to system downtime.

System resources should have the top priority in resource estimation.
The operating system needs some memory for its own kernel data structures and other system programs.
So it is recommended to leave at least 10-15GB for that purpose.

The Presto coordinator / Spark driver needs sufficient memory to launch and complete queries. 
So sufficient `COMPUTE_JVM_SIZE` would demand the next highest priority.

The Alluxio master's memory requirement depends on the amount of file metadata and workload.
See the [Master Heap Size Estimation](#heap-size) section for more details.
If the heap size is sufficient for all the metadata in your namespace, you may put all metadata in
the heap to achieve the best performance. Otherwise, we recommend using RocksDB as metadata storage
to reduce the Alluxio master heap consumption. It is wise to always leave enough space to
prepare for unexpected workload surges. If you are using RocksDB as metadata store but have
extra space on the heap, you may increase `alluxio.metadata.cache.max.size` to allocate more cache space for the RocksDB.

#### Co-located Workers

It is also natural to colocate the Presto workers / Spark executors with the Alluxio workers. 
However, both of them can require a large amount of memory, so it is important to prioritize their allocations.
Similar to the master's case, the total memory consumption can be abstracted as follows:
```
ALLUXIO_RAMDISK_SIZE + ALLUXIO_WORKER_JVM_SIZE + COMPUTE_WORKER_JVM_SIZE + SYSTEM_RESOURCE_REQUIREMENT
```
> Note: The JVM_SIZE means ALL the memory allocated to the JVM, including off-heap and metaspace.

When the worker memory is constrained, we recommend the following prioritization.

System resources contains file descriptor tables and thread allocations, and are limited on the workers, 
because workers tend to have fewer concurrent accesses compared to master. 
But we recommend leaving 10-15 GB at least for this purpose as well. 

The next priority should be `COMPUTE_WORKER_JVM_SIZE`.
If the compute worker's JVM heap is too small, some queries will simply fail.
Unfortunately, it is difficult to know much memory a query will need unless you run it.
So you should monitor the system while your normal workload runs, in order to get better
estimations on how much resources the compute worker typically needs.
Tools such as `top` can be used to monitor the memory consumption of a process.
There are more advanced operating system level monitor tools and we recommend you to explore
and find the one that is the most suitable to your architecture.
Also, compute frameworks tend to have their own resource monitoring tools that can provide more insights.

`ALLUXIO_WORKER_HEAP_SIZE` does not need to be very large, but it is critical to ensure 
it is enough for the correct operation of the Alluxio worker.
See [Worker Heap Size Estimation](#heap-size-1) for more details.

The last priority should be the `ALLUXIO_RAMDISK_SIZE`. 
Cache misses will negatively impact the performance, but will not have any impact on query correctness.
Alluxio also has the ability to cache on SSD and HDD, thus reducing the memory demand.
We recommend SSDs over HDDs.

### CPU Allocation between Compute and Alluxio

Note that when we co-locate compute with Alluxio, compute frameworks are also the clients to the Alluxio system.
This is important because the overall system performance depends on the clients supplying enough work that the Alluxio system can efficiently handle. 
Given the fixed total system resource, giving too much resource to compute / clients will result in Alluxio not being able to handle such requests.
Vice versa, giving too much resource to Alluxio will result in not enough requests being generated.

This is usually not a huge issue because of dynamic CPU scheduling on these nodes. 
However, when containers or strict CPU quotas are enforced, we may run into situations where we have too few or too many requests. 
The optimal balance is heavily dependent on the exact workload. 
We recommend looking at several metrics such as `Worker.BlocksEvictionRate`, `Cluster.BytesReadLocalThroughput`, 
and master RPC latency metrics to find clues whether your allocation is too compute heavy or too Alluxio heavy.

## Alluxio Master-Worker Interactions

Alluxio workers will register with the master and regularly send heartbeats. 
The overhead of this worker state synchronization is linear to the number of workers
in the cluster, and the amount of blocks each worker possesses.
As your cluster scales up, this overhead become non-trivial and requires thoughts and tunings ahead.

When the cluster scales up, this bottleneck is typically on the master side.
In the extreme case, adding a worker to the cluster will slow down work,
instead of providing more cache capabilities.
This section will list the parts of this synchronization that are most resource demanding.
When tuning towards a larger scale, start with these first.

### Worker Registration

When the worker registers with a master, it will report the below information:
1. Some metadata about the worker itself, like the WorkerId.
1. Metadata about the worker's tiered storage, like the capacity/usage of each tier.
1. A full block list of all the blocks that are currently on the worker.

Obviously, the size of the report is linear to the number of blocks.
The serialized size of the report is roughly
`(number of blocks) * 6 bytes + constant size for other metadata`.
When processing the worker registration report, the master side will spend much more
memory than the serialized size.
During the process, the total master-side memory consumption is 200MB~400MB per 1 million blocks.
If you have 20 workers that all register at the same time, each having 1 million blocks,
the master-side memory demand will have the upper limit of
```
20 * 1 * 400MB = 8000MB
``` 

Allocating a large amount of memory from heap will take away the memory that are used to
handle other requests, so when your workers are registering, the throughput for other
workload will be hurt. You may also see the Alluxio master experience Full GC or OOM errors.

Therefore, in Alluxio 2.7 we introduced two mechanisms below to perform better flow control.

If you are on older Alluxio versions which do not have the new flow control features,
you can manually start workers in batches to have a similar effect.

#### Worker Register Lease

Workers now need to request a "lease" from the master before sending the register report messages.
If rejected, the worker will back off exponentially and retry.

```properties
# Below are the default values
alluxio.master.worker.register.lease.enabled=true
alluxio.master.worker.register.lease.count=20
```

Now the master will only allow a certain number of workers to register at the same time.
When the cluster scale is large, workers will queue up and register.
The impact on the master side will be more smoothed out.
The optimal number of leases depends on your cluster size and average number of
blocks per worker node. You may find relevant information from the metrics
or by looking at the storage on each worker, and follow the size estimation process
above to make your best estimation. If your workers are currently empty, you may
estimate based on the worker storage and the average file/block size.

This feature is enabled by default. Typically you only need to tweak the 2 properties above.
But you may find all relevant properties in the [Properties List]({{ '/en/reference/Properties-List.html' | relativize_url }})
starting with `alluxio.master.worker.register.lease` and `alluxio.worker.register.lease`.

When the register lease feature is turned on, you will see a delay when
the workers attempt to register to the master. This is expected.

#### Streaming Worker Registration

In Alluxio 2.7, workers can send the register report to the master in a stream of smaller requests,
instead of one single large request. Although the total master-side overhead of dealing with
all the requests is larger, the master-side heap consumption spent on registering workers can be
kept at a lower level overall. Smaller requests are also more GC-friendly so the master JVM has a higher throughput.

```properties
# Below are the default values
alluxio.worker.register.stream.enabled=true
alluxio.worker.register.stream.batch.size=100000
alluxio.worker.register.stream.deadline=15min
```

As previously mentioned, the worker will send metadata and the full block list to the master
when it registers. When streaming is used, the full request is broken into several smaller request.
The 1st request will contain the metadata and the block list 
(limited by `alluxio.worker.register.stream.batch.size`).
The following requests will only contain the block list.
Therefore, if your worker has fewer blocks than `alluxio.worker.register.stream.batch.size`,
only 1 request will be sent and the performance should be identical.
Thus we recommend to keep this enabled despite your worker storage size.

This feature is enabled by default. Typically you only need to tune the 3 properties above.
But you may find all relevant properties in the [Properties List]({{ '/en/reference/Properties-List.html' | relativize_url }})
starting with `alluxio.master.worker.register.stream` and `alluxio.worker.register.stream`.

### Master-Worker Heartbeat

The workers and the master maintain regular heartbeats. Workers send the block state changes
to the master, and the master replies with commands to persist/remove blocks.

#### Master-side
The frequency with which the master checks for lost workers is set by the
`alluxio.master.worker.heartbeat.interval` property, with a default value of `10s`.
If the master has not received a heartbeat from the worker for more than 
`alluxio.master.worker.timeout`, the worker will be identified as lost. 
The master will then know the cached blocks that are served by this worker
are no longer available, and instruct the clients to not read or write on that worker.

If a worker sends a heartbeat to the master after being identified as lost,
the master will instruct the worker to register again and report all the blocks
on the worker.

On the master side, removing the worker from the locations of each block cache is an expensive operation. 
Registering a worker which has many blocks later is also and expensive operation. 
So it is ideal to avoid incorrectly identifying a worker as lost.
You should set `alluxio.master.worker.timeout` to a value where you
do not have false positives for this lost worker detection, 
and still be able to tell a worker is lost with a reasonable delay.

If you still see workers being identified as lost incorrectly, that suggests
the master is unable to process the worker heartbeat requests in time.
That means your Alluxio master process requires tuning.
You should either increase the resources/concurrency allowed on the master,
or reduce the pressure.

The master distinguishes the worker by (hostname, ports, domain socket path, tiered identity).
If any of these attributes change, the master will think the worker is a new one
and allocate one new worker ID to it.
Consequently, the "old" worker will be removed and the "new" worker will be added,
which are expensive metadata updates.
Therefore, in order to avoid worker starting with different ports, it is recommended to
configure workers to use static ports. In other words, we do not recommend setting workers to use port 0 like
`alluxio.worker.rpc.port=0` because the port will be decided dynamically at runtime.
All port configurations in Alluxio are by default static.

#### Worker-side
The frequency with which a worker checks in with the master is set by the following property:
```properties
alluxio.worker.block.heartbeat.interval=1s
```

`alluxio.worker.block.heartbeat.interval` controls the heartbeat interval for the block service in Alluxio.
In each heartbeat, the worker will report the changes in blocks during the last heartbeat interval.
If you have a large number of workers, or there are large numbers of operations so that each
heartbeat carries a lot of updates, the master will experience significant pressure in handling the
heartbeat requests. In that case, it is recommended to increase `alluxio.worker.block.heartbeat.interval`.

`alluxio.worker.block.heartbeat.interval` affects how fast you see the block information update
from workers. 
For example, you issued an `alluxio fs free /path` command. The files and blocks will be marked
as freed in Alluxio and when the worker that contains relevant blocks heartbeat to the master,
the master will instruct it to remove the freed blocks.
The worker will remove those block cache from its storage and report the change in the next heartbeat,
as soon as the removal have completed.
Therefore it takes at least two heartbeat cycles for the master to remove blocks,
and subsequently update the worker storage percentage on the web UI. 

Typically, having a longer heartbeat interval should not affect Alluxio performance and throughput except:
1. How fast the master orders the workers to persist blocks written by `ASYNC_THROUGH`.
1. The master will be less up-to-date on the worker's storage usage and block states for the asynchronously
persisted blocks and freed blocks.

### Master-Worker Pinned File List Sync
The master periodically updates the worker on which files are currently pinned in the Alluxio namespace.
The blocks that belong to pinned files cannot be evicted from the worker.
In other words, if the worker storage is full, the worker will evict blocks that do not
belong to pinned files.

Currently, the worker will sync with the master on the pinned file list once every 1 second.
```properties
alluxio.worker.block.heartbeat.interval.ms=1sec
```

If the cluster scale is large or there are many pinned files, this will create significant
pressure on the master. In that case we recommend increasing this interval. 
The pinned files come from:
1. Files that are manually pinned using the `alluxio fs pin` command.
2. Files written with ASYNC_THROUGH are pinned until they are persisted into UFS.
