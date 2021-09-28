---
layout: global
title: Scalability Tuning
nickname: Scalability Tuning
group: Operations
priority: 9
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

### Number of Files in Alluxio

In this section "files" refers to regular files and directories.
The number of files in Alluxio can be monitored through the metric `Master.TotalPaths`.
A third party metrics collector can be used to monitor the rate of
change of this metric to determine how the number of files are growing over time.

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
* MMaster.UniqueBlocks and Master.BlockReplicaCount
* Master.BlockHeapSize 
In addition to the total path, the total number of blocks also affects the heap usage, also to a lesser degree. 
Be sure to monitor this metric if you expect your total data size to be very large or grow quickly.

* Cluster.CapacityTotal
* Cluster.CapacityUsed
* Cluster.CapacityFree
Monitor if worker capacity is always full, consider more workers if that is the case.

### Number of Concurrent Clients

Concurrent clients represents number of logical Alluxio clients communicating with the Alluxio
Master or Worker. Concurrency is typically considered at a per node level.

Calculating concurrent clients requires estimating the number of Alluxio clients in the deployment.
This can typically be attributed to the number of threads allowed in the compute frameworks used.
For example, the number of tasks in a Presto job or the number of slots in a MapReduce node.

#### Master

Client connections to the master are short lived, so we first estimate the number of concurrent
clients and then convert to a operations/second metric
* 1 per Alluxio worker in the cluster
* Max of
	* Number of concurrent clients of the system as calculated above
	* (`alluxio.worker.block.master.client.pool.size` + `alluxio.user.file.master.client.pool.size.max`)
	per user per service using the Alluxio client

For example, in a deployment with 2 users, 50 Presto worker nodes (with 200 task concurrency), and
50 Alluxio nodes, the estimations would come out to the following
* 50 (workers) + 2 (users) x 11 (block pool size) x 10 (file pool size) x 50 (services) = 11050
* 50 (workers) + 50 (Presto workers) x 200 (task concurrency) = 10050

Yielding a max of 11050

Note based on the number of concurrent queries, add 1 more service for each to account for the
Presto coordinator’s connections.

If a maximum latency of 1 second is expected at absolute peak capacity, the master would need to
support about 11050 operations per second. The typical operation to benchmark is `getFileInfo` for
OLAP frameworks. Note that although the number of potential concurrent clients are high, it is
unlikely for all clients to simultaneously hit the master. The steady state number of concurrent
clients to the master is generally much lower.

The number of concurrent clients to the master impacts the following
* Number of cores required by the master - We recommend 8 clients per core, or to determine the
number of cores based on required operation throughput.
* Number of open files allowed on the master - We recommend about 4 open files per expected
concurrent client. On Linux machines this can be set by modifying `/etc/security/limits.d` and
checked with the `ulimit` command.

It is also important to monitor key timer metrics, as an abnormally high response rate would indicate the master is under stress.
* Master.JournalFlushTimer 
Journal can’t keep up with the flush, master request per second might be too high。 Consider using a more powerful master node.

* Master.ListStatus Timer
Any of the RPC timer statistics would help here. If the latency is abnormally high, master is under a lot of load.  It might be time for a more powerful master node.

#### Worker

Client connections to the worker are long lived, lasting the duration of a block read. Therefore,
the number of concurrent clients should be used to estimate the resource requirements, as opposed to
converting to operations per second like the master.

Concurrent clients can be estimated with the same formula.

Using the same example as the master, in a deployment with 2 users, 50 Presto worker nodes
(with 200 task concurrency), and 50 Alluxio nodes, the estimations would come out to the following
* 50 (Presto workers) x 200 (task concurrency) / (50 (workers) x 0.5 (distribution)) = 400

The distribution factor is an estimate of how well the data is distributed among the workers. It can
be thought of the probability of a random worker being able to serve the data. If all the data is on
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
When this rate is high, it is a warning sign that the working set is significantly larger than what we can cache, or the access pattern is unfriendly to caching.  Consider increasing the cache size per worker or number of workers.


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
 
The following JVM options, set in `alluxio-env.sh`, determine the respective maximum heap sizes for
the Alluxio master and standby master processes to `256 GB`:

```bash
ALLUXIO_MASTER_JAVA_OPTS+=" -Xms256g -Xmx256g "
```

* As a rule of thumb set the min and max heap size equal to avoid heap resizing.
* Each thread spawned by the master JVM requires off heap space determined by the thread stack size.
When setting the heap size, ensure that there is enough memory allocated for off heap storage.
For example, spawning `4000` threads with a default thread stack size of `1 MB` requires at least
`4 GB` of off-heap space available.
* Network buffers are often allocated from a pool of direct memory in Java. 
The configuration controlling the maximum size of direct memory allocated defaults to the `-Xmx` setting, 
which can leave very little space for the other critical processes in the system. 
We recommend setting it to 10GB for both Alluxio Master and Alluxio Workers in a typical deployment, and only increase it
if the number of concurrent clients/RPC threads are increased.

```bash
ALLUXIO_JAVA_OPTS+=" -XX:MaxDirectMemorySize=10g "
```

### Number of Cores

The Alluxio Master’s ability to handle concurrent requests and parallelize recursive operations
(ie. full sync, check consistency) scales with the number of cores available. In addition,
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
Job Master.

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

When using RocksDB as the storage backend for the file system metadata, the disk space required is 
proportional to the namespace size.
We recommend 4 GB of disk space for each 1 million files in the name space.

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

These limits are often set for the particular user that launch the Alluxio process.
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

The frequency with which the master checks for lost workers is set by the
`alluxio.master.worker.heartbeat.interval` property, with a default value of `10s`.
Increase the interval to reduce the number of heartbeat checks.

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

### Disk

The Alluxio worker needs local disk space for writing logs and temporary files to object stores.

We recommend at least 8 GB of disk space for writing logs. The write speed of the disk should be at
least 128 MB/s.

We recommend 8 GB + expected number of concurrent writers * max size of file written to object
stores disk space for writes to an object store. This disk should be a dedicated SSD supporting
512 MB/s read and write.

### Worker Storage

The Alluxio worker needs storage space (memory, SSD, or HDD) to cache files. We recommend sizing the
total aggregated worker storage to be at least 120% of the expected working set. If the expected
working set is unknown, we recommend 33% of the total dataset.

### Heartbeat Intervals and Timeouts

The frequency with which a worker checks in with the master is set by the following property:
```properties
alluxio.worker.block.heartbeat.interval=1s
```

`alluxio.worker.block.heartbeat.interval` controls the heartbeat interval for the block service in
Alluxio.
Again, increase the interval to reduce the number of heartbeat checks.

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
alluxio.user.rpc.retry.base.sleep=1s
```

The retry duration and sleep duration should be increased if frequent timeouts are observed
when a client attempts to communicate with the Alluxio master.

### Keepalive Time and Timeout

The Alluxio client can also be configured to check the health of connected workers using keepalive
pings.
This is controlled by the following properties
```properties
alluxio.user.network.keepalive.time=2h
alluxio.user.network.keepalive.timeout=30s
```
`alluxio.user.network.keepalive.time` controls the maximum wait time since a worker sent the last
message before client issues a keepalive request.
`alluxio.user.network.keepalive.timeout` controls the maximum wait time after a keepalive request is
sent before the client determines the worker is no longer alive and closes the connection.
This is disabled by default (the default value for `alluxio.user.network.keepalive.time` is
`Long.MAX_VALUE` which effectively disables the keepalive) to minimize unintended performance impact
to workers.
You might want to enable it if you find that the Alluxio client is waiting a long time on dead
workers.
To enable it, set the property `alluxio.user.network.keepalive.time` to a desired interval.

## Resource Sharing with Colocated Services

In many cases, Alluxio is not the only resource intensive service running on a node.
Frequently, our users choose to colocate the computation framework such as Presto or Spark with Alluxio,
to fully take advantage of the data locality.
Allocation of limited resources to different services such as Presto, Spark and Alluxio becomes an interesting challenge,
and can have signficant impact on the performance of the tasks or queries.
Unbalanced resource allocation can even lead to query failures and processes exiting with an error.

### Memory Allocation between Compute with Alluxio

When Presto or Spark is colocated with Alluxio, memory is often the most contentious resource.
Both Presto and Spark need a large amount of memory to be able to efficiently process queries.
Alluxio also needs memory for caching and metadata management, unless SSD or HDD is used as the primary caching medium.

#### Colocated Coordinator and Master

In many deployment settings, Presto coordinator or Spark master is running on the same node as the Alluxio master.
They are good candidates to be colocated because Alluxio master consumes large amount of memory due to the metadata it keeps, but Presto coordinator and Spark masters are often less demanding on the memory compared to their workers.

The total amount of memory consumed by these two applications are roughly
Alluxio JVM size + Presto/Spark JVM size  + System resource memory size
Linux also needs some memory for its own kernel data structures and other system programs as well.
So it is recommended to leave at least 10-15GB for that purpose as well.
If the sum of these values are near the system total available memory, Out-of-memory killer may be triggered.
It will choose the process with the highest badness score (frequently the process using the most memory) and kill it.
This would likely kill the Alluxio master and lead to system downtime.

If memory resource is constrained, Presto coordinator / Spark master needs sufficient memory to launch and complete queries. 
So it would demand the highest priority. 
If Alluxio metadata can not fit in the remaining memory, RocksDb-based offheap storage solution should be considered. 
Then we consider memory required by the thread allocations and direct memory, this is dependent on the number of threads, so we leave it as the last priority.

#### Colocated Workers

It is also natural to colocate the Presto / Spark workers with Alluxio workers. 
However, both of them can require a large amount of memory, so it is important to prioritize their allocations.
Similar to the master's case, the total memory consumption is 
ALLUXIO_RAM_DISK_SIZE + ALLUXIO_WORKER_HEAP_SIZE + COMPUTE_WORKER_HEAP_SIZE + SYSTEM RESOURCE REQUIREMENT

When the worker memory is constrained, we recommend the following prioritization.
System resources contains file descriptor tables and thread allocations, and are limited on the workers, because workers tend to have fewer concurrent accesses compared to master. But we recommend leaving 10-15 GB at least for this purpose as well.
The next priority should be COMPUTE_WORKER_HEAP_SIZE.
If the compute worker's heap is too small, some queries will simply fail.
Unfortunately, it is difficult to know much memory a query will need unless you run it.
Tools such as top can be used to monitor the peak memory consumptions of the presto process.
ALLUXIO_WORKER_HEAP_SIZE does not need to be very large, but it is critical to ensure it is enough for the correct operation of the Alluxio worker.
The last priority should be the RAMDISK_SIZE. 
Uncached data will negatively impact the performance, but will not have any impact on query correctness.
Alluxio also has the ability to cache on SSD and HDD, thus avoid using the valuable memory resource as ramdisks.

### CPU Allocation between Compute and Alluxio

Note that when we colocate compute with Alluxio, compute frameworks are also the clients to Alluxio system.
This is important because the overall system performance depends on the clients supplying enough work that the Alluxio system can efficiently handle. 
Given the fixed total system resource, giving too much resource to compute / clients will result in Alluxio not being able to handle such requests.
Vice versa, giving too much resource to Alluxio will result in not enough requests being generated.

This is usually not a huge issue because of dynamic CPU scheduling on these nodes. 
However, when containers or strict CPU quotas are enforced, we may run into situations where we have too few or too many requests. 
The correct balance is heavily dependent on the exact workload. We recommend looking at several metrics such as `Worker.BlocksEvictionRate` , `Cluster.BytesReadLocalThroughput`, and master RPC latency metrics to find clues whether your allocation is too compute heavy or too Alluxio heavy.
