---
layout: global
title: Performance Tuning
nickname: Performance Tuning
group: Operations
priority: 8
---

* Table of Contents
{:toc}

This document goes over various tips and configuration to tune Alluxio performance.

## Common Performance Issues

The following is a checklist to run through to address common problems when tuning performance:

1. Are all nodes working?

   Check that the Alluxio cluster is healthy. You can check the web user interface at
   `http://<master hostname>:19999` to see if the master is reachable from a browser.
   Similarly, workers can be reached by clicking on the "workers" tab of the Alluxio master UI
   or by navigating to `http://<worker hostname>:30000/`
   Alternatively, run `bin/alluxio fsadmin report` to collect similar information from the console.
   Both the web interfaces and command-line output contain metrics to verify if any nodes are out of
   service and the last known heartbeat times.

1. Are short-circuit operations working?

   If a compute application is running co-located with Alluxio workers, check that the
   application is performing short-circuit reads and writes with its local Alluxio worker.
   Monitor the metrics values for `cluster.BytesReadRemoteThroughput` and `cluster.BytesReadLocalThroughput`
   while the application is running (Metrics can be viewed through `alluxio fsadmin report metrics`).
   If the local throughput is zero or significantly lower than the remote alluxio read throughput,
   the compute application is likely not interfacing with a local Alluxio worker.
   The Alluxio client uses hostname matching to determine the existence of a local Alluxio worker.
   Check that the client and worker use the same hostname string.
   Configuring `alluxio.user.hostname` and `alluxio.worker.hostname` sets the client and worker
   hostnames respectively.

   Note: In order to retrieve metrics for short circuit IO, the client metrics collection need to
   be enabled by setting `alluxio.user.metrics.collection.enabled=true` in
   `alluxio-site.properties` or corresponding application configuration.

1. Is data is well-distributed across Alluxio workers?

   By default, Alluxio clients will use the `LocalFirstPolicy` to write data to their local
   Alluxio worker. This is efficient for applications which write data from many nodes concurrently.
   In a scenario where all data is written from a single node, its local worker will be filled,
   leaving the remaining workers empty.
   See [this page][1] for discussion of the different location policies and how to configure them.

1. Are there error messages containing "DeadlineExceededException" in the user logs?

   This could indicate that the client is timing out when communicating with an Alluxio worker.
   To increase the timeout, configure `alluxio.user.streaming.data.timeout`, which has a default of `30s`.

   If write operations are timing out, configure `alluxio.user.streaming.writer.close.timeout`,
   which has a default of `30m`. This is especially important when writing large files to object stores
   with a slow network connection. The entire object is uploaded at once upon closing the file.

   There have also been rare cases, where on linux [NUMA-based systems](https://en.wikipedia.org/wiki/Non-uniform_memory_access)
   that Alluxio workers might behave in a sporadic way causing pauses or periods of unavailability due
   to the kernel's `vm.zone_reclaim_mode` behavior. For NUMA systems it is recommended to
   **disable zone reclaim** by setting `vm.zone_reclaim_mode=0` inside of `/etc/sysctl.conf` or
   similar configuration files on Alluxio workers.

1. Are there frequent JVM GC events?

   Frequent and long GC operations on master or worker JVMs drastically slow down the process.
   This can be identified by adding logging for GC events; append the following to `conf/allulxio-env.sh`:

```
ALLUXIO_JAVA_OPTS=" -XX:+PrintGCDetails -XX:+PrintTenuringDistribution -XX:+PrintGCTimeStamps"
```

   Restart the Alluxio servers and check the output in `${ALLUXIO_HOME}/logs/master.out` or
   `${ALLUXIO_HOME}/logs/worker.out` for masters and workers respectively.

Also check out the [metrics system][2] for better insight in how the Alluxio service is performing.

[1]: {{ '/en/api/FS-API.html' | relativize_url }}#location-policy
[2]: {{ '/en/operation/Metrics-System.html' | relativize_url }}

## General Tuning

### JVM Monitoring

To detect long GC pauses, Alluxio administrators can set `alluxio.master.jvm.monitor.enabled=true`
for masters or `alluxio.worker.jvm.monitor.enabled=true` for workers.
They are enabled by default in Alluxio 2.4.0 and newer.
This will trigger a monitoring thread that periodically measures the delay between two GC pauses.
A long delay could indicate that the process is spending significant time garbage collecting,
or performing other JVM safepoint operations.
The following parameters tune the behavior of the monitor thread:

<table class="table table-striped">
<tr><th>Property</th><th>Default</th><th>Description</th></tr>
<tr>
  <td>alluxio.jvm.monitor.warn.threshold</td>
  <td>10sec</td>
  <td>Delay required to log at WARN level</td>
</tr>
<tr>
  <td>alluxio.jvm.monitor.info.threshold</td>
  <td>1sec</td>
  <td>Delay required to log at INFO level</td>
</tr>
<tr>
  <td>alluxio.jvm.monitor.sleep.interval</td>
  <td>1sec</td>
  <td>The time for the JVM monitor thread to sleep</td>
</tr>
</table>

### Improve Cold Read Performance

When applications read directly from the UFS, multiple clients may try to read the same portion
of the input data simultaneously.
For example, at the start of a SparkSQL query, all Spark executors will read the same parquet
footer metadata.
This potentially results in Alluxio caching the same block on every node, which is can
waste both UFS bandwidth and Alluxio storage capacity.

One way to avoid this situation is to apply a deterministic hashing policy by specifying the
following configuration property:

```
alluxio.user.ufs.block.read.location.policy=alluxio.client.block.policy.DeterministicHashPolicy
```

This will cause Alluxio to select a single random worker to read the given block from the UFS
and cause any other clients requesting the same block to instead read from the selected worker.
To increase the number of workers allowed to simultaneously read the same block from the UFS,
update the following configuration property to a value greater than the default of `1`:

```
alluxio.user.ufs.block.read.location.policy.deterministic.hash.shards=3
```

Setting this to 3 means there will be 3 Alluxio workers responsible for reading a particular
UFS block, and all clients will read that UFS block from one of those 3 workers.

### Cache Hit Ratio

In Alluxio versions 2.6 and before, there are several metrics which indicate the number of bytes read through different means. 
Together, they can be used to calculate the hit ratio of alluxio. 

The general formula is 1- `Cluster.BytesReadUfsAll`/ (`Cluster.BytesReadLocal` + `Cluster.BytesReadDomain` + `Cluster.BytesReadRemote`)
The latter part uses the bytes Alluxio reads from the UFS and divide by all the bytes read.
This computes the cache miss rate. 
1 - cache miss rate gives us cache hit rate. 

In Alluxio versions after 2.6, we included an additional metric `Cluster.CacheHitRatio`, which indicates the cache hit ratio. 
Here the cache hit ratio means the percentage of data that is accessed and already in Alluxio storage.
If there is a drop in the hit ratio, consider boosting cache size or examine the recent access pattern to see why data accesses are going to the ufs.


## Master Tuning

### Journal performance tuning
<table class="table table-striped">
<tr><th>Property</th><th>Default</th><th>Description</th></tr>
<tr>
  <td>alluxio.master.journal.flush.batch.time</td>
  <td>5ms</td>
  <td>Time to wait for batching journal writes</td>
</tr>
<tr>
  <td>alluxio.master.journal.flush.timeout</td>
  <td>5min</td>
  <td>The amount of time to retry journal writes before giving up and shutting down the master</td>
</tr>
</table>

Increasing the batch time can improve master throughput for update/write RPCs, but may also
increase the latency for those update/write RPCs.
Setting a larger timeout value for `alluxio.master.journal.flush.timeout` helps keep the master
alive if the journal writing location is unavailable for an extended duration.

### Journal garbage collection

<table class="table table-striped">
<tr><th>Property</th><th>Default</th><th>Description</th></tr>
<tr>
  <td>alluxio.master.journal.checkpoint.period.entries</td>
  <td>2000000</td>
  <td>The number of journal entries to write before creating a new journal checkpoint</td>
</tr>
</table>

Journal checkpoints are expensive to create, but decrease startup time by reducing the number of
journal entries that the master needs to process during startup. If startup is taking too long,
consider reducing this value so that checkpoints happen more often.

### UFS block locations cache

The Alluxio client provides block locations, similar to the HDFS client.
If a file block is not stored in Alluxio, Alluxio will consult the UFS for its block locations,
requiring an additional RPC.
This extra overhead can be avoided by caching the UFS block locations.
The size of this cache is determined by the value of `alluxio.master.ufs.block.location.cache.capacity`.
Caching is disabled if the value is set to `0`.

Increasing the cache size will allow the Alluxio master to store more UFS block locations,
leading to greater metadata throughput for files which are not residing in Alluxio storage; however,
note that increasing this value will result in higher JVM heap utilization.

### UFS Path Cache

When Alluxio mounts a UFS to a path in the Alluxio namespace, the Alluxio master maintains metadata
on its namespace.
The UFS metadata is only pulled when a client accesses a path.
When a client accesses a path which does not exist in Alluxio, Alluxio may consult the UFS to load the UFS metadata.
There are 3 options for loading a missing path: `NEVER`, `ONCE`, `ALWAYS`.

`ALWAYS` will always check the UFS for the latest state of the given path,
`ONCE` will use the default behavior of only scanning each directory once ever, and `NEVER` will never consult the UFS
and thus prevent Alluxio from scanning for new files at all.

The Alluxio master maintains a cache to keep track of which UFS paths have been previously loaded,
to approximate the `ONCE` behavior.
The parameter `alluxio.master.ufs.path.cache.capacity` controls the number of paths to store in the cache.
A larger cache size will consume more memory, but will better approximate the `ONCE` behavior.
The Alluxio master maintains the UFS path cache asynchronously.
Alluxio uses a thread pool to process the paths asynchronously, whose size is controlled by
`alluxio.master.ufs.path.cache.threads`.
Increasing the number of threads can decrease the staleness of the UFS path cache,
but may impact performance by increasing work on the Alluxio master, as well as consuming UFS bandwidth.
If this is set to 0, the cache is disabled and the `ONCE` setting will behave like the `ALWAYS` setting.

### Metadata Sync

If the content on the UFS are modified without going through Alluxio, Alluxio needs to sync its metadata with the UFS to reflect those changes in Alluxio namespace. 
The cost of metadata sync scales linearly with the number of files in the directory that is being synced. 
If metadata sync operation happens frequently on large directories, more threads may be allocated to speed up this process. 
Two configurations are relevant here. 

`alluxio.master.metadata.sync.concurrency.level` controls the concurrency that is used in a single sync operation.
Adjust this to 1x to 2x virtual core count on the master node to speed up the speed of metadata sync.

`alluxio.master.metadata.sync.executor.pool.size` controls the number of threads performing sync operations.
This defaults to the number of virtual cores in the system, but can be adjusted to 2x or 4x number of virtual cores if we expect many concurrent sync operations. 

## Worker Tuning

### Block reading thread pool size

The `alluxio.worker.network.block.reader.threads.max` property configures the maximum number of threads used to
handle block read requests. This value should be increased if you are getting connection refused errors while
reading files.

### Async block caching

When a worker requests for data from a portion of a block, the worker reads as much data as requested
and immediately returns the requested data to the client.
The worker will asynchronously continue to read the remainder of the block without blocking the client request.

The number of asynchronous threads used to finish reading partial blocks is set by the
`alluxio.worker.network.async.cache.manager.threads.max` property.
When large amounts of data are expected to be asynchronously cached concurrently, it may be helpful
to increase this value to handle a higher workload.
This is most commonly effective in cases where the files being cached are relatively small (> 10MB).
However, increase this number sparingly, as it will consume more CPU resources on the worker node
as the number is increased.

Another important property related to async caching is `alluxio.worker.network.async.cache.manager.queue.max`. When a sudden surge of async caching traffic arrives, and Alluxio can no longer hold all the request in the queue, it will start to drop some async caching request, as async caching is strictly an performance optimization.
Increase this if Alluxio drops many async cache requests.
Monitor `Worker.AsyncCacheRequests` and `Worker.AsyncCacheSucceededBlocks` to see if the number of blocks cached matches expectations.

### UFS InStream cache size

Alluxio workers use a pool of open input streams to the UFS controlled by the parameter
`alluxio.worker.ufs.instream.cache.max.size`. A high number reduces the overhead of opening a new
stream to the UFS. However, it also places greater load on the UFS. For HDFS as the UFS, the
parameter should be set based on `dfs.datanode.handler.count`. For instance, if the number of
Alluxio workers matches the the number of HDFS datanodes, set
`alluxio.worker.ufs.instream.cache.max.size=<value of HDFS setting dfs.datanode.handler.count>`
under the assumption that the workload is spread evenly over Alluxio workers.

## Job Service Tuning

### Job Service Capacity
Job service limits the total number of currently running jobs to control its resource usage.
Note that a single CLI command such as distributedLoad can trigger many jobs to be created, one for each file. 
If jobs tend to be created in large batches, consider increasing `alluxio.job.master.job.capacity` to a larger value than the default 100K. 
Job submissions will be rejected if the job service is out of capacity.

There are configurations which control the capacity and parallelism for commands such as `DistributedLoad`.
Please consult the CLI documentation for more details.

### Job Service Throughput
When there are many concurrent jobs running, and a higher throughput is desired, consider increasing `alluxio.job.worker.threadpool.size` configuration. 
This allows each job worker to run with more parallel threads. The drawback is it will compete for resources on the worker machines. 
Recommend 2x virtual core count if most jobs are running in off peak hours only and 1/2 to 1x virtual core count if jobs are running in all hours. 

## Client Tuning

### Passive caching

Passive caching causes an Alluxio worker to cache another copy of data already cached on a
separate worker. Passive caching is disabled by setting the configuration property:
```
alluxio.user.file.passive.cache.enabled=false
```
When enabled, the same data blocks are available across multiple workers,
reducing the amount of available storage capacity for unique data.
Disabling passive caching is important for workloads that have no concept of locality and whose
dataset is large compared to the capacity of a single Alluxio worker.

### Optimized Commits for Compute Frameworks

Running with optimized commits through Alluxio can provide an order of magnitude improvement in the
overall runtime of compute jobs.

Computation frameworks that leverage the Hadoop MapReduce committer pattern (ie. Spark, Hive) are
not optimally designed for interacting with storages that provide slow renames (mainly Object
Stores). This is most common when using stacks such as Spark on S3 or Hive on Ceph.

The Hadoop MapReduce committer leverages renames to commit data from a staging directory (usually
`output/_temporary`) to the final output directory (ie. `output`). When writing data with
`CACHE_THROUGH` or `THROUGH` this protocol translates to the following:
1. Write temporary data to Alluxio and Object Store
    - Data is written to Alluxio storage quickly
    - Data is written to object store slowly
1. Rename temporary data to final output location
    - Rename within Alluxio is fast because it is a metadata operation
    - Rename in object store is slow because it is a copy and delete
1. Job completes to the user

When running jobs which have a large number or size of output files, the overhead of the object
store dominates the run time of the workload.

Alluxio provides a way to only incur the cost of writing the data to Alluxio (fast) on the critical
path. Users should configure the following Alluxio properties in the compute framework:

```properties
# Writes data only to Alluxio before returning a successful write
alluxio.user.file.writetype.default=ASYNC_THROUGH
# Does not persist the data automatically to the underlying storage, this is important because
# only the final committed data is necessary to persist
alluxio.user.file.persistence.initial.wait.time=-1
# Hints that Alluxio should treat renaming as committing data and trigger a persist operation
alluxio.user.file.persist.on.rename=true
# Determines the number of copies in Alluxio when files are not yet persisted, increase this to
# a larger number to ensure fault tolerance in case of Alluxio worker failures
alluxio.user.file.replication.durable=1
# Blacklists persisting files which contain the string "_temporary" anywhere in their path
alluxio.master.persistence.blacklist=_temporary
```

With this configuration, the protocol translates to the following:
1. Write temporary data to Alluxio
    - Data is written to Alluxio storage quickly
1. Rename temporary data to final output location
    - Rename within Alluxio is fast because it is a metadata operation
    - An asynchronous persist task is launched
1. Job completes to the user
1. Asynchronously write final output to object store
    - Data is written to object store slowly

Overall, a copy and delete operation in the object store is avoided, and the slow portion of writing
to the object store is moved off the critical path.

In some cases, the compute framework's commit protocol involves multiple renames or temporary files.
Alluxio provides a mechanism for preventing files from being persisted by blacklisting a set of
strings which are associated with temporary files.
Any file which has any of the configured strings as part of its path will not be considered for
persist.

For example, if

```
alluxio.master.persistence.blacklist=.staging,_temporary
```

Files such as `/data/_temporary/part-00001`, `/data/temporary.staging` will not be considered for
persist.
This works because eventually these temporary files will be deleted or renamed to permanent files.
Because `alluxio.user.file.persist.on.rename=true` is set, the files will be considered for
persistence again when renamed.
Note that persist on rename works for directories as well as files - if a top-level directory is
renamed with the persist on rename option, any files underneath the top-level directory will be
considered for persistence.

## Frequently Seen Performance Issues

This section lists a set of common performance issues and possible reasons and diagnostics steps.
It is a good place to start when you have a performance issue and may lead to an answer quickly if your symptom matches one of those described here. 

### Slow Queries / Overall performance 
Unexpectedly large `Cluster.BytesReadUfs` metric is observed.

When Alluxio is going to UFS for the data, it is sacrificing performance and incurring additional cost. This is usually the biggest red flag. 

Reason:
There are many possible reasons for this. Here is a partial list to check if it is the root cause for your problem.

1. Not enough cache space
   * Check eviction stats
  `Worker.BlocksEvictionRate`
   * Check worker capacity
   Use the command `alluxio fsadmin report capacity`
1. Access pattern is really adversarial
   * Check eviction stats
  `Worker.BlocksEvictionRate`
1. Too many pinned files and directories
   * Check pinned files
1. Too many copies of the same block
   * Reduce `maxReplication` for files
1. Async cache request getting dropped or progressing too slowly
   * Investigate async cache statistics
   * Worker.AsyncCacheRequests vs Worker.AsyncCacheCompleted
    If necessary， increase `alluxio.worker.network.async.cache.manager.threads.max`
    If requests are dropped, increase `alluxio.worker.network.async.cache.manager.queue.max`
1. Unbalanced workers
All of your data access might be going to a small set of workers. Checker worker capacity using either the webui or the `alluxio fsadmin` command. 

### Slow read/write to Alluxio
This is indicated by read/write throughput metrics in Alluxio, or usually reported by the user. 
Reason: 

1. Client rpc thread setting
   * alluxio.user.network.netty.worker.threads
1. Worker rpc thread max
   * alluxio.worker.network.block.reader.threads.max
   * alluxio.worker.network.block.writer.threads.max
These two settings control the concurrency levels of the reader and writer threads. 
   * alluxio.worker.remote.io.slow.threshold  
This setting controls when a remote io is considered slow. If a remote io is slower than this, check the worker log for messages
1. Worker timeout
   * Check client log for any worker timeout and check worker log for any dead worker

### Slow metadata sync
Possible reasons:

1. Synced too often, too many files
   * `alluxio.user.file.metadata.sync.interval` controls how often metadata is synced. Frequent syncing can lead to extra ufs calls and slow down the system performance.
   * Slowness in syncing can also be caused by not enough sync threads
adjust
`alluxio.master.metadata.sync.concurrency.level`
`alluxio.master.metadata.sync.executor.pool.size`
`alluxio.master.metadata.sync.ufs.prefetch.pool.size`

### Slow distributedLoad / distCp / async persist (Job service jobs) 
1. Using `jps` to ensure job master and job worker processes are running
1. `alluxio jobs ls` to see if there are active jobs
1. Check the master log to see if the jobs are triggered
1. Check if we have reached job service capacity, increase 
`alluxio.job.master.job.capacity` if necessary
1. Adjust `alluxio.job.worker.threadpool.size` to increase concurrency (this might affect worker performance)

### OOM of Alluxio processes
1. Alluxio process can get killed by system OOM killer and die silently
 * Check `dmesg -T | egrep -i 'killed process'`
 * This will show which process (if any) got killed by OOM killer
If confirmed OOM issue, start by increasing xmx, directmemory setting of the relevant process
 *  Sometimes the log will show an Out Of Memory exception, this is a Java reported OOM. 
 * This is typically caused by not enough system resources, such as ulimit, thread stack space etc.  

