---
layout: global
title: Performance Tuning
nickname: Performance Tuning
group: Advanced
priority: 0
---

* Table of Contents
{:toc}

This document goes over all the knobs that can be turned to tune Alluxio performance.

## Common Performance Issues

If you aren't seeing the performance you expect, follow this checklist to fix common issues.

1. Are all nodes working?

   We should first check out if the Alluxio cluster is healthy. One can check the webUI at 
   `http://MasterHost:19999` to find out if masters and workers are working correctly from a brouswer. 
   Alternatively, one can run `bin/alluxio fsadmin report` to collect similar information from the console.
   Important metrics to check out include the number of lost workers, last heartbeat time and etc to see
   if any masters or workers are out of service.
1. Are short-circuit operations working?

   If your compute application is running co-located with your Alluxio workers, make sure the
   application is performing short-circuit reads and writes with its local Alluxio worker. To
   check this, monitor the `cluster.BytesReadAlluxioThroughput` and `cluster.BytesReadAlluxio` while the
   application is running. If `cluster.BytesReadLocalThroughput` is zero or much lower than
   `cluster.BytesReadAlluxioThroughput`, the compute applications are using remote Alluxio workers
   instead of their local Alluxio workers. The Alluxio client uses hostname matching to look for
   a local Alluxio worker, so make sure client and worker use the same hostname string. To set the
   client-side hostname, configure `alluxio.user.hostname` on the client. To set the worker-side
   hostname, configure `alluxio.worker.hostname` on the worker.
1. Is data is well-distributed across Alluxio workers?

   By default, Alluxio clients will use the `LocalFirstPolicy` to write data to their local
   Alluxio worker. This is efficient for applications which write data from many nodes concurrently.
   However, if all data is being written from a single node, you could end up with one full worker
   and the rest of your workers empty. See [this doc][1] for discussion of the different location
   policies and how to configure them.
1. Do you see warnings or errors in the Master/Worker logs about threadpool exhaustion?

   The master uses a threadpool for serving client requests. Alluxio clients cache master
   connections to avoid the overhead of creating a new connection each time. Until a client shuts down, it will
   hold on to dedicated server threads, even when the client isn't sending requests. To increase the size of
   the master threadpool, increase `alluxio.master.worker.threads.max` (defaults to 1/3 of the system's max
   file descriptor limit). You may need to increase your file descriptor limit. To decrease the number of
   threads used per client, decrease `alluxio.user.file.master.client.threads` (10 by default) and
   `alluxio.user.block.master.client.threads` (10 by default).
1. Do you see worker-side error messages containing "Connection reset by peer"?

   This could indicate that the client is timing out when communicating with the Alluxio worker. To
   increase the timeout, adjust `alluxio.user.network.netty.timeout` (30 seconds by default).
   Timeouts can happen due to having a slow under store. If timeouts are happening on writes,
   you can adjust `alluxio.user.network.netty.writer.close.timeout` (5 minutes by default). Increase this value if you
   are writing large files to object storages with a slow network connection. On closing a file for an
   object store, the entire object is uploaded during the close timeout.
1. Do you see frequent JVM GC?

   Frequent and long GC operations on master or worker JVMs can cause the process very slow. If you suspect
   the performance degraded due to heavy GC load, you can verify your hypothesis by adding  
   `ALLUXIO_JAVA_OPTS=" -XX:+PrintGCDetails -XX:+PrintTenuringDistribution -XX:+PrintGCTimestamps"`
   to `conf/alluxio-env.sh`, start Alluxio service and check the output in `logs/master.out` for masters and 
   `logs/worker.out` for workers.

If you're still looking for solutions, check out the [metrics system][2] to better understand how
your jobs are performing. The remainder of this doc discusses various knobs that can be turned to
help tune Alluxio to your needs.

[1]: {{ site.baseurl }}{% link en/api/FS-API.md %}#location-policy
[2]: {{ site.baseurl }}{% link en/operation/Metrics-System.md %}

## General Tuning

### JVM Monitoring

To detect long GC pauses, Alluxio administrators can set
"`alluxio.master.jvm.monitor.enabled=true`"
for masters or "`alluxio.worker.jvm.monitor.enabled=true`"
for workers. When this is activated, Alluxio will run a monitoring thread which periodically runs
Thread.sleep and tests the delay between when it was supposed to wake up and when it actually woke
up. A long delay indicates that GC could be taking a long time. If the delay exceeds a certain
threshold, the thread will print diagnostic information such memory usage and garbage collection
counts. These parameters further control the behavior of the monitor thread:

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

When the application cold-reads from UFS, multiple clients may try to read the same part of the
input data at about the same time (e.g., to start a SparkSQL query, all Spark executors may read the
same paquet header); as a result, the same block may be cached on every node. This can be a waste of
UFS bandwidth as all workers are reading the same data from UFS, as well as the Alluxio storage
capacity is not effectively used as excessive copies of the same data may evict other valuable
cached data., and may lead to much worse performance when the connection UFS is slow and Alluxio
space is almost full.

This performance degradation is due to less-optimized UFS read schedule which is termed as the
“thundering-herd" problem in Alluxio. One way to resolve this problem is to apply deterministic
hashing. By specifying "`alluxio.user.ufs.block.read.location.policy=DeterministicHashPolicy`",
Alluxio will select only one “random" worker to read the given block from UFS. As a result, even
multiple Alluxio clients are requesting the same block from different nodes, only that “selected"
worker will read from UFS and cache only on that worker. Sometimes we want more parallelism, then
one can also change the value of
"`alluxio.user.ufs.block.read.location.policy.deterministic.hash.shards`" from 1 (default) to N so
there will be no more than N workers working at the same time to cold-read and cache this block.
Note that, for different block even in the same file, they will select different "N" workers and
thus achieve balanced load across the cluster.

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
  <td>The amount of time to keep retrying journal writes
before giving up and shutting down the master</td>
</tr>
</table>

Increasing `alluxio.master.journal.flush.batch.time` can improve metadata throughput, but reduce
metadata latency. To keep master alive across long journal downtimes (e.g. HDFS crashing), set
`alluxio.master.journal.flush.timeout` to a large value

### Journal garbage collection

<table class="table table-striped">
<tr><th>Property</th><th>Default</th><th>Description</th></tr>
<tr>
  <td>alluxio.master.journal.checkpoint.period.entries</td>
  <td>2000000</td>
  <td>The number
of journal entries to write before creating a new journal checkpoint</td>
</tr>
</table>

Journal checkpoints are expensive to create, but decrease startup time by reducing the number of
journal entries that the master needs to process during startup. If startup is taking too long,
consider reducing this value so that checkpoints happen more often.

### UFS block locations cache

Alluxio provides block locations, similar to the HDFS client. If a file block is not in Alluxio
storage yet, the Alluxio will still return the UFS block locations. Therefore, if a file block is
not in Alluxio storage, the Alluxio master will have to consult the UFS for the UFS block locations,
which would require an additional RPC to the UFS. This extra overhead may be expensive, so the
Alluxio master maintains a cache to store these UFS block locations. The parameter
`alluxio.master.ufs.block.location.cache.capacity` controls the size of this cache. If it is set to 0,
the cache is disabled.

Increasing the cache size will allow the Alluxio master to store more UFS block locations, and be
able to return those locations without consulting the UFS, and this may lead to greater metadata
throughput, primarily for files which are not resident in Alluxio storage.

### UFS Path Cache

When Alluxio mounts a UFS to a path in the Alluxio namespace, the Alluxio master maintains metadata
about the UFS namespace. The UFS metadata is pulled into the Alluxio master on demand, when a client
accesses a path. When a client accesses a path which does not currently exist in Alluxio, Alluxio
may consult the UFS to load the UFS metadata. There are 3 options for loading a missing path: `Never`,
`Once`, `Always`. "`Never`" will never consult the UFS, and "`Always`" will always interact with the UFS,
but the "Once" behavior is approximated in the Alluxio master. The Alluxio master maintains a cache
to approximate which UFS paths have been previously loaded, to approximate the "`Once`" behavior. The
parameter `alluxio.master.ufs.path.cache.capacity` controls the number of paths to store in the cache.
A larger cache size will consume more memory, but will better approximate the "Once" behavior.

Additionally, the Alluxio master maintains the UFS path cache asynchronously, in order to avoid
synchronous interactions with the UFS. Alluxio uses a thread pool to process the paths
asynchronously, and the size of the thread pool is controlled by
`alluxio.master.ufs.path.cache.threads`. Increasing the number of threads can decrease the staleness
of the UFS path cache, but may impact performance by increasing work on the Alluxio master, as well
as increasing the parallel interactions with the UFS. If this is set to 0, the cache is disabled,
and the "`Once`" setting will behave like the "`Always`" setting.

## Worker Tuning

### Block thread pool size

Property `alluxio.worker.block.threads.max` configures the maximum number of incoming RPC requests to block
worker that can be handled. This value is used to configure maximum number of threads in Thrift
thread pool with block worker. This value should be greater than the sum of
`alluxio.user.block.worker.client.threads` across concurrent Alluxio clients. Otherwise, the worker
connection pool can be drained, preventing new connections from being established.

### Async caching

Since Alluxio 1.7.0 improves the performance of cold-reading a block partially or non-sequentially
with the default read type. Previously clients used a flag to force full reads of blocks in order to
store them into Alluxio to speed up follow up reads on the same blocks. Now these reads for caching
purpose will be handled by the Alluxio in the background, vastly decreasing the latency of partial
read requests in many workloads. For example, reading the first 10MB of a 512MB block with partial
caching on required a read of the entire block (512MB); now the client reads 10MB and continues
processing while an Alluxio worker loads the 512MB block in the background.

A tunable configuration for this feature is the
`alluxio.worker.network.netty.async.cache.manager.threads.max` property. This determines the maximum
number of concurrent blocks a worker will attempt to cache. By default the value is high (512), but
in cases where large amounts of data are expected to be asynchronously cached concurrently, lowering
the value can reduce resource contention (e.g., # of cores)

### Netty Configs

`alluxio.worker.network.netty.rpc.threads.max` - Default value is 2048, consider increasing this value
if you have a very large number of concurrent client accesses and are seeing threadpool exhausted
errors on the worker.

### Async eviction and watermarks

There are two modes of eviction in Alluxio, asynchronous and synchronous (default). When the
workload are write-heavy, it’s recommended to use the asynchronous eviction. Asynchronous eviction
relies on a periodic space reserver thread in each worker to evict data. It waits until the worker
storage utilization reaches a configurable high watermark. Then it evicts data based on the eviction
policy until it reaches the configurable low watermark. For example, if we had the same
16+100+100=216GB storage configured, we can set eviction to kick in at around 200GB and stop at
around 160GB:

<table class="table table-striped">
<tr><th>Property</th><th>Value</th><th>Result</th></tr>
<tr>
  <td>alluxio.worker.tieredstore.level0.watermark.high.ratio</td>
  <td>0.9</td>
  <td>~ 200GB (216GB * 0.9)</td>
</tr>
<tr>
  <td>alluxio.worker.tieredstore.level0.watermark.low.ratio</td>
  <td>0.75</td>
  <td>~ 160GB (216GB * 0.75)</td>
</tr>
</table>

It’s recommended to set the high watermark below a threshold to keep an absolute size (e.g. 1~ 5 GB)
as buffering. And keep low and high watermarks not too close to each other, otherwise Async eviction
will be triggered frequently when space utilization gets close to the high watermark.

## Client Tuning

### Client threads

 In cases where there are a large number of concurrent clients, these cached clients can take
up resources on the master which will degrade performance by causing resource contention. It is best
to consider the maximum number of connections accepted by the master and set the thread pools of the
clients and master accordingly. For example, if you expect 100 concurrent clients each with up to 10
cached master clients, then you will want to have a master max connection thread pool of at least
1000.

On the client side, these are the configurations to tune: alluxio.user.block.master.client.threads -
Default is 10, set this number lower and/or the corresponding master configuration higher if you are
running into resource contention on the master. alluxio.user.file.master.client.threads - Default is
10, set this number lower and/or the corresponding master configuration higher if you are running
into resource contention on the master.

### Loading UFS Metadata

Once a UFS path is mounted to Alluxio, Alluxio maintains metadata on the UFS namespace. However,
since interacting with UFS may be costly, the UFS metadata information is pulled into the Alluxio
master on demand. This is performed whenever a user lists a directory a lists a file. If a user
accesses a particular path, and that path does not exist in Alluxio, Alluxio may then decide to
interact with the UFS to get that information. There are 3 ways to configure how to pull in the
information on demand: "Always", "Never", and "Once". This configurable by the user, via the user
configuration parameter, alluxio.user.file.metadata.load.type, which can be set to "Always",
"Never", or "Once". If it is set to "Never", Alluxio will never consult the UFS for a missing
Alluxio path. If it is set to "Always", Alluxio will always interact with the UFS for a missing
Alluxio path. For "Once", Alluxio will only load it from the UFS the first time, and afterwards,
will not consult the UFS. This is approximated via a cache that the Alluxio Master maintains. This
user defined configuration parameter only affects how Alluxio loads UFS metadata for paths which do
not exist in Alluxio already. If a user access an existing Alluxio path, Alluxio will never consult
the UFS, regardless of this configuration setting.

Setting this configuration value is dependent on the workloads. "Once" is a reasonable default
value, since it will load any path at least once, but not access the UFS for every single access.
This can be commonly used for when once a file is loaded into Alluxio, it is unlikely that the file
will be removed from the corresponding UFS. "Never" can be used for scenarios when all writes and
reads go only to Alluxio, and there are no direct writes to UFS. In this scenario, the Alluxio
metadata will always know about the metadata, so it will never have to consult the UFS. "Always"
should be used in scenarios when files a continually being directly added to UFS (not via Alluxio).
In this case, Alluxio can always check the UFS to see if there are new files available. Since more
frequent loading of metadata is costly with respect to performance, it is recommended to keep the
setting as infrequent as possible.

Compared with the UFS sync feature, loading metadata only discovers new files from the UFS. It does
not modify any updated metadata of files, such as updated timestamps, or deleted files.

### Syncing UFS Metadata

Loading UFS metadata is important for discovering new files from the UFS, but it does not handle any
modifications to existing UFS files. Therefore, Alluxio enables syncing the UFS metadata to
Alluxio's metadata. To sync the metadata for a particular path, the Alluxio master will query the
UFS for the same path, and then update the corresponding Alluxio metadata to match the UFS
information. Users can optionally configure Alluxio to "sync" metadata with the UFS when running any
Alluxio command. The user defined configuration value `alluxio.user.file.metadata.sync.interval`
controls how often Alluxio operations should sync with the UFS. This configuration parameter defines
the interval of time between multiple UFS synchronizations. A value of -1 means no syncing will be
performed. 0 means Alluxio will always sync with UFS for every Alluxio operation. Any positive time
interval defines the interval in which Alluxio will not re-sync the specified path. For example,
using a value of "5m" means Alluxio will sync a particular path at most every 5 minutes.

Since syncing the UFS metadata is an expensive operation, it recommended to not sync very often.
However, there may be scenarios when it is desirable to do so. A value of -1 will never sync, so
that is appropriate for when all writes go to Alluxio, and not bypassed to UFS. In this scenario,
Alluxio will always know about the metadata, so interacting with UFS is not necessary. A value of 0
will perform UFS sync for every single operation, and this is appropriate for when there are many
files created or updated directly in the UFS (bypassing Alluxio). In this scenario, Alluxio will
always query the latest state of the path from the UFS. Any time interval value is useful for to
limit the frequency of UFS syncing. Larger values will perform UFS sync less, but the chances will
be higher for the Alluxio metadata to be stale. Interacting with the UFS incurs performance
overheads, so it is recommended to limit frequent UFS syncing.

Since the UFS sync feature includes the loading metadata functionality, if UFS syncing is performed,
the loading metadata configuration parameter, `alluxio.user.file.metadata.load.type`, is ignored. In
comparison to the loading metadata feature, UFS sync takes care of any UFS modifications, like
deleting files, or updating the timestamp, in addition to discovering new files.

### Passive caching

Passive caching causes data to be re-cached in an Alluxio worker when one or more copies already
exist in Alluxio storage. Passive caching should be disabled
(`alluxio.user.file.passive.cache.enabled=false`) when the workloads being run have no locality
concept and the dataset being accessed is large compared to the Alluxio storage of one worker. If
passive caching is enabled, you may see data blocks residing in many workers, possibly all of them,
greatly reducing the amount of Alluxio storage available for unique data.
