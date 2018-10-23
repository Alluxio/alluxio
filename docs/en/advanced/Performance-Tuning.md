---
layout: global
title: Performance Tuning
nickname: Performance Tuning
group: Advanced
priority: 0
---

* Table of Contents
{:toc}

This document goes over various knobs that can be used to tune Alluxio performance.

## Common Performance Issues

The following is a checklist to run through to address common problems when tuning performance:

1. Are all nodes working?

   Check that the Alluxio cluster is healthy. You can check the web user interface at 
   `http://MasterHost:19999` to see if the masters and workers are working correctly from a browser. 
   Alternatively, you can run `bin/alluxio fsadmin report` to collect similar information from the console.
   Important metrics to verify if any nodes are out of service are the number of lost workers and
   the last heartbeat time.

1. Are short-circuit operations working?

   If the compute application is running co-located with Alluxio workers, check that the
   application is performing short-circuit reads and writes with its local Alluxio worker.
   Monitor the values for `cluster.BytesReadAlluxioThroughput` and `cluster.BytesReadLocalThroughput`
   while the application is running.
   If the local throughput is zero or significantly lower than the total throughput,
   the compute application is likely not interfacing with a local Alluxio worker.
   The Alluxio client uses hostname matching to discover a local Alluxio worker;
   check that the client and worker use the same hostname string. 
   Configuring `alluxio.user.hostname` and `alluxio.worker.hostname` sets the client and worker
   hostnames respectively.

1. Is data is well-distributed across Alluxio workers?

   By default, Alluxio clients will use the `LocalFirstPolicy` to write data to their local
   Alluxio worker. This is efficient for applications which write data from many nodes concurrently.
   In a scenario where all data is written from a single node, its local worker will be filled,
   leaving the remaining workers empty.
   See [this doc][1] for discussion of the different location policies and how to configure them.

1. Are there warnings or errors in the master or worker logs related to thread pool exhaustion?

   Alluxio clients maintain a connection to the master to avoid using a new connection each time.
   Until a client shuts down, it will occupy a server thread even if it is not sending requests.
   This may deplete the master's thread pool; its size can be increased by setting
   `alluxio.master.worker.threads.max`, which defaults to 1/3 of the system's max file descriptor limit.
   The file descriptor limit may also need to be increased to allow the desired number of open connections.
   The default number of threads used by a client can be decreased by setting
   `alluxio.user.file.master.client.threads` and `alluxio.user.block.master.client.threads`,
   both of which have a default value of `10`.

1. Are there error messages containing "Connection reset by peer" in the worker logs?

   This could indicate that the client is timing out when communicating with the Alluxio worker.
   To increase the timeout, configure `alluxio.user.network.netty.timeout`, which has a default of `30s`.
    
   If write operations are timing out, configure `alluxio.user.network.netty.writer.close.timeout`,
   which has a default of `5m`. This is especially important when writing large files to object stores
   with a slow network connection. The entire object is uploaded at once upon closing the file.

1. Are there frequent JVM GC events?

   Frequent and long GC operations on master or worker JVMs drastically slow down the process.
   This can be identified by adding logging for GC events; append the following to `conf/allulxio-env.sh`:
   
```
ALLUXIO_JAVA_OPTS=" -XX:+PrintGCDetails -XX:+PrintTenuringDistribution -XX:+PrintGCTimestamps"
```

   Restart the Alluxio service and check the output in `logs/master.out` or `logs/worker.out`
   for masters and workers respectively.

Also check out the [metrics system][2] for better insight in how jobs are performing.

[1]: {{ site.baseurl }}{% link en/api/FS-API.md %}#location-policy
[2]: {{ site.baseurl }}{% link en/operation/Metrics-System.md %}

## General Tuning

### JVM Monitoring

To detect long GC pauses, Alluxio administrators can set `alluxio.master.jvm.monitor.enabled=true`
for masters or `alluxio.worker.jvm.monitor.enabled=true` for workers.
This will trigger a monitoring thread that periodically measures the delay between two GC pauses.
A long delay could indicate that the process is spending significant time garbage collecting.
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

When the application reads directly from the UFS, multiple clients may try to read the same portion
of the input data simultaneously. For example, at the start of a SparkSQL query, all Spark executors
will read the same parquet header. This results in Alluxio caching the same block on every node,
which is potentially a waste of both UFS bandwidth and Alluxio storage capacity.

One way to avoid this situation is to apply a deterministic hashing policy by specifying the
following configuration property:

```
alluxio.user.ufs.block.read.location.policy=DeterministicHashPolicy
```

This will cause Alluxio to select a single random worker to read the given block from the UFS
and cause any other worker requesting the same block to instead read from the selected worker.
To increase the number of workers allowed to simultaneously read the same block from the UFS,
update the following configuration property to a value greater than the default of `1`:
```
alluxio.user.ufs.block.read.location.policy.deterministic.hash.shards=3
```

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

Increasing the batch time can improve metadata throughput but reduce metadata latency.
Setting a larger timeout value helps keep the master alive if the journal source is unavailable for
an extended duration.

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

Alluxio provides block locations, similar to the HDFS client.
If a file block is stored in Alluxio, Alluxio will consult the UFS for its block locations,
requiring an additional RPC.
This extra overhead can be avoided by caching the UFS block locations.
The size of this cache is determined by the value of `alluxio.master.ufs.block.location.cache.capacity`.
Caching is disabled if the value is set to `0`.

Increasing the cache size will allow the Alluxio master to store more UFS block locations,
leading to greater metadata throughput for files which are not residing in Alluxio storage.

### UFS Path Cache

When Alluxio mounts a UFS to a path in the Alluxio namespace, the Alluxio master maintains metadata
on its namespace.
The UFS metadata is only pulled when a client accesses a path.
When a client accesses a path which does not exist in Alluxio, Alluxio may consult the UFS to load the UFS metadata.
There are 3 options for loading a missing path: `Never`, `Once`, `Always`.

`ALWAYS` will always check the UFS for the latest state of the given path,
`ONCE` will use the default behavior of only scanning each directory once ever, and `NEVER` will never consult the UFS
and thus prevent Alluxio from scanning for new files at all.

The Alluxio master maintains a cache to approximate which UFS paths have been previously loaded, to approximate the `Once` behavior.
The parameter `alluxio.master.ufs.path.cache.capacity` controls the number of paths to store in the cache.
A larger cache size will consume more memory, but will better approximate the `Once` behavior.
The Alluxio master maintains the UFS path cache asynchronously.
Alluxio uses a thread pool to process the paths asynchronously, whose size is controlled by
`alluxio.master.ufs.path.cache.threads`.
Increasing the number of threads can decrease the staleness of the UFS path cache,
but may impact performance by increasing work on the Alluxio master, as well as consuming UFS bandwidth.
If this is set to 0, the cache is disabled and the `Once` setting will behave like the `Always` setting.

## Worker Tuning

### Block thread pool size

The `alluxio.worker.block.threads.max` property configures the maximum number of incoming RPC requests to 
worker that can be handled. 
This value is used to configure maximum number of threads in Thrift thread pool of the block worker.
This value should be greater than the sum of `alluxio.user.block.worker.client.threads` across concurrent Alluxio clients.
Otherwise, the worker connection pool can be drained, preventing new connections from being established.

### Async block caching

When a worker requests for data from a portion of a block, the worker reads as much data as requested
and immediately returns the requested data to the client.
The worker will asynchronously continue to read the remainder of the block without blocking the client request.

The number of asynchronous threads used to finish reading partial blocks is set by the
`alluxio.worker.network.netty.async.cache.manager.threads.max` property, with a default value of `512`.
When large amounts of data are expected to be asynchronously cached concurrently, it may be helpful
to reduce this value to reduce resource contention.

### Netty Configs

The number of RPC threads available on a worker is configured by
`alluxio.worker.network.netty.rpc.threads.max`, with a default value of `2048`.
This value should be increased if exhausted thread pool errors are being logged on workers.

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
