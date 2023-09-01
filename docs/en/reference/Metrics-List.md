---
layout: global
title: List of Metrics
---


There are two types of metrics in Alluxio, cluster-wide aggregated metrics, and per-process detailed metrics.

* Cluster metrics are collected and calculated by the leading master and displayed in the metrics tab of the web UI. 
  These metrics are designed to provide a snapshot of the cluster state and the overall amount of data and metadata served by Alluxio.

* Process metrics are collected by each Alluxio process and exposed in a machine-readable format through any configured sinks.
  Process metrics are highly detailed and are intended to be consumed by third-party monitoring tools.
  Users can then view fine-grained dashboards with time-series graphs of each metric,
  such as data transferred or the number of RPC invocations.

Metrics in Alluxio have the following format for master node metrics:

```
Master.[metricName].[tag1].[tag2]...
```

Metrics in Alluxio have the following format for non-master node metrics:

```
[processType].[metricName].[tag1].[tag2]...[hostName]
```

There is generally an Alluxio metric for every RPC invocation, to Alluxio or to the under store.

Tags are additional pieces of metadata for the metric such as user name or under storage location.
Tags can be used to further filter or aggregate on various characteristics.

## Cluster Metrics

Workers and clients send metrics data to the Alluxio master through heartbeats.
The interval is defined by property `alluxio.master.worker.heartbeat.interval` and `alluxio.user.metrics.heartbeat.interval` respectively.

Bytes metrics are aggregated value from workers or clients. Bytes throughput metrics are calculated on the leading master.
The values of bytes throughput metrics equal to bytes metrics counter value divided by the metrics record time and shown as bytes per minute.

<table class="table table-striped">
<tr><th style="width:35%">Name</th><th style="width:15%">Type</th><th style="width:50%">Description</th></tr>
{% for item in site.data.generated.cluster-metrics %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.metricName }}"></a> `{{ item.metricName }}`</td>
    <td markdown="span">{{ item.metricType }}</td>
    <td markdown="span">{{ site.data.generated.en.cluster-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## Process Metrics

Metrics shared by the all Alluxio server and client processes.

<table class="table table-striped">
<tr><th style="width:35%">Name</th><th style="width:15%">Type</th><th style="width:50%">Description</th></tr>
{% for item in site.data.generated.process-metrics %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.metricName }}"></a> `{{ item.metricName }}`</td>
    <td markdown="span">{{ item.metricType }}</td>
    <td markdown="span">{{ site.data.generated.en.process-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## Server Metrics

Metrics shared by the Alluxio server processes.

<table class="table table-striped">
<tr><th style="width:35%">Name</th><th style="width:15%">Type</th><th style="width:50%">Description</th></tr>
{% for item in site.data.generated.server-metrics %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.metricName }}"></a> `{{ item.metricName }}`</td>
    <td markdown="span">{{ item.metricType }}</td>
    <td markdown="span">{{ site.data.generated.en.server-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## Master Metrics

### Default Master Metrics

<table class="table table-striped">
<tr><th style="width:35%">Name</th><th style="width:15%">Type</th><th style="width:50%">Description</th></tr>
{% for item in site.data.generated.master-metrics %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.metricName }}"></a> `{{ item.metricName }}`</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.generated.en.master-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

### Dynamically Generated Master Metrics

<table class="table table-striped">
  <tr>
    <th>Metric Name</th>
    <th>Description</th>
  </tr>
  <tr>
    <td markdown="span">`Master.CapacityTotalTier{TIER_NAME}`</td>
    <td markdown="span">Total capacity in tier `{TIER_NAME}` of the Alluxio file system in bytes</td>
  </tr>
  <tr>
    <td markdown="span">`Master.CapacityUsedTier{TIER_NAME}`</td>
    <td markdown="span">Used capacity in tier `{TIER_NAME}` of the Alluxio file system in bytes</td>
  </tr>
  <tr>
    <td markdown="span">`Master.CapacityFreeTier{TIER_NAME}`</td>
    <td markdown="span">Free capacity in tier `{TIER_NAME}` of the Alluxio file system in bytes</td>
  </tr>
  <tr>
    <td markdown="span">`Master.UfsSessionCount-Ufs:{UFS_ADDRESS}`</td>
    <td markdown="span">The total number of currently opened UFS sessions to connect to the given `{UFS_ADDRESS}`</td>
  </tr>
  <tr>
    <td markdown="span">`Master.{UFS_RPC_NAME}.UFS:{UFS_ADDRESS}.UFS_TYPE:{UFS_TYPE}.User:{USER}`</td>
    <td markdown="span">The details UFS rpc operation done by the current master</td>
  </tr>
  <tr>
    <td markdown="span">`Master.PerUfsOp{UFS_RPC_NAME}.UFS:{UFS_ADDRESS}`</td>
    <td markdown="span">The aggregated number of UFS operation `{UFS_RPC_NAME}` ran on UFS `{UFS_ADDRESS}` by leading master</td>
  </tr>
  <tr>
    <td markdown="span">`Master.{LEADING_MASTER_RPC_NAME}`</td>
    <td markdown="span">The duration statistics of RPC calls exposed on leading master</td>
  </tr>
</table>

## Worker Metrics

### Default Worker Metrics

<table class="table table-striped">
<tr><th style="width:35%">Name</th><th style="width:15%">Type</th><th style="width:50%">Description</th></tr>
{% for item in site.data.generated.worker-metrics %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.metricName }}"></a> `{{ item.metricName }}`</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.generated.en.worker-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

### Dynamically Generated Worker Metrics

<table class="table table-striped">
  <tr>
    <th style="width:35%">Metric Name</th>
    <th style="width:65%">Description</th>
  </tr>
  <tr>
    <td markdown="span">`Worker.UfsSessionCount-Ufs:{UFS_ADDRESS}`</td>
    <td markdown="span">The total number of currently opened UFS sessions to connect to the given `{UFS_ADDRESS}`</td>
  </tr>
  <tr>
    <td markdown="span">`Worker.{RPC_NAME}`</td>
    <td markdown="span">The duration statistics of RPC calls exposed on workers</td>
  </tr>
</table>

## Client Metrics

Each client metric will be recorded with its local hostname or `alluxio.user.app.id` is configured.
If `alluxio.user.app.id` is configured, multiple clients can be combined into a logical application.

<table class="table table-striped">
<tr><th style="width:35%">Name</th><th style="width:15%">Type</th><th style="width:50%">Description</th></tr>
{% for item in site.data.generated.client-metrics %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.metricName }}"></a> `{{ item.metricName }}`</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.generated.en.client-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## Fuse Metrics

Fuse is a long-running Alluxio client. 
Depending on the launching ways, Fuse metrics show as
* client metrics when Fuse client is launching in a standalone AlluxioFuse process.
* worker metrics when Fuse client is embedded in the AlluxioWorker process.

Fuse metrics includes:

<table class="table table-striped">
<tr><th style="width:35%">Name</th><th style="width:15%">Type</th><th style="width:50%">Description</th></tr>
{% for item in site.data.generated.fuse-metrics %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.metricName }}"></a> `{{ item.metricName }}`</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.generated.en.fuse-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

Fuse reading/writing file count can be used as the indicators for Fuse application pressure.
If a large amount of concurrent read/write occur in a short period of time, each of the read/write operations may take longer time to finish.

When a user or an application runs a filesystem command under Fuse mount point, 
this command will be processed and translated by operating system which will trigger the related Fuse operations
exposed in [AlluxioFuse](https://github.com/Alluxio/alluxio/blob/db01aae966849e88d342a71609ab3d910215afeb/integration/fuse/src/main/java/alluxio/fuse/AlluxioJniFuseFileSystem.java).
The count of how many times each operation is called, and the duration of each call will be recorded with metrics name `Fuse.<FUSE_OPERATION_NAME>` dynamically.

The important Fuse metrics include:

<table class="table table-striped">
  <tr>
    <th style="width:35%">Metric Name</th>
    <th style="width:65%">Description</th>
  </tr>
  <tr>
    <td markdown="span">`Fuse.readdir`</td>
    <td markdown="span">The duration metrics of listing a directory</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.getattr`</td>
    <td markdown="span">The duration metrics of getting the metadata of a file</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.open`</td>
    <td markdown="span">The duration metrics of opening a file for read or overwrite</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.read`</td>
    <td markdown="span">The duration metrics of reading a part of a file</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.create`</td>
    <td markdown="span">The duration metrics of creating a file for write</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.write`</td>
    <td markdown="span">The duration metrics of writing a file</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.release`</td>
    <td markdown="span">The duration metrics of closing a file after read or write. Note that release is async so fuse threads will not wait for release to finish</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.mkdir`</td>
    <td markdown="span">The duration metrics of creating a directory</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.unlink`</td>
    <td markdown="span">The duration metrics of removing a file or a directory</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.rename`</td>
    <td markdown="span">The duration metrics of renaming a file or a directory</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.chmod`</td>
    <td markdown="span">The duration metrics of modifying the mode of a file or a directory</td>
  </tr>
  <tr>
    <td markdown="span">`Fuse.chown`</td>
    <td markdown="span">The duration metrics of modifying the user and/or group ownership of a file or a directory</td>
  </tr>
</table>

Fuse related metrics include:
* `Client.TotalRPCClients` shows the total number of RPC clients exist that is using to or can be used to connect to master or worker for operations.
* Worker metrics with `Direct` keyword. When Fuse is embedded in worker process, it can go through worker internal API to read from / write to this worker.
The related metrics are ended with `Direct`. For example, `Worker.BytesReadDirect` shows how many bytes are served by this worker to its embedded Fuse client for read.
* If `alluxio.user.block.read.metrics.enabled=true` is configured, `Client.BlockReadChunkRemote` will be recorded. 
This metric shows the duration statistics of reading data from remote workers via gRPC.

`Client.TotalRPCClients` and `Fuse.TotalCalls` metrics are good indicator of the current load of the Fuse applications.
If applications (e.g. Tensorflow) are running on top of Alluxio Fuse but these two metrics show a much lower value than before,
the training job may be stuck with Alluxio.

## Process Common Metrics

The following metrics are collected on each instance (Master, Worker or Client).

### JVM Attributes

<table class="table table-striped">
  <tr>
    <th style="width:35%">Metric Name</th>
    <th style="width:65%">Description</th>
  </tr>
  <tr>
    <td markdown="span">`name`</td>
    <td markdown="span">The name of the JVM</td>
  </tr>
  <tr>
    <td markdown="span">`uptime`</td>
    <td markdown="span">The uptime of the JVM</td>
  </tr>
  <tr>
    <td markdown="span">`vendor`</td>
    <td markdown="span">The current JVM vendor</td>
  </tr>
</table>

### Garbage Collector Statistics

<table class="table table-striped">
  <tr>
    <th style="width:35%">Metric Name</th>
    <th style="width:65%">Description</th>
  </tr>
  <tr>
    <td markdown="span">`PS-MarkSweep.count`</td>
    <td markdown="span">Total number of mark and sweep</td>
  </tr>
  <tr>
    <td markdown="span">`PS-MarkSweep.time`</td>
    <td markdown="span">The time used to mark and sweep</td>
  </tr>
  <tr>
    <td markdown="span">`PS-Scavenge.count`</td>
    <td markdown="span">Total number of scavenge</td>
  </tr>
  <tr>
    <td markdown="span">`PS-Scavenge.time`</td>
    <td markdown="span">The time used to scavenge</td>
  </tr>
</table>

### Memory Usage

Alluxio provides overall and detailed memory usage information.
Detailed memory usage information of code cache, compressed class space, metaspace, PS Eden space, PS old gen, and PS survivor space
is collected in each process.

A subset of the memory usage metrics are listed as following:

<table class="table table-striped">
  <tr>
    <th style="width:35%">Metric Name</th>
    <th style="width:65%">Description</th>
  </tr>
  <tr>
    <td markdown="span">`total.committed`</td>
    <td markdown="span">The amount of memory in bytes that is guaranteed to be available for use by the JVM</td>
  </tr>
  <tr>
    <td markdown="span">`total.init`</td>
    <td markdown="span">The amount of the memory in bytes that is available for use by the JVM</td>
  </tr>
  <tr>
    <td markdown="span">`total.max`</td>
    <td markdown="span">The maximum amount of memory in bytes that is available for use by the JVM</td>
  </tr>
  <tr>
    <td markdown="span">`total.used`</td>
    <td markdown="span">The amount of memory currently used in bytes</td>
  </tr>
  <tr>
    <td markdown="span">`heap.committed`</td>
    <td markdown="span">The amount of memory from heap area guaranteed to be available</td>
  </tr>
  <tr>
    <td markdown="span">`heap.init`</td>
    <td markdown="span">The amount of memory from heap area available at initialization</td>
  </tr>
  <tr>
    <td markdown="span">`heap.max`</td>
    <td markdown="span">The maximum amount of memory from heap area that is available</td>
  </tr>
  <tr>
    <td markdown="span">`heap.usage`</td>
    <td markdown="span">The amount of memory from heap area currently used in GB</td>
  </tr>
  <tr>
    <td markdown="span">`heap.used`</td>
    <td markdown="span">The amount of memory from heap area that has been used</td>
  </tr>
  <tr>
    <td markdown="span">`pools.Code-Cache.used`</td>
    <td markdown="span">Used memory of collection usage from the pool from which memory is used for compilation and storage of native code</td>
  </tr>
  <tr>
    <td markdown="span">`pools.Compressed-Class-Space.used`</td>
    <td markdown="span">Used memory of collection usage from the pool from which memory is use for class metadata</td>
  </tr>
  <tr>
    <td markdown="span">`pools.PS-Eden-Space.used`</td>
    <td markdown="span">Used memory of collection usage from the pool from which memory is initially allocated for most objects</td>
  </tr>
  <tr>
    <td markdown="span">`pools.PS-Survivor-Space.used`</td>
    <td markdown="span">Used memory of collection usage from the pool containing objects that have survived the garbage collection of the Eden space</td>
  </tr>
</table>

### ClassLoading Statistics

<table class="table table-striped">
  <tr>
    <th style="width:35%">Metric Name</th>
    <th style="width:65%">Description</th>
  </tr>
  <tr>
    <td markdown="span">`loaded`</td>
    <td markdown="span">The total number of classes loaded</td>
  </tr>
  <tr>
    <td markdown="span">`unloaded`</td>
    <td markdown="span">The total number of unloaded classes</td>
  </tr>
</table>

### Thread Statistics

<table class="table table-striped">
  <tr>
    <th style="width:35%">Metric Name</th>
    <th style="width:65%">Description</th>
  </tr>
  <tr>
    <td markdown="span">`count`</td>
    <td markdown="span">The current number of live threads</td>
  </tr>
  <tr>
    <td markdown="span">`daemon.count`</td>
    <td markdown="span">The current number of live daemon threads</td>
  </tr>
  <tr>
    <td markdown="span">`peak.count`</td>
    <td markdown="span">The peak live thread count</td>
  </tr>
  <tr>
    <td markdown="span">`total_started.count`</td>
    <td markdown="span">The total number of threads started</td>
  </tr>
  <tr>
    <td markdown="span">`deadlock.count`</td>
    <td markdown="span">The number of deadlocked threads</td>
  </tr>
  <tr>
    <td markdown="span">`deadlock`</td>
    <td markdown="span">The call stack of each thread related deadlock</td>
  </tr>
  <tr>
    <td markdown="span">`new.count`</td>
    <td markdown="span">The number of threads with new state</td>
  </tr>
  <tr>
    <td markdown="span">`blocked.count`</td>
    <td markdown="span">The number of threads with blocked state</td>
  </tr>
  <tr>
    <td markdown="span">`runnable.count`</td>
    <td markdown="span">The number of threads with runnable state</td>
  </tr>
  <tr>
    <td markdown="span">`terminated.count`</td>
    <td markdown="span">The number of threads with terminated state</td>
  </tr>
  <tr>
    <td markdown="span">`timed_waiting.count`</td>
    <td markdown="span">The number of threads with timed_waiting state</td>
  </tr>
</table>
