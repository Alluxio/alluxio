---
layout: global
title: Metrics System
nickname: Metrics
group: Operations
priority: 7
---

* Table of Contents
{:toc}

Metrics provide insight into what is going on in the cluster. 
They are an invaluable resource for monitoring and debugging. 
Alluxio has a configurable metrics system based on the [Coda Hale Metrics Library](https://github.com/dropwizard/metrics). 
In the metrics system, sources generate metrics, and sinks consume these metrics. 
The metrics system polls sources periodically and passes metric records to sinks.

Alluxio's metrics are partitioned into different instances corresponding to Alluxio components.
Within each instance, users can configure a set of sinks to which metrics are reported. 
The following instances are currently supported:

* Client: Any process with the Alluxio client library.
* Master: The Alluxio master process.
* Worker: The Alluxio worker process.

A **sink** specifies where metrics are delivered to. 
Each instance can report to zero or more sinks.

* `ConsoleSink`: Outputs metrics values to the console.
* `CsvSink`: Exports metrics data to CSV files at regular intervals.
* `JmxSink`: Registers metrics for viewing in a JMX console.
* `GraphiteSink`: Sends metrics to a Graphite server.
* `MetricsServlet`: Adds a servlet in Web UI to serve metrics data as JSON data.

## Configuration

The metrics system is configured via a configuration file that Alluxio expects to be present at `$ALLUXIO_HOME/conf/metrics.properties`. 
A custom file location can be specified via the `alluxio.metrics.conf.file` configuration property. 
Alluxio provides a `metrics.properties.template` under the `conf` directory which includes all configurable properties 
and guidance of how to specify each property. 

### Default HTTP JSON Sink

By default, `MetricsServlet` is enabled in Alluxio leading master and workers. 

You can send an HTTP request to `/metrics/json/` of the Alluxio leading master to get a snapshot of all metrics in JSON format. 
Metrics on the Alluxio leading master contains its own instance metrics and a summary of the cluster-wide aggregated metrics.

```console
# Get the metrics in JSON format from Alluxio leading master
$ curl <LEADING_MASTER_HOSTNAME>:<MASTER_WEB_PORT>/metrics/json

# For example, get the metrics from master process running locally with default web port
$ curl 127.0.0.1:19999/metrics/json/
```

Send an HTTP request to `/metrics/json/` of the active Alluxio workers to get per-worker metrics.

```console
# Get the metrics in JSON format from an active Alluxio worker
$ curl <WORKER_HOSTNAME>:<WORKER_WEB_PORT>/metrics/json

# For example, get the metrics from worker process running locally with default web port
$ curl 127.0.0.1:30000/metrics/json/
``` 

### Sample CSV Sink Setup

This section gives an example of writing collected metrics to a CSV file.

First, create the polling directory for `CsvSink` (if it does not already exist):
```console
$ mkdir /tmp/alluxio-metrics
```

In the metrics property file, `$ALLUXIO_HOME/conf/metrics.properties` by default, add the following properties:

```
# Enable CsvSink
sink.csv.class=alluxio.metrics.sink.CsvSink

# Polling period for CsvSink
sink.csv.period=1
sink.csv.unit=seconds

# Polling directory for CsvSink, ensure this directory exists!
sink.csv.directory=/tmp/alluxio-metrics
```

If Alluxio is deployed in a cluster, this file needs to be distributed to all the nodes.

After starting Alluxio, the CSV files containing metrics will be found in the `sink.csv.directory`. 
The filename will correspond with the metric name.

Refer to `metrics.properties.template` for all possible sink specific configurations. 

## Metric Types

Each metric falls into one of the following metric types:

* Gauge: Records a value
* Meter: Measures the rate of events over time (e.g., "requests per second")
* Counter: Measures the number of times an event occurs
* Timer: Measures both the rate that a particular event is called and the distribution of its duration

For more details about the metric types, please refer to [the metrics library documentation](https://metrics.dropwizard.io/3.1.0/getting-started/)

## Alluxio Metrics

There are two types of metrics in Alluxio, cluster-wide aggregated metrics, and per-process detailed metrics.

Cluster metrics are collected by the leading master and displayed in the metrics tab of the web UI. 
These metrics are designed to provide a snapshot of the cluster state and the overall amount of data and metadata served by Alluxio.

Process metrics are collected by each Alluxio process and exposed in a machine-readable format through any configured sinks. 
Process metrics are highly detailed and are intended to be consumed by third-party monitoring tools. 
Users can then view fine-grained dashboards with time-series graphs of each metric, 
such as data transferred or the number of RPC invocations.

Metrics in Alluxio have the following format for master node metrics:

master.[metricName].[tag1].[tag2]...

Metrics in Alluxio have the following format for non-master node metrics:

[processType].[hostName].[metricName].[tag1].[tag2]...

There is generally an Alluxio metric for every RPC invocation, to Alluxio or to the under store.

Tags are additional pieces of metadata for the metric such as user name or under storage location.
Tags can be used to further filter or aggregate on various characteristics.

### Cluster Metrics

![Master Metrics]({{ '/img/screenshot_generalMetrics.png' | relativize_url }})

Workers and clients send metrics data to the Alluxio master through heartbeats.
The interval is defined by property `alluxio.master.worker.heartbeat.interval` and `alluxio.user.metrics.heartbeat.interval` respectively.

Each client will be assigned an application id. 
All the metrics sent by this client contain the client application id information. 
By default, this will be in the form of 'app-[random number]'. 
This value can be configured through the property `alluxio.user.app.id`, 
so multiple clients can be combined into a logical application.

* Alluxio cluster information

| Metric Name | Description |
|-------------------------|-----------------------------------------------|
| Master.Workers | Total number of active Alluxio workers in this cluster |

* Alluxio storage capacity

| Metric Name | Description |
|--------------------------------|--------------------------------------------------------------------------|
| Master.CapacityTotal | Total capacity of the Alluxio file system in bytes |
| Master.CapacityTotalTier<TIER> | Total capacity in tier <TIER> of the Alluxio file system in bytes |
| Master.CapacityUsed | Used capacity of the file system in bytes |
| Master.CapacityUsedTier<TIER> | Used capacity in tier <TIER> of the Alluxio file system in bytes |
| Master.CapacityFree | Free capacity of the Alluxio file system in bytes |
| Master.CapacityFreeTier<TIER> | Free capacity in tier <TIER> of the Alluxio file system in bytes |

* Under storage capacity

| Metric Name | Description |
|-------------------------|--------------------------------------------------|
| Master.UfsCapacityTotal | Total capacity of the under file system in bytes |
| Master.UfsCapacityUsed | Used capacity of the under file system in bytes |
| Master.UfsCapacityFree | Free capacity of the under file system in bytes |

* Total amount of data transferred through Alluxio and I/O throughput estimates (meter statistics)

| Metric Name | Description |
|--------------------------------------|---------------------------------------------------------------------------|
| cluster.BytesReadAlluxio | Total number of bytes read from Alluxio storage. This does not include UFS reads |
| cluster.BytesReadAlluxioThroughput | Bytes read throughput from Alluxio storage |
| cluster.BytesReadDomain | Total number of bytes read from Alluxio storage via domain socket |
| cluster.BytesReadDomainThroughput | Bytes read throughput from Alluxio storage via domain socket |
| cluster.BytesReadLocal | Total number of bytes read from local filesystem |
| cluster.BytesReadLocalThroughput | Bytes read throughput from local filesystem |
| cluster.BytesWrittenAlluxio | Total number of bytes written to Alluxio storage. This does not include UFS writes |
| cluster.BytesWrittenAlluxioThroughput | Bytes write throughput to Alluxio storage |
| cluster.BytesWrittenDomain | Total number of bytes written to Alluxio storage via domain socket |
| cluster.BytesWrittenDomainThroughput | Throughput of bytes written to Alluxio storage via domain socket |

* I/O to under storages

| Metric Name | Description |
|-----------------------------------|---------------------------------------------|
| cluster.BytesReadUfsAll | Total number of bytes read from all Alluxio UFSes |
| cluster.BytesReadUfsThroughput | Bytes read throughput from all Alluxio UFSes |
| cluster.BytesWrittenUfsAll | Total number of bytes written to all Alluxio UFSes | 
| cluster.BytesWrittenUfsThroughput | Bytes write throughput to all Alluxio UFSes |

* Under storage RPCs

For all th UFS RPCs (e.g. create file, delete file, get file status), 
the timer metrics of each RPC will be recorded as well as the failure counters if any.

For example: `cluster.UfsOp<RPC_NAME>.UFS:<UFS_ADDRESS>` records the number of UFS operation <RPC_NAME> ran on UFS <UFS_ADDRESS>

### Master Metrics

* Master summary information

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| Master.TotalPaths | Total number of files and directory in Alluxio namespace |
| Master.UfsSessionCount-Ufs:<UFS_ADDRESS> | The total number of currently opened UFS sessions to connect to the given <UFS_ADDRESS> |

* Master Logical operations and results

| Metric Name | Description |
|---------------------------|-----------------------------------------------------|
| Master.CreateFileOps | Total number of the CreateFile operations |
| Master.FilesCreated | Total number of the succeed CreateFile operations |
| Master.CompleteFileOps | Total number of the CompleteFile operations |
| Master.FilesCompleted | Total number of the succeed CompleteFile operations |
| Master.GetFileInfoOps | Total number of the GetFileInfo operations |
| Master.FileInfosGot | Total number of the succeed GetFileInfo operations |
| Master.GetFileBlockInfoOps | Total number of GetFileBlockInfo operations |
| Master.FileBlockInfosGot | Total number of succeed GetFileBlockInfo operations |
| Master.FreeFileOps | Total number of FreeFile operations |
| Master.FilesFreed | Total number of succeed FreeFile operations |
| Master.FilesPersisted | Total number of successfully persisted files |
| Master.FilesPinned | Total number of currently pinned files |
| Master.CreateDirectoryOps | Total number of the CreateDirectory operations |
| Master.DirectoriesCreated | Total number of the succeed CreateDirectory operations |
| Master.DeletePathOps | Total number of the Delete operations |
| Master.PathsDeleted | Total number of the succeed Delete operations |
| Master.GetNewBlockOps | Total number of the GetNewBlock operations |
| Master.NewBlocksGot | Total number of the succeed GetNewBlock operations |
| Master.MountOps | Total number of Mount operations |
| Master.PathsMounted | Total number of succeed Mount operations |
| Master.UnmountOps | Total number of Unmount operations |
| Master.PathsUnmounted | Total number of succeed Unmount operations |
| Master.RenamePathOps | Total number of Rename operations |
| Master.PathsRenamed | Total number of succeed Rename operations |
| Master.SetAclOps | Total number of SetAcl operations |
| Master.SetAttributeOps | Total number of SetAttribute operations |

All the Alluxio filesystem client operations come with a retry mechanism 
where master metrics record how many retries an operation has (in the format of `Master.<RPC_NAME>Retries`) and 
how many failures an operation runs into (in the format of `Master.<RPC_NAME>Failures`).

* Master timer metrics

| Metric Name | Description |
|----------------------------|-----------------------------------------------------|
| Master.blockHeartbeat.User | The duration statistics of BlockHeartbeat operations |
| Master.ConnectFromMaster.UFS:<UFS_ADDRESS>.UFS_TYPE:<UFS_TYPE> | The duration statistics of connecting from master to UFS <UFS_ADDRESS> |
| Master.GetSpace.UFS:<UFS_ADDRESS>.UFS_TYPE:<UFS_TYPE> | The duration statistics of getting space of UFS <UFS_ADDRESS> |
| Master.getConfigHash | The duration statistics of getting hashes of cluster and path level configuration |
| Master.getConfiguration | The duration statistics of getting cluster level and path level configuration |
| Master.getPinnedFileIds | The duration statistics of getting the ids of pinned files 
| Master.getWorkerId | The duration statistics of getting worker id |
| Master.registerWorker | The duration statistics of registering worker to master |

* Other Master metrics

| Metric Name | Description |
|---------------------------------|-----------------------------------------------------|
| Master.LastBackupEntriesCount | The total number of entries written in last leading master metadata backup |
| Master.BackupEntriesProcessTime | The process time of the last backup |
| Master.LastBackupRestoreCount | The total number of entries restored from backup when a leading master initializes its metadata |
| Master.BackupRestoreProcessTime | The process time of the last restore from backup |

### Worker Metrics

| Metric Name | Description |
|---------------------------------------------|-----------------------------------------------------|
| Worker.<WORKER_HOSTNAME>.CapacityTotal | Total capacity of this worker in bytes
| Worker.<WORKER_HOSTNAME>.CapacityUsed | Used capacity of this worker in bytes
| Worker.<WORKER_HOSTNAME>.CapacityFree | Free capacity of this worker in bytes
| Worker.<WORKER_HOSTNAME>.BlocksCached | Total number of blocks in Alluxio worker storages |
| Worker.<WORKER_HOSTNAME>.BlocksAccessed | Total number of times blocks in this worker are accessed |
| Worker.<WORKER_HOSTNAME>.BlocksCanceled | Total number of aborted temporary blocks |
| Worker.<WORKER_HOSTNAME>.BlocksDeleted | Total number of deleted blocks |
| Worker.<WORKER_HOSTNAME>.BlocksEvicted | Total number of blocks removed by this worker |
| Worker.<WORKER_HOSTNAME>.BlocksLost | Total number of lost blocks |
| Worker.<WORKER_HOSTNAME>.BlocksPromoted | Total number of blocks moved by clients from one location to another in this worker |
| Worker.<WORKER_HOSTNAME>.AsyncCacheRequests | Total number of async cache requests |
| Worker.<WORKER_HOSTNAME>.AsyncCacheDuplicateRequests | Total number of duplicate requests of caching the same block asynchronously |
| Worker.<WORKER_HOSTNAME>.AsyncCacheSucceededBlocks | Total number of blocks succeed in async cache |
| Worker.<WORKER_HOSTNAME>.AsyncCacheFailedBlocks | Total number of blocks failed to be cached asynchronously |
| Worker.<WORKER_HOSTNAME>.AsyncCacheUfsBlocks | Total number of async cache blocks which have local source |
| Worker.<WORKER_HOSTNAME>.AsyncCacheRemoteBlocks | Total number of remote blocks to async cache locally |

### Process Common Metrics

The following metrics are collected on each instance (Master, Worker or Client).

* JVM attributes

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| name | The name of the JVM |
| uptime | The uptime of the JVM |
| vendor | The current JVM vendor |

* Garbage collector statistics

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| PS-MarkSweep.count | Total number of mark and sweep |
| PS-MarkSweep.time | The time used to mark and sweep |
| PS-Scavenge.count | Total number of scavenge |
| PS-Scavenge.time | The time used to scavenge |

* Memory usage

Alluxio provides overall and detailed memory usage information.
Detailed memory usage information of code cache, compressed class space, metaspace, PS Eden space, PS old gen, and PS survivor space
is collected in each process.

A subset of the memory usage metrics are listed as following:

| Metric Name | Description |
|------------------------------|-----------------------------------------------------|
| total.committed | The amount of memory in bytes that is guaranteed to be available for use by the JVM |
| total.init | The amount of the memory in bytes that is available for use by the JVM |
| total.max | The maximum amount of memory in bytes that is available for use by the JVM |
| total.used | The amount of memory currently used in bytes |
| heap.committed | The amount of memory from heap area guaranteed to be available |
| heap.init | The amount of memory from heap area available at initialization |
| heap.max | The maximum amount of memory from heap area that is available |
| heap.usage | The amount of memory from heap area currently used in GB|
| heap.used | The amount of memory from heap area that has been used |
| pools.Code-Cache.used | Used memory of collection usage from the pool from which memory is used for compilation and storage of native code |
| pools.Compressed-Class-Space.used | Used memory of collection usage from the pool from which memory is use for class metadata |
| pools.PS-Eden-Space.used | Used memory of collection usage from the pool from which memory is initially allocated for most objects |
| pools.PS-Survivor-Space.used | Used memory of collection usage from the pool containing objects that have survived the garbage collection of the Eden space |
