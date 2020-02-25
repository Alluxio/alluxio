---
layout: global
title: List of Metrics
group: Reference
priority: 1
---

* Table of Contents
{:toc}

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
<tr><th>Name</th><th>Type</th><th>Description</th></tr>
{% for item in site.data.table.cluster-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.en.cluster-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## Master Metrics

Default master metrics:

<table class="table table-striped">
<tr><th>Name</th><th>Type</th><th>Description</th></tr>
{% for item in site.data.table.master-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.en.master-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

Dynamically generated master metrics:

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| Master.CapacityTotalTier<TIER_NAME> | Total capacity in tier <TIER_NAME> of the Alluxio file system in bytes |
| Master.CapacityUsedTier<TIER_NAME>  | Used capacity in tier <TIER_NAME> of the Alluxio file system in bytes |
| Master.CapacityFreeTier<TIER_NAME>  | Free capacity in tier <TIER_NAME> of the Alluxio file system in bytes |
| Master.UfsSessionCount-Ufs:<UFS_ADDRESS> | The total number of currently opened UFS sessions to connect to the given <UFS_ADDRESS> |
| Master.<UFS_RPC_NAME>.UFS:<UFS_ADDRESS>.UFS_TYPE:<UFS_TYPE>.User:<USER> | The details UFS rpc operation done by the current master |
| Master.PerUfsOp<UFS_RPC_NAME>.UFS:<UFS_ADDRESS> | The aggregated number of UFS operation <UFS_RPC_NAME> ran on UFS <UFS_ADDRESS> by leading master |  
| Master.<LEADING_MASTER_RPC_NAME> | The duration statistics of RPC calls exposed on leading master |

## Worker Metrics

Default master metrics:

<table class="table table-striped">
<tr><th>Name</th><th>Type</th><th>Description</th></tr>
{% for item in site.data.table.worker-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.en.worker-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

Dynamically generated master metrics:

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| Worker.UfsSessionCount-Ufs:<UFS_ADDRESS> | The total number of currently opened UFS sessions to connect to the given <UFS_ADDRESS> |
| Worker.<RPC_NAME>                        | The duration statistics of RPC calls exposed on workers |

## Client Metrics

Each client metric will be recorded with its local hostname or `alluxio.user.app.id` is configured.
If `alluxio.user.app.id` is configured, multiple clients can be combined into a logical application.

<table class="table table-striped">
<tr><th>Name</th><th>Type</th><th>Description</th></tr>
{% for item in site.data.table.client-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.en.client-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## Process Common Metrics

The following metrics are collected on each instance (Master, Worker or Client).

### JVM Attributes

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| name | The name of the JVM |
| uptime | The uptime of the JVM |
| vendor | The current JVM vendor |

### Garbage Collector Statistics

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| PS-MarkSweep.count | Total number of mark and sweep |
| PS-MarkSweep.time | The time used to mark and sweep |
| PS-Scavenge.count | Total number of scavenge |
| PS-Scavenge.time | The time used to scavenge |

### Memory Usage

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
