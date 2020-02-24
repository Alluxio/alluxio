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

* Master: The Alluxio master process.
* Worker: The Alluxio worker process.
* Client: Any process with the Alluxio client library.

## Metrics Sink Configuration

A **sink** specifies where metrics are delivered to. 
Each instance can report to zero or more sinks.

* `ConsoleSink`: Outputs metrics values to the console.
* `CsvSink`: Exports metrics data to CSV files at regular intervals.
* `JmxSink`: Registers metrics for viewing in a JMX console.
* `GraphiteSink`: Sends metrics to a Graphite server.
* `MetricsServlet`: Adds a servlet in Web UI to serve metrics data as JSON data.

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
* Meter: Measures the rate of events over time (e.g., "requests per minute")
* Counter: Measures the number of times an event occurs
* Timer: Measures both the rate that a particular event is called and the distribution of its duration

For more details about the metric types, please refer to [the metrics library documentation](https://metrics.dropwizard.io/3.1.0/getting-started/)

## Alluxio Metrics

There are two types of metrics in Alluxio, cluster-wide aggregated metrics, and per-process detailed metrics.

Cluster metrics are collected and calculated by the leading master and displayed in the metrics tab of the web UI. 
These metrics are designed to provide a snapshot of the cluster state and the overall amount of data and metadata served by Alluxio.

Process metrics are collected by each Alluxio process and exposed in a machine-readable format through any configured sinks. 
Process metrics are highly detailed and are intended to be consumed by third-party monitoring tools. 
Users can then view fine-grained dashboards with time-series graphs of each metric, 
such as data transferred or the number of RPC invocations.

Metrics in Alluxio have the following format for master node metrics:

Master.[metricName].[tag1].[tag2]...

Metrics in Alluxio have the following format for non-master node metrics:

[processType].[metricName].[tag1].[tag2]...[hostName]

There is generally an Alluxio metric for every RPC invocation, to Alluxio or to the under store.

Tags are additional pieces of metadata for the metric such as user name or under storage location.
Tags can be used to further filter or aggregate on various characteristics.

### Cluster Metrics

Workers and clients send metrics data to the Alluxio master through heartbeats.
The interval is defined by property `alluxio.master.worker.heartbeat.interval` and `alluxio.user.metrics.heartbeat.interval` respectively.

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

Bytes metrics are aggregated value from workers or clients. Bytes throughput metrics are calculated on the leading master.
The values of bytes throughput metrics equal to bytes metrics counter value divided by the metrics record time and shown as bytes per minute.

### Master Metrics

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

### Worker Metrics

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

### Client Metrics

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

### Process Common Metrics

The following metrics are collected on each instance (Master, Worker or Client).

#### JVM Attributes

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| name | The name of the JVM |
| uptime | The uptime of the JVM |
| vendor | The current JVM vendor |

#### Garbage Collector Statistics

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| PS-MarkSweep.count | Total number of mark and sweep |
| PS-MarkSweep.time | The time used to mark and sweep |
| PS-Scavenge.count | Total number of scavenge |
| PS-Scavenge.time | The time used to scavenge |

#### Memory Usage

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

## Master Web UI Metrics

Besides the raw metrics shown via metrics servlet or custom metrics configuration,
users can view more human-readable metrics stored in the leading master via leading master web UI metrics page.

![Master Metrics]({{ '/img/screenshot_generalMetrics.png' | relativize_url }})

The nick name and original metric name corresponding are shown:
| Nick Name | Original Metric Name |
|-----------------------------------|------------------------------|
| Local Alluxio (Domain Socket) Read | cluster.BytesReadDomain |
| Local Alluxio (Domain Socket) Write | cluster.BytesWrittenDomain |
| Local Alluxio (Short-circuit) Read | cluster.BytesReadLocal |
| Local Alluxio (Short-circuit) Write | cluster.BytesWrittenLocal |
| Remote Alluxio Read | cluster.BytesReadAlluxio |
| Remote Alluxio Write | cluster.BytesWrittenAlluxio |
| Under Filesystem Read | cluster.BytesReadUfsAll | 
| Under Filesystem Write | cluster.BytesWrittenUfsAll |
Detailed descriptions of those metrics are in [cluster metrics section](#cluster-metrics)

`Mounted Under FileSystem Read` shows the `cluster.BytesReadPerUfs.UFS:<UFS_ADDRESS>` of each Alluxio UFS.
`Mounted Under FileSystem Write` shows the `cluster.BytesWrittenPerUfs.UFS:<UFS_ADDRESS>` of each Alluxio UFS.

`Logical Operations` and `RPC Invocations` present [master logical operation metrics](#master-logical-operations-and-results).
