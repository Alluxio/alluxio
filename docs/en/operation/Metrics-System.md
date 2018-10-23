---
layout: global
title: Metrics System
nickname: Metrics
group: Operations
priority: 0
---

* Table of Contents
{:toc}

Metrics provide insight into what is going on in the cluster. They are an invaluable resource for
monitoring and debugging. Alluxio has a configurable metrics system based on the [Coda Hale Metrics
Library](https://github.com/dropwizard/metrics). In the metrics system, sources generate metrics,
and sinks consume these metrics. The metrics system polls sources periodically and passes
metric records to sinks.

Alluxio's metrics are partitioned into different instances corresponding to Alluxio components.
Within each instance, users can configure a set of sinks to which metrics are reported. The
following instances are currently supported:

* Client: Any process with the Alluxio client library.
* Master: The Alluxio master process.
* Worker: The Alluxio worker process.

Each instance can report to zero or more sinks.

* ConsoleSink: Outputs metrics values to the console.
* CsvSink: Exports metrics data to CSV files at regular intervals.
* JmxSink: Registers metrics for viewing in a JMX console.
* GraphiteSink: Sends metrics to a Graphite server.
* MetricsServlet: Adds a servlet in Web UI to serve metrics data as JSON data.

## Configuration

The metrics system is configured via a configuration file that Alluxio expects to be present at
`$ALLUXIO_HOME/conf/metrics.properties`. A custom file location can be specified via the
`alluxio.metrics.conf.file` configuration property. Alluxio provides a `metrics.properties.template`
under the `conf` directory which includes all configurable properties. By default, MetricsServlet
is enabled in Alluxio master and workers. You can send an HTTP request to "/metrics/json" to get a
snapshot of all metrics in JSON format.

## Sample Sink Setup

This section gives an example of writing collected metrics to a CSV file.

In the metrics property file, `$ALLUXIO_HOME/conf/metrics.properties` by default, add the following
properties.

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

Then, start Alluxio, CSV files containing metrics will be found in the `sink.csv.directory`. The
file name will correspond with the metric name.

## Supported Metrics

There are two types of metrics in Alluxio, cluster-wide aggregated metrics, and per process detailed
metrics.

### Cluster Metrics

Cluster metrics are collected by the master and displayed in the metrics tab of the web UI. These
metrics are designed to provide a snapshot of the cluster state and the overall amount of data and
metadata served by Alluxio.

![Master Metrics]({{ site.baseurl }}{% link img/screenshot_generalMetrics.png %})

Clients and workers send metrics data to the Alluxio master tagged with an application id. By
default this will be in the form of 'app-[random number]'. This value can be configured through the
property `alluxio.user.app.id`, so multiple processes can be combined into a logical application.

Cluster metrics include:
* Alluxio storage capacity
* Under storage capacity
* Total amount of data transferred through Alluxio
* I/O throughput estimates
* Cache hit rate
* I/O to under storages
* Master Logical operations and RPCs
* Under storage RPCs

### Process Metrics

Process metrics are collected by each Alluxio process and exposed in a machine readable format
through any configured sinks. Process metrics are highly detailed and are intended to be consumed
by third-party monitoring tools. Users can then view fine grained dashboards with time series graphs
of each metric, such as data transferred or number of rpc invocations.

Metrics in Alluxio have the following format for master node metrics:

master.[metricName].[tag1].[tag2]...

Metrics in Alluxio have the following format for non-master node metrics:

[processType].[hostName].[metricName].[tag1].[tag2]...

The list of process metrics exposed by the master or workers can be found at the `/metrics/json`
endpoint of the web UI. There is generally an Alluxio metric for every RPC invocation, to Alluxio or
to the under store.

Tags are additional pieces of metadata for the metric such as user name or under storage location.
Tags can be used to further filter or aggregate on various characteristics.
