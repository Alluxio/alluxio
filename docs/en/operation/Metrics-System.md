---
layout: global
title: Metrics System
---


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

When the cluster is running with High Availability, by default the standby masters do not
serve metrics. Set `alluxio.standby.master.metrics.sink.enabled=true`
to have the standby masters also serve metrics.

## Metric Types

Each metric falls into one of the following metric types:

* Gauge: Records a value
* Meter: Measures the rate of events over time (e.g., "requests per minute")
* Counter: Measures the number of times an event occurs
* Timer: Measures both the rate that a particular event is called and the distribution of its duration

For more details about the metric types, please refer to [the metrics library documentation](https://metrics.dropwizard.io/3.1.0/getting-started/).

## Metrics Sink Configuration

A <b>sink</b> specifies where metrics are delivered to.
Each instance can report to zero or more sinks.

* `PrometheusMetricsServlet`: Adds a servlet in Web UI to serve metrics data in Prometheus format.
* `ConsoleSink`: Outputs metrics values to the console.
* `CsvSink`: Exports metrics data to CSV files at regular intervals.
* `JmxSink`: Registers metrics for viewing in a JMX console.
* `GraphiteSink`: Sends metrics to a Graphite server.
* `MetricsServlet`: Adds a servlet in Web UI to serve metrics data as JSON data.

The metrics system is configured via a configuration file that Alluxio expects to be present at `${ALLUXIO_HOME}/conf/metrics.properties`.
A custom file location can be specified via the `alluxio.metrics.conf.file` configuration property.
Refer to `${ALLUXIO_HOME}/conf/metrics.properties.template` for all possible sink specific configurations.
To configure the metrics system on Kubernetes, refer to [Metrics On Kubernetes]({{ '/en/kubernetes/Metrics-On-Kubernetes.html' | relativize_url }})

The Alluxio leading master emits both its instance metrics and a summary of the cluster-wide aggregated metrics.

### Default HTTP JSON Sink

#### Prerequisites

* Alluxio leading master and workers: no prerequisites, enabled by default
* [Alluxio standalone Fuse process]({{ '/en/fuse-sdk/FUSE-SDK-Overview.html' | relativize_url }}):
setting `alluxio.fuse.web.enabled` to `true` in `${ALLUXIO_HOME}/conf/alluxio-site.properties` before launching the standalone Fuse process.

#### Usage

You can send an HTTP request to `/metrics/json/` of the target Alluxio processes to get a snapshot of all metrics in JSON format.

```shell
# Get the metrics in JSON format from Alluxio leading master or workers
$ curl <LEADING_MASTER_HOSTNAME>:<MASTER_WEB_PORT>/metrics/json/
$ curl <WORKER_HOSTNAME>:<WORKER_WEB_PORT>/metrics/json/
```

```shell
# For example, get the local master metrics with its default web port 19999
$ curl 127.0.0.1:19999/metrics/json/
```

```shell
# Get the local worker metrics with its default web port 30000
$ curl 127.0.0.1:30000/metrics/json/
```

```shell
# Get the local job master metrics with its default web port 20002
$ curl 127.0.0.1:20002/metrics/json/
```

```shell
# Get the local job worker metrics with its default web port 30003
$ curl 127.0.0.1:30003/metrics/json/
```

After setting alluxio.fuse.web.enabled=true and launching the standalone Fuse process, get the metrics with its default web port:
```shell
$ curl <FUSE_WEB_HOSTNAME>:<FUSE_WEB_PORT>/metrics/json/
$ curl 127.0.0.1:49999/metrics/json/
```

### Prometheus Sink Setup

[Prometheus](https://prometheus.io/) is a monitoring tool that can help to monitor Alluxio metrics changes.

#### Prerequisites

In the metrics property file, `$ALLUXIO_HOME/conf/metrics.properties` by default, add the following properties:

```properties
# Enable PrometheusMetricsServlet
sink.prometheus.class=alluxio.metrics.sink.PrometheusMetricsServlet
```

If Alluxio is deployed in a cluster, this file needs to be distributed to all the nodes.
Restart the Alluxio servers to activate new configuration changes.

To enable Prometheus Sink Setup in the [Alluxio standalone Fuse process]({{ '/en/fuse-sdk/FUSE-SDK-Overview.html' | relativize_url }}),
setting `alluxio.fuse.web.enabled` to `true` in `${ALLUXIO_HOME}/conf/alluxio-site.properties` before launching the standalone Fuse process.

#### Usage

You can send an HTTP request to `/metrics/prometheus/` of the target Alluxio process to get a snapshot of metrics in Prometheus format.

Get the metrics in Prometheus format from Alluxio leading master or workers or job service or standalone fuse:
```shell
$ curl <LEADING_MASTER_HOSTNAME>:<MASTER_WEB_PORT>/metrics/prometheus/
$ curl <WORKER_HOSTNAME>:<WORKER_WEB_PORT>/metrics/prometheus/
$ curl <LEADING_JOB_MASTER_HOSTNAME>:<JOB_MASTER_WEB_PORT>/metrics/prometheus/
$ curl <JOB_WORKER_HOSTNAME>:<JOB_WORKER_WEB_PORT>/metrics/prometheus/
$ curl <FUSE_WEB_HOSTNAME>:<FUSE_WEB_PORT>/metrics/prometheus/
```

```shell
# For example, get the local master metrics with its default web port 19999
$ curl 127.0.0.1:19999/metrics/prometheus/
```

```shell
# Get the local worker metrics with its default web port 30000
$ curl 127.0.0.1:30000/metrics/prometheus/
```

```shell
# Get the local job master metrics with its default web port 20002
$ curl 127.0.0.1:20002/metrics/prometheus/
```

```shell
# Get the local job worker metrics with its default web port 30003
$ curl 127.0.0.1:30003/metrics/prometheus/
```

```shell
# Get the local standalone Fuse process metrics with its default web port 49999
$ curl 127.0.0.1:49999/metrics/prometheus/
```

You can now direct platforms like Grafana or Datadog to these HTTP endpoints and read the metrics in Prometheus format.

Alternatively, you can configure the Prometheus client using this sample `prometheus.yml` to read the endpoints. This is
recommended for interfacing with Grafana.

```yaml
scrape_configs:
  - job_name: "alluxio master"
      metrics_path: '/metrics/prometheus/'
      static_configs:
      - targets: [ '<LEADING_MASTER_HOSTNAME>:<MASTER_WEB_PORT>' ]
  - job_name: "alluxio worker"
      metrics_path: '/metrics/prometheus/'
      static_configs:
      - targets: [ '<WORKER_HOSTNAME>:<WORKER_WEB_PORT>' ]
  - job_name: "alluxio job master"
      metrics_path: '/metrics/prometheus/'
      static_configs:
      - targets: [ '<LEADING_JOB_MASTER_HOSTNAME>:<JOB_MASTER_WEB_PORT>' ]
  - job_name: "alluxio job worker"
      metrics_path: '/metrics/prometheus/'
      static_configs:
      - targets: [ '<JOB_WORKER_HOSTNAME>:<JOB_WORKER_WEB_PORT>' ]
  - job_name: "alluxio standalone fuse"
      metrics_path: '/metrics/prometheus/'
      static_configs:
      - targets: [ '<FUSE_WEB_HOSTNAME>:<FUSE_WEB_PORT>' ]
```

<b>Be wary when specifying which metrics you want to poll.</b> Prometheus modifies metrics names in order to process them.
It usually replaces `.` with `_`, and sometimes appends text. It is good practice to use the `curl` commands
listed above to see how the names are transformed by Prometheus.

### CSV Sink Setup

This section gives an example of writing collected metrics to CSV files.

First, create the polling directory for `CsvSink` (if it does not already exist):

```shell
$ mkdir /tmp/alluxio-metrics
```

In the metrics property file, `$ALLUXIO_HOME/conf/metrics.properties` by default, add the following properties:

```properties
# Enable CsvSink
sink.csv.class=alluxio.metrics.sink.CsvSink

# Polling period for CsvSink
sink.csv.period=1
sink.csv.unit=seconds

# Polling directory for CsvSink, ensure this directory exists!
sink.csv.directory=/tmp/alluxio-metrics
```

If Alluxio is deployed in a cluster, this file needs to be distributed to all the nodes.
Restart the Alluxio servers to activate the new configuration changes.

After starting Alluxio, the CSV files containing metrics will be found in the `sink.csv.directory`.
The filename will correspond with the metric name.

## Web UI

### Master Web UI Metrics

Besides the raw metrics shown via metrics servlet or custom metrics configuration,
users can track key cluster performance metrics in a more human-readable way in the web interface of
Alluxio leading master (`http://<leading_master_host>:19999/metrics`).

![Master Metrics]({{ '/img/screenshot_generalMetrics.png' | relativize_url }})

The web page includes the following information:
* Timeseries for Alluxio space and root UFS space percentage usage information
* Timeseries for aggregated cluster throughput which is essential for determining the effectiveness of the Alluxio cache
* Cumulative RPC invocations and operations performed by the Alluxio cluster
* Cumulative API calls served per mount point that can serve as a strong metric for quantifying the latency
  and potential cost savings provided by Alluxio's namespace virtualization

The nickname and original metric name corresponding are shown:

<table class="table table-striped">
  <tr>
    <th>Nick Name</th>
    <th>Original Metric Name</th>
  </tr>
  <tr>
    <td markdown="span">Local Alluxio (Domain Socket) Read</td>
    <td markdown="span">`Cluster.BytesReadDomain`</td>
  </tr>
    <tr>
    <td markdown="span">Local Alluxio (Domain Socket) Write</td>
    <td markdown="span">`Cluster.BytesWrittenDomain`</td>
  </tr>
    <tr>
    <td markdown="span">Local Alluxio (Short-circuit) Read</td>
    <td markdown="span">`Cluster.BytesReadLocal`</td>
  </tr>
    <tr>
    <td markdown="span">Local Alluxio (Short-circuit) Write</td>
    <td markdown="span">`Cluster.BytesWrittenLocal`</td>
  </tr>
    <tr>
    <td markdown="span">Remote Alluxio Read</td>
    <td markdown="span">`Cluster.BytesReadRemote`</td>
  </tr>
    <tr>
    <td markdown="span">Remote Alluxio Write</td>
    <td markdown="span">`Cluster.BytesWrittenRemote`</td>
  </tr>
    <tr>
    <td markdown="span">Under Filesystem Read</td>
    <td markdown="span">`Cluster.BytesReadUfsAll`</td>
  </tr>
    <tr>
    <td markdown="span">Under Filesystem Write</td>
    <td markdown="span">`Cluster.BytesWrittenUfsAll`</td>
  </tr>
</table>

Detailed descriptions of those metrics are in [cluster metrics]({{ '/en/reference/Metrics-List.html' | relativize_url }}#cluster-metrics).

`Mounted Under FileSystem Read` shows the `Cluster.BytesReadPerUfs.UFS:<UFS_ADDRESS>` of each Alluxio UFS.
`Mounted Under FileSystem Write` shows the `Cluster.BytesWrittenPerUfs.UFS:<UFS_ADDRESS>` of each Alluxio UFS.

`Logical Operations` and `RPC Invocations` present parts of the [master metrics]({{ '/en/reference/Metrics-List.html' | relativize_url }}#master-metrics).

`Saved Under FileSystem Operations` shows the operations fulfilled by Alluxio's namespace directly
without accessing UFSes. Performance improvement can be significant if the target UFS is remote or slow in response.
Costs can be saved if the underlying storage charges are based on requests.

### Grafana Web UI with Prometheus

Grafana is a metrics analytics and visualization software used for visualizing time series data. You can use Grafana to better visualize the various metrics that Alluxio collects. The software allows users to more easily see changes in memory, storage, and completed operations in Alluxio.

Grafana supports visualizing data from Prometheus. The following steps can help you to build your Alluxio monitoring based on Grafana and Prometheus easily.

1. Install Grafana using the instructions [here](https://grafana.com/docs/grafana/latest/installation/#install-grafana/).
2. [Download](https://grafana.com/grafana/dashboards/13467) the Grafana template JSON file for Alluxio.
3. Import the template JSON file to create a dashboard. See this [example](https://grafana.com/docs/grafana/latest/dashboards/export-import/#importing-a-dashboard) for importing a dashboard.
4. Add the Prometheus data source to Grafana with a custom name, for example, *prometheus-alluxio*. Refer to the [tutorial](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/#add-a-data-source) for help on importing a dashboard.
5. Modify the variables in the dashboard/settings with instructions [here](https://grafana.com/docs/grafana/latest/variables/) and **save** your dashboard.

<table class="table table-striped">
  <tr>
    <th>Variable</th>
    <th>Value</th>
  </tr>
  <tr>
    <td markdown="span">`alluxio_datasource`</td>
    <td markdown="span">Your prometheus datasource name (eg. *prometheus-alluxio* used in step 4)</td>
  </tr>
  <tr>
    <td markdown="span">`masters`</td>
    <td markdown="span">Master 'job_name' configured in `prometheus.yml` (eg. *alluxio master*)</td>
  </tr>
  <tr>
    <td markdown="span">`workers`</td>
    <td markdown="span">Worker 'job_name' configured in `prometheus.yml` (eg. *alluxio worker*)</td>
  </tr>
  <tr>
    <td markdown="span">`alluxio_user`</td>
    <td markdown="span">The user used to start up Alluxio (eg. *alluxio*)</td>
  </tr>
</table>

If your Grafana dashboard appears like the screenshot below, you have built your monitoring successfully. Of course, you can modify the JSON file or just operate on the dashboard to design your monitoring.

![Grafana Web UI]({{ '/img/screenshot_grafana_webui.png' | relativize_url }})

### Datadog Web UI with Prometheus

Datadog is a metrics analytics and visualization software, much like Grafana above. It supports visualizing data from Prometheus.
The following steps can help you to build your Alluxio monitoring based on Datadog and Prometheus easily.

1. Install and run the Datadog agent using the instructions [here](https://docs.datadoghq.com/getting_started/agent/).
2. Modify the `conf.d/openmetrics.d/conf.yaml` file (which you can locate using
   [this resource](https://docs.datadoghq.com/agent/guide/agent-configuration-files/?tab=agentv6v7#agent-configuration-directory)).
   Here is a sample `conf.yaml` file:
   ```yaml
   init_config:
   
   instances:
     - prometheus_url: 'http://<LEADING_MASTER_HOSTNAME>:<MASTER_WEB_PORT>/metrics/prometheus/'
       namespace: 'alluxioMaster'
       metrics: [ "<Master metric 1>", "<Master metric 2>" ]
     - prometheus_url: 'http://<WORKER_HOSTNAME>:<WORKER_WEB_PORT>/metrics/prometheus/'
       namespace: 'alluxioWorker'
       metrics: [ "<Worker metric 1>", "<Worker metric 2>" ]
   ```
3. Restart the Datadog agent (instructions [here](https://docs.datadoghq.com/agent/guide/agent-commands/?tab=agentv6v7#restart-the-agent)).

The metrics emitted by Alluxio should now display on the Datadog web interface.

## JVM metrics

You can get JVM related metrics via `jvm_exporter` as a Java agent.

Download [jmx_prometheus_javaagent-0.16.0.jar](https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.0/jmx_prometheus_javaagent-0.16.0.jar) and run:

```shell
$ java -javaagent:./jmx_prometheus_javaagent-0.16.0.jar=8080:config.yaml -jar yourJar.jar
```

Metrics will now be accessible at http://localhost:8080/metrics.

`config.yaml` file provides the configuration for jmx_exporter. Empty file can be used for a quick start. For more information, please refer to [jmx_exporter documentation](https://github.com/prometheus/jmx_exporter).
