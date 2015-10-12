---
layout: global
title: Metrics System
group: Features
---

* Table of Contents
{:toc}

Metrics provide insight to what is going on in the cluster. They are an invaluable resource for
monitoring the services and debugging problems. Tachyon has a configurable metrics system based
on the [Coda Hale Metrics Library](https://github.com/dropwizard/metrics). In the metrics system,
metrics sources are where the metrics generated, and metrics sinks consume the records generated
by the metrics sources. The metrics system would poll the metrics sources periodically and pass
the metrics records to the metrics sinks.

Tachyon's metrics are decoupled into different instances corresponding to Tachyon components.
Within each instance, user can configure a set of sinks to which metrics are reported. The
following instances are currently supported:

* Master: The Tachyon standalone master process.
* Worker: The Tachyon standalone worker process.

Each instance can report to zero or more sinks.

* ConsoleSink: Outputs metrics values to the console.
* CsvSink: Exports metrics data to CSV files at regular intervals.
* JmxSink: Registers metrics for viewing in a JMX console.
* GraphiteSink: Sends metrics to a Graphite server.
* MetricsServlet: Adds a servlet in Web UI to serve metrics data as JSON data.

Some metrics like `BytesReadLocal` rely on data collected from client heartbeat. To get the
accurate metrics data, the client is expected to properly close the `TachyonFS` client after
using it.

# Configuration
The metrics system is configured via a configuration file that Tachyon expects to be present at
`$TACHYON_HOME/conf/metrics.properties`. A custom file location can be specified via the
`tachyon.metrics.conf.file` configuration property. Tachyon provides a metrics.properties.template
under the conf directory which includes all configurable properties. By default, MetricsServlet
is enabled and you can send HTTP request "/metrics/json" to get a snapshot of all the registered
metrics in JSON format.


# Supported Metrics
The following shows the details of the available metrics.

### Master

* CapacityTotal: Total capacity of the file system in byte.
* CapacityUsed: Used capacity of the file system in byte.
* CapacityFree: Free capacity of the file system in byte.
* UnderFsCapacityTotal: Total capacity of the under file system in byte.
* UnderFsCapacityUsed: Used capacity of the under file system in byte.
* UnderFsCapacityFree: Free capacity of the under file system in byte.
* Workers: Number of the workers.
* FilesTotal: Total number of files and directories in the file system.
* FilesCreated: Total number of files and directories created.
* CreateFileOps: Total number of the CreateFile operation.
* FilesDeleted: Total number of files and directories deleted.
* DeleteFileOps: Total number of the DeleteFile operation.
* FilesRenamed: Total number of the Rename operation.
* RenameFileOps: Total number of the RenameFile operation.
* FilesPinned: Total number of the files pinned.
* FilesCheckpointed: Total number of the files checkpointed.
* GetFileStatusOps: Total number of the getClientFileInfo operation.

### Worker

* CapacityTotal: Total capacity of the worker in byte.
* CapacityUsed: Used capacity of the worker in byte.
* CapacityFree: Free capacity of the worker in byte.
* BlocksAccessed: Total number of the accessBlock operation.
* BlocksCached: Total number of blocks cached.
* BlocksCanceled: Total number of blocks canceled when being written.
* BlocksDeleted: Total number of blocks deleted.
* BlocksEvicted: Total number of blocks evicted.
* BlocksPromoted: Total number of blocks promoted.
* BlocksReadLocal: Total number of blocks read locally from the worker.
* BlocksReadRemote: Total number of blocks read remotely from the worker.
* BlocksWrittenLocal: Total number of blocks written to the worker locally.
* BytesReadLocal: Total number of bytes read locally from the worker.
* BytesReadRemote: Total number of bytes read remotely from the worker.
* BytesReadUfs: Total number of bytes read from under file system on the worker.
* BytesWrittenLocal: Total number of bytes written to the worker locally.
* BytesWrittenUfs: Total number of bytes written to under file system on the worker.
