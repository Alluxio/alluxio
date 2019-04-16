---
layout: global
title: Scalability Tuning
nickname: Scalability Tuning
group: Advanced
priority: 0
---

Alluxio is a scalable distributed file system designed to handle many workers within a single cluster.
Several parameters can be tuned to prevent the Alluxio master from being overloaded.
This page details the parameters to tune when scaling a cluster.

* Table of Contents
{:toc}

## Alluxio Master Configuration

### Heap Size

The Alluxio master heap size controls the total number of files that can fit into the master memory.
The following JVM options, set in `alluxio-env.sh`, determine the respective maximum heap sizes for the
Alluxio master and standby master processes to `256 GB`:

```properties
ALLUXIO_MASTER_JAVA_OPTS+=" -Xms128g -Xmx256g "
ALLUXIO_SECONDARY_MASTER_JAVA_OPTS+=" -Xms128g -Xmx256g "
```

* As a rule of thumb set the min heap size to half the max heap size.
* Each thread spawned by the master JVM requires off heap space determined by the thread stack size.
When setting the heap size, ensure that there is enough memory allocated for off heap storage.
For example, spawning `4000` threads with a default thread stack size of `1 MB` requires at least
`4 GB` of off-heap space available.

### Operating System Limits

An exception message like `java.lang.OutOfMemoryError: unable to create new native thread`
indicates that operating system limits may need tuning.

Several parameters limit the number of threads that a process can spawn:
- `kernel.pid_max`: Run `sysctl -w kernel.pid_max=<new value>`
- `vm.max_map_count`: Run command `sysctl -w vm.max_map_count=<new value>`
- Max user process limit: `ulimit -u`
- Max open files limit: `ulimit -n`

These limits are often set for the particular user that launch the Alluxio process.
As a rule of thumb, `vm.max_map_count` should be at least twice the limit for master threads
as set by `alluxio.master.worker.threads.max`.

### Heartbeat Intervals and Timeouts

The frequency in which the master checks for lost workers is set by the
`alluxio.master.heartbeat.interval.ms` property, with a default value of `60s`.
Increase the interval to reduce the number of heartbeat checks.

## Alluxio Worker Configuration

### Heartbeat Intervals and Timeouts

The frequency in which a worker checks in with the master is set by the following properties:
```properties
alluxio.worker.block.heartbeat.interval=60s
alluxio.worker.filesystem.heartbeat.interval=60s
```
`alluxio.worker.block.heartbeat.interval` controls the heartbeat intervals for the block service in Alluxio and
`alluxio.worker.filesystem.heartbeat.interval` for the filesystem service.
Again, increase the interval to reduce the number of heartbeat checks.

### Keepalive Time and Timeout

Alluxio workers are configured to check the health of connected clients by sending keepalive pings.
This is controlled by the following properties
```properties
alluxio.worker.network.keepalive.time=30s
alluxio.worker.network.keepalive.timeout=30s
```
`alluxio.worker.network.keepalive.time` controls the maximum wait time since a client sent the last message before
worker issues a keepalive request. `alluxio.worker.network.keepalive.timeout` controls the maximum wait time after a
keepalive request is sent before the worker determines the client is no longer alive and closes the connection.

## Alluxio Client Configuration

### RPC Retry Interval

The following properties tune RPC retry intervals:

```properties
alluxio.user.rpc.retry.max.duration=2min
alluxio.user.rpc.retry.base.sleep=1s
```

The retry duration and sleep duration should be increased if frequent timeouts are observed
when a client attempts to communicate with the Alluxio master.

### Keepalive Time and Timeout

The Alluxio client can also be configured to check the health of connected workers using keepalive pings.
This is controlled by the following properties
```properties
alluxio.user.network.keepalive.time=2h
alluxio.user.network.keepalive.timeout=30s
```
`alluxio.user.network.keepalive.time` controls the maximum wait time since a worker sent the last message before client
issues a keepalive request. `alluxio.user.network.keepalive.timeout` controls the maximum wait time after a keepalive
request is sent before the client determines the worker is no longer alive and closes the connection. This
is disabled by default (the default value for `alluxio.user.network.keepalive.time` is `Long.MAX_VALUE` which
effectively disables the keepalive) to minimize unintended performance impact to workers. You might want to enable it if
you find that the Alluxio client is waiting a long time on dead workers. To enable it, set the property
`alluxio.user.network.keepalive.time` to a desired interval.
