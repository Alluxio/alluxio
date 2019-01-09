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
For example, spawning `50000` threads with a default thread stack size of `1MB` requires at least
`50 GB` of off-heap space available.

### Thread Pool Size

An executor pool is used on the master to handle concurrent client requests. If you expect a large
number of concurrent clients communicating with the master, tune the thread pool size by modifying
the following properties. The actual thread count depends on the maximum concurrency expected on the
cluster. For example, if spawning `10000` concurrent tasks (clients) each with a client thread
pool size of `4`, set the master thread pool max to greater than `40000`.

```properties
alluxio.master.worker.threads.max=51200
alluxio.master.worker.threads.min=25600
```

* You may need to set OS limits, as defined in the following section, to allow the above number of
threads to be spawned.
* Check that the amount of off heap storage available allows for the count thread.

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
The first one controls the heartbeat intervals for block service in Alluxio and the second one for 
filesystem service.
Again, increase the interval to reduce the number of heartbeat checks.

## Alluxio Client Configuration

### RPC Retry Interval

The following properties tune RPC retry intervals:

```properties
alluxio.user.rpc.retry.max.duration=2min
alluxio.user.rpc.retry.base.sleep=1s
```

The retry duration and sleep duration should be increased if frequent timeouts are observed
when a client attempts to communicate with the Alluxio master.

### Thread Pool Size

On a single client, the number of threads connecting to the master is configured by the
`alluxio.user.block.master.client.threads` and `alluxio.user.file.master.client.threads` properties,
each with a default value of `10`.
The size of the master thread pool that serves connections to clients should be tuned to match
the maximum number of concurrrent client connections.
For example, if the master expects up to 100 clients, each with the default number of connections,
the master's thread pool should be configured to be at least `100 * 10 * 2 = 2000`.

Consider reducing these values if the master is not responsive
as it is possible that the master thread pool is completely drained:

```properties
alluxio.user.block.master.client.threads=5
alluxio.user.file.master.client.threads=5
```
