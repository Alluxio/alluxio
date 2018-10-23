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

TODO: wait aren't these properties for this master <-> worker communication, not master <-> client communication?
and the latter is covered in Block thread pool size under Performance Tuning so i'm thoroughly confused

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
TODO: how does one configure the first two???
- `kernel.pid_max`
- `vm.max_map_count`
- Max user process limit: `ulimit -u`
- Max open files limit: `ulimit -n`

These limits are often set for the particular user that launch the Alluxio process.
As a rule of thumb, `vm.max_map_count` should be at least twice the limit for master threads.
TODO: twice the limit for master threads, so what determines the limit for master threads???

### Heartbeat Intervals and Timeouts

The frequency in which the master checks for lost workers is set by the
`alluxio.master.heartbeat.interval.ms` property, with a default value of `60s`.
Increase the interval to reduce the number of heartbeat checks.

## Alluxio Worker Configuration

### Heartbeat Intervals and Timeouts

The frequency in which a worker checks in with the master is set by the following properties:
TODO: I feel like there should be an explanation on why there's two separate heartbeat intervals...
```properties
alluxio.worker.block.heartbeat.interval.ms=60s
alluxio.worker.filesystem.heartbeat.interval.ms=60s
```
Again, increase the interval to reduce the number of heartbeat checks.

## Alluxio Client Configuration

## RPC Retry Interval

The following properties tune RPC retry intervals:

```properties
alluxio.user.rpc.retry.max.duration=2min
alluxio.user.rpc.retry.base.sleep.ms=1s
```
TODO: `alluxio.user.rpc.retry.base.sleep.ms` is awkwardly named if it takes a duration and not a number of ms as the value? is this correct?

The retry duration and sleep duration should be increased if frequent timeouts are observed
when a client attempts to communicate with the Alluxio master.

### Thread Pool Size

The size of the client thread pool for block and file operations are configured by the following properties:

```properties
alluxio.user.block.master.client.threads=5
alluxio.user.file.master.client.threads=5
```

Consider reducing these values if the master is not responsive, as it is possible that the master thread pool is completely drained.
