---
layout: global
title: Scalability Tuning
nickname: Scalability Tuning
group: Advanced
priority: 0
---

Alluxio is a scalable distributed file system designed to handle many workers within a single cluster.
Several parameters can be tuned to prevent the Alluxio master from getting overloaded. This page
aims to enumerate a list of such parameters which should be tuned for the scale of your cluster.

* Table of Contents
{:toc}

## Alluxio Master Configuration

### Heap Size

The Alluxio master heap size directly controls the total number of files that can fit into the
master memory. Control the heap size by setting JVM options for the Alluxio master and secondary
master processes. For example, to set the heap size limit to `256GB`, modify `alluxio-env.sh` as
follows.
```properties
ALLUXIO_MASTER_JAVA_OPTS+=" -Xms128g -Xmx256g "
ALLUXIO_SECONDARY_MASTER_JAVA_OPTS+=" -Xms128g -Xmx256g "
```

Note:

* As a rule of thumb set the min heap size to half the max heap size.
* Each thread spawned by the master JVM requires off heap space determined by the thread stack
size. When setting the heap size, ensure that you have enough memory allocated for off heap storage.
For example, to spawn `50000` threads with a default thread stack size of `1MB` ensure you
have at least `50GB` of off-heap space available.

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

Note:

* You may need to set OS limits, as defined in the following section, to allow the above number of
threads to be spawned.
* Check that the amount of off heap storage available allows for the count thread.

### Operating System Limits
Several OS parameters limit the number of threads that a process can spawn. These limits are often
set for the specific user the process in running as. Tune the following parameters to allow the
Alluxio master JVM to spawn the number of threads specified in the previous section: `kernel.pid_max`,
`vm.max_map_count`, max user processes limit (`ulimit -u`) and open files limit (`ulimit -n`).

An exception message like `java.lang.OutOfMemoryError: unable to create new native thread`
indicates that the limits may need tuning.

Note:

* As a rule of thumb, `vm.max_map_count` should be at least twice the limit for master threads.

### Heartbeat Intervals and Timeouts

Frequent heartbeats can cause delays. Tune the following parameter(s) to control the frequency with
which the master checks for lost workers and other heartbeat functions.
```properties
alluxio.master.heartbeat.interval.ms=60s
```

## Alluxio Worker Configuration

### Heartbeat Intervals and Timeouts

Frequent heartbeats can cause delays. Tune the following parameter(s) to control the frequency with
which all workers heartbeat with the master.
```properties
alluxio.worker.block.heartbeat.interval.ms=60s
alluxio.worker.filesystem.heartbeat.interval.ms=60s
```

## Alluxio Client Configuration

## RPC Retry Interval

If frequent timeouts are observed communicating with the Alluxio master, tune the rpc retry intervals:
```properties
alluxio.user.rpc.retry.max.duration=2min
alluxio.user.rpc.retry.base.sleep.ms=1s
```

### Thread Pool Size

Consider reducing the client thread pool sizes if the master is not responsive.
```properties
alluxio.user.block.master.client.threads=5
alluxio.user.file.master.client.threads=5
```
