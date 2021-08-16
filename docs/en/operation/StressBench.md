---
layout: global
title: StressBench Framework
nickname: StressBench
group: Operations
priority: 7
---

* Table of Contents
  {:toc}

The Alluxio system runs in different environments with different hardware for various purposes. The
StressBench test suite is a performance benchmark suite to help you understand the performance of
Alluxio in different scenarios. It will help you quickly test and analyze the Alluxio usage in your
environment by simply running the command line.

The StressBench is consisted of different tests to help you test the metadata and IO performance of
the Alluxio system. The following benchmark tools are currently supported:
* `StressMasterBench` - A benchmark tool measuring the master performance of Alluxio.
* `StressClientBench` - A benchmark tool measuring the IO performance of Alluxio through the client.
* `StressWorkerBench` - A benchmark tool measuring the IO performance of Alluxio with single node.
* `UfsIOBench` - A benchmark tool measuring the IO performance to UFS.
* `FuseIOBench` - A benchmark tool measuring the IO performance of Alluxio through Fuse interface.

## High-Level Design
StressBench is a job that runs with the job service. Therefore, the design follows what the job
service supports. There are a few components for StressBench as follows:

### Client CLI
The user-facing CLI program allows you to invoke the StressBench tasks through the command line.

### Job Definition
This is the definition of the job master, which is invoked by the client CLI program, and will
further coordinate distributed tasks for the job.

### Distributed Tasks
These tasks are run on the job worker, and eventually report back to the job master.

## Consistent Results
When testing results are inconsistent and wildy variable, it makes trusting the results more
difficult. Therefore, there are some features that help StressBench to be consistent with results.

### Barriers
Barriers are one way that StressBench tries to achieve consistent results. This allows distributed
tasks to all start simultaneously and not stagger the starting of the distributed tasks. The job
definition will define the timestamp of when the tasks should start. This depends on the timestamps
of the job workers to be in sync. Then, the tasks themselves can use that start time to ensure all
tasks and threads start at the defined time. This ensures that all distributed tasks and threads
start at the same time for the testing.

### Duration-based testing (with warmups)
Another way to improve the consistency of results is to run tests with warmups and also for a
defined amount of time. If a particular thread or task is finished first, it would also provide any
load to the system instead of ending with an idle stage. That means tasks could start at the same
times and end at the same times, which will ensure the system is under load during the whole testing
time.

Therefore, StressBench tests should have a warmup period and duration period. For example, the
master test has a `--warmup` flag and a `--duration` flag. This ensures all tasks and threads will do
work until they all stop at the same time, since they all started simultaneously.

### MaxThroughput
StressBench uses the MaxThroughput algorithm to reach the max throughput of an operation. The
high-level idea is that the test will produce an input load throughput, and if the server can
achieve that input throughput, the load will be increased. If the server cannot achieve the input
throughput, then that throughput value is not valid, and the actual max throughput is some value
smaller than that so that the next input throughput will be a smaller value.

The algorithm is similar to binary search, where the input throughput is doubled until the
throughput is not achieved. Then, the input throughput is reduced (half of the previous window).
This process continues until the max throughput is discovered.

This algorithm uses many iterations and time to determine the throughput, but it produces a more
consistent result, and the actual number is meaningful. If the resulting throughput is X, that
means if the clients produce X throughput, the server can achieve that. It also means if the clients
produce more than X throughput, like X+N, the server will not be able to achieve X+N throughput.
