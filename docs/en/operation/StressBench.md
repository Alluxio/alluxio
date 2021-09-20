---
layout: global
title: Built-in Benchmark Framework
nickname: StressBench (Experimental)
group: Operations
priority: 7
---

* Table of Contents
{:toc}

The Alluxio system runs in different environments with different hardware for various purposes.
The StressBench test suite is a performance benchmark suite to help you understand the performance of Alluxio in different scenarios. 
It will help you quickly test and analyze the Alluxio usage in your environment by simply running the command line.

StressBench provides different benchmark suites to cover both metadata and IO performance of different components in an Alluxio deployment.
The following benchmark suites are currently supported:

* `FuseIOBench` - A benchmark tool measuring the IO performance of Alluxio through Fuse interface.
* `RpcBench` - A set of benchmark tools which simulate RPCs with a specified load and concurrency.
  * `GetPinnedFileIdsBench` - for the `GetPinnedFileIds` RPC
  * `RegisterWorkerBench` - for the `RegisterWorker` RPC
  * `WorkerHeartbeatBench` - for the `WorkerHeartbeat` RPC
* `StressClientBench` - A benchmark tool measuring the IO performance of Alluxio through the client.
* `StressMasterBench` - A benchmark tool measuring the master performance of Alluxio.
* `StressWorkerBench` - A benchmark tool measuring the IO performance of Alluxio with single node.
* `UfsIOBench` - A benchmark tool measuring the IO throughput between the Alluxio cluster and the UFS.

## Examples
### Basic Examples
The user-facing CLI program allows you to invoke the StressBench tasks through the command line.
Run a benchmark by its class name using the `runClass` command (there is a feature request to implement a generic CLI for StressBench [14083](https://github.com/Alluxio/alluxio/issues/14083)).

For example,

```console
$ bin/alluxio runClass alluxio.stress.cli.StressMasterBench --help
```

This command invokes the `StressMasterBench` and prints its usage.

To test RPC throughput of Alluxio master (e.g., for certain RPCs):

```console
$ bin/alluxio runClass alluxio.stress.cli.GetPinnedFileIdsBench
$ bin/alluxio runClass alluxio.stress.cli.RegisterWorkerBench
$ bin/alluxio runClass alluxio.stress.cli.WorkerHeartbeatBench
```

To benchmark Alluxio FUSE performance
```console
$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench
```

To benchamrk job service
```console
$ bin/alluxio runClass alluxio.stress.cli.StressJobServiceBench
```

### Options

Here is the list of common options that all benchmarks accept:

* `--bench-timeout`: the maximum time a benchmark should run for
* `--cluster`: run the benchmark with job service.
* `--cluster-limit`: specify how many job workers are used to run the benchmark in parallel. 
  Omit to use all available job workers.
* `--cluster-start-delay`: specify the start delay before starting the benchmark, used to synchronize jobs.
* `--java-opt`: specify additional JVM options
* `--help`: print usage, description and benchmark-specific options of a benchmark

Individual benchmark suite may also define its own set of options.
Run the benchmark with the `--help` option to get a list of supported options and example usage.


## Ensuring Consistent Results
When tests are inconsistent and wildly variable, the benchmark results will be less reliable and reproducible. 
We introduced following features to help StressBench generate consistent results.

### Barriers
Barriers are one way that StressBench tries to achieve consistent results.
This allows distributed tasks to all start simultaneously and not stagger the starting of the distributed tasks.
The job definition will define the timestamp of when the tasks should start.
This depends on the timestamps of the job workers to be in sync.
Then, the tasks themselves can use that start time to ensure all tasks and threads start at the defined time.
This ensures that all distributed tasks and threads start at the same time for the testing.

The default start time is set to `10s` from the current time.
Use a larger value for `--cluster-start-delay` to accommodate large clusters.

### Duration-based testing (with warmups)
Another way to improve the consistency of results is to run tests with warmups and also for a defined amount of time.
If a particular thread or task is finished first, it would also provide any load to the system instead of ending with an idle stage.
That means tasks could start at the same times and end at the same times, which will ensure the system is under load during the whole testing
period.

Therefore, StressBench tests should have a warmup period and duration period.
For example, the master test has a `--warmup` flag and a `--duration` flag.
This ensures all tasks and threads will do work until they all stop at the same time, since they all started simultaneously.

### MaxThroughput
StressBench uses the MaxThroughput algorithm to reach the max throughput of an operation.
The high-level idea is that the test will produce an input load throughput, and if the server can achieve that input throughput, the load will be increased.
If the server cannot achieve the input throughput, then that throughput value is not valid,
and the actual max throughput is some value smaller than that so that the next input throughput will be a smaller value.

The algorithm is similar to binary search, where the input throughput is doubled until the throughput is not achieved.
Then, the input throughput is reduced (half of the previous window).
This process continues until the max throughput is discovered.

This algorithm uses many iterations and time to determine the throughput, but it produces a more consistent result, and the actual number is meaningful.
If the resulting throughput is X, that means if the clients produce X throughput, the server can achieve that.
It also means if the clients produce more than X throughput, like X+N, the server will not be able to achieve X+N throughput.
