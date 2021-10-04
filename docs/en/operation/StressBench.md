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


## Fuse IO Stress Bench
The Fuse IO Stress Bench is a tool to measure the reading performance of Alluxio Fuse-based POSIX API.

### Procedure

#### 0. Prerequisite
A running Alluxio cluster is required. The Alluxio path for testing needs to be mounted to the local file system. To prevent Fuse cache affecting
test accuracy, it is suggested to use different mount points for writing test files and read. See 
[FUSE-based POSIX API](https://docs.alluxio.io/os/user/stable/en/api/POSIX-API.html) for details on how to mount Alluxio to local file system.

#### 1. Write test files
Write the test files by running the benchmark with `--operation Write` into the mount point for writing files. More specifically, run 

`$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation Write --local-path /path/to/writing/mount/point ...`

#### 2. List out test files (optional)
List out the test files by running the benchmark with `--operation ListFile` at the mount point for reading files. More specifically, run

`$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation ListFile --local-path /path/to/reading/mount/point`

This operation will cache the file metadata, so that the metadata operation while reading won't affect reading throughput accuracy. To enable metadata cache,
`alluxio.user.metadata.cache.enabled` needs to be set to `true` in `${ALLUXIO_HOME}/conf/alluxio-site.properties` before launching Alluxio cluster. 

#### 3. Read test files 
Read the test files by running the benchmark with `--operation {LocalRead, RemoteRead, ClusterRead}` from the reading mount point. For example, run

`$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation LocalRead --local-path /path/to/reading/mount/point ...`

### Single-node mode test
Single node mode has only one client and the client always reads test files stored in one worker from one mount point. Only `LocalRead` is supported
because both `RemoteRead` and `ClusterRead` involve reading data in multiple workers. Here is a sample usage demo, where the Alluxio master, worker, 
and client (standalone Fuse) are co-located on one machine:

[![Watch the video](https://img.youtube.com/vi/pKaFQPFvuxo/maxresdefault.jpg)](https://youtu.be/pKaFQPFvuxo)

By tweaking the setups, the reading performance under more scenarios can also be tested: 
* By writing to worker on one machine and reading from mount point on another machine, the throughput of a remote read using network and Grpc can be tested;
* By using workerFuse for reading, which needs to be configured in `${ALLUXIO_HOME}/conf/alluxio-site.properties` 
(see [FUSE-based POSIX API](https://docs.alluxio.io/os/user/stable/en/api/POSIX-API.html#fuse-on-worker-process)), the throughput of reading from
Alluxio internal channel without Grpc involved can be tested.
* By using StackFS mount, the throughput of JNI-fuse without Alluxio can be tested.
* By setting `alluxio.user.file.writetype.default` to `THROUGH`, the throughput of reading from UFS can be tested.
* ...

### Cluster mode test
To run the test in cluster mode, include `--cluster` argument when running each command. Cluster mode uses job service to run the bench and job workers
become the clients. Each job worker reads from its local reading mount point. To get a more accurate result, it is highly suggested to having one worker and
one job worker on each machine. In other words, each worker has a co-located job worker and each job worker has a co-located worker. This scenario is considered
as the defulat case. There are 3 types of read available in cluster mode:
* LocalRead: each job worker only reads the files that it wrote. In the default case, each job worker reads the test files that are stored in the co-located worker.
* RemoteRead: each job worker evenly reads the files written by other job workers. In the default case, each job worker reads evenly from all workers except
the co-located one.
* ClusterRead: each job worker evenly reads the files written by all job workers. In the default case, each job worker reads evenly from all workers.

Here is a sample usage demo, where we have one master and three workers. Each client (job worker) reads from standalone Fuse:

[![Watch the video](https://img.youtube.com/vi/UqmQbYYR4NQ/maxresdefault.jpg)](https://youtu.be/UqmQbYYR4NQ)

### Note
1. Fuse IO Stress Bench only supports reading self-generated test files. It cannot read arbitrary files.
2. The `Writing` operation is only for generating test files. It is not for measuring writing throughput.
3. To prevent caching affecting accuracy, each file is only read at most once by each worker. Therefore, the total size of the test files and duration
need to be tuned such that no thread should finish reading its designated files. Otherwise, some threads would finish its job and thus affecting accuracy.
4. To prevent Fuse cache affecting accuracy, it is highly suggested mounting Alluxio test path to two different local path, one for writing and the other for reading. 