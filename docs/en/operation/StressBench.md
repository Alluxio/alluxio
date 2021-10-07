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

The Fuse IO Stress Bench is a tool to measure the reading performance of local folders.

In single node mode, Fuse IO Stress Bench measures the reading performance of a local folder. This folder can be local filesystem folder or any kinds of mount point.

In the cluster mode, it uses Alluxio job service to launch distributed reading jobs mocking the actual distributed training data loading workloads.
In each worker node, there is one Alluxio worker, one Alluxio job worker, and one Fuse mount point. Each job worker launches a bench job reading data from the local Fuse mount point.
All the reading data from all job worker will be aggregated to get the cluster read throughput.

### Parameters
The parameters for the Fuse IO Stress Bench are (other than common parameters for any stress bench):

<table class="table table-striped">
    <tr>
        <td>Parameters</td>
        <td>Default Value</td>
        <td>Description</td>
    </tr>
    <tr>
        <td>operation</td>
        <td>Required. No default value.</td>
        <td>The operation that is going to perform. Available operations are [Write, ListFile, LocalRead, RemoteRead, ClusterRead], where Write and ListFile are for testing reading performance, not individual tests.</td>
    </tr>
    <tr>
        <td>local-path</td>
        <td>/mnt/alluxio-fuse/fuse-io-bench</td>
        <td>The path that the operation is going to perform at. Can be a local filesystem path or a mounted Fuse path.</td>
    </tr>
    <tr>
        <td>threads</td>
        <td>1</td>
        <td>The number of concurrent threads that is going to be used for the operation.</td>
    </tr>
    <tr>
        <td>num-dirs</td>
        <td>1</td>
        <td>The number of directories that the files will be evenly distributed into. It must be at least the number of threads and preferably a multiple of it. </td>
    </tr>
    <tr>
        <td>num-files-per-dir</td>
        <td>1000</td>
        <td>The number of files per directory.</td>
    </tr>
    <tr>
        <td>file-size</td>
        <td>100k</td>
        <td>The files size for IO operations. (100k, 1m, 1g, etc.)</td>
    </tr>
    <tr>
        <td>warmup</td>
        <td>15s</td>
        <td>The length of time to warmup before recording measurements. (1m, 10m, 60s, 10000ms, etc.)</td>
    </tr>
    <tr>
        <td>duration</td>
        <td>30s</td>
        <td>The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)</td>
    </tr>
</table>

### Single node testing
#### Prerequisite
* A running Alluxio cluster with one master, and one worker.
* A Fuse mount point. See [FUSE-based POSIX API](https://docs.alluxio.io/os/user/stable/en/api/POSIX-API.html) for more details on mounting.
* (Optional) To prevent operating system cache and Fuse cache from affecting benchmark accuracy, using different mount points for writing and reading test files. 

#### Write test files
Write the test files by running the benchmark with `--operation Write` into the mount point for writing files
```console
$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation Write --local-path /path/to/writing/mount/point ...
```

For example,
```console
$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation Write --local-path /mnt/FuseIOBenchWrite --num-dirs 128 \
--num-files-per-dir 100 --file-size 1m --threads 32
```

`warmup`  and `duration` parameters are not valid in writing. The `Write` operation always writes all the files needed for reading test.

#### List out test files (optional)
Listing out the test files could cache the file metadata, so that the metadata operation won't affect reading throughput accuracy in the reading step.
This step could be particularly useful when the file size is small and metadata operation introduces a relatively large overhead. To enable metadata cache,
`alluxio.user.metadata.cache.enabled` needs to be set to `true` in `${ALLUXIO_HOME}/conf/alluxio-site.properties` before launching Alluxio cluster.

To cache the file metadata, run the benchmark with `--operation ListFile` at the mount point for reading files. More specifically, run
```console
$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation ListFile --local-path /path/to/reading/mount/point ...
```

For example,
```console
$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation ListFile --local-path /mnt/FuseIOBenchRead
```

`num-dirs`, `num-files-per-dir`, `file-size`, `warmup`, and `duration` are not valid in listing stage.

#### Read test files 
Read the test files written by running the benchmark with `--operation LocalRead` from the reading mount point.
```console
$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation LocalRead --local-path /path/to/reading/mount/point ...
```

For example,
```console
$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation LocalRead --local-path /mnt/FuseIOBenchRead --num-dirs 128 \
--num-files-per-dir 100 --files-size 1m --buffer-size 512k --warmup 5s --duration 30s 
```

#### Demo
Here is a sample usage demo, where the Alluxio master, worker,
and client (standalone Fuse) are co-located on one machine:
[![Watch the video](https://img.youtube.com/vi/pKaFQPFvuxo/maxresdefault.jpg)](https://youtu.be/pKaFQPFvuxo)

By tweaking the setups, the reading performance under more scenarios can also be tested:
* By using Fuse in worker process for reading, which needs to be configured in `${ALLUXIO_HOME}/conf/alluxio-site.properties`
  (see [FUSE-based POSIX API](https://docs.alluxio.io/os/user/stable/en/api/POSIX-API.html#fuse-on-worker-process)), the local read
  throughput can be benchmarked. Not gRPC or network is involved in the local read.
* By writing to worker on one machine and reading from the Fuse mount point on another machine, remote read throughput can be benchmarked.
  Remote read introduces extra gRPC and network overhead compared to local read.
* By using `StackFS` mount, the throughput of JNI-fuse without Alluxio can be tested.
* By setting `alluxio.user.file.writetype.default` to `THROUGH` when writing test files, the throughput of reading from UFS can be tested.
* ...

### Cluster testing
#### Prerequisite
- A running Alluxio cluster. Each worker node contains one worker, one job worker, and one Fuse mount point.
- Alluxio cluster is configured with `alluxio.user.block.write.location.policy.class=alluxio.client.block.policy.LocalFirstPolicy` (which is the default configuration)
- Each Alluxio worker has enough space to store the test data. The worker storage size for each worker is bigger than
`num-dirs` * `num-files-per-dir` * `file-size` / `worker-number` / `alluxio.worker.tieredstore.levelX.watermark.low.ratio=0.7 by default`.

#### Testing
The cluster testing is similar to single node testing except that
- `--cluster` argument needs to be added to each operation so that the bench jobs will be submitted to job master, distributed to job workers, and executed by job workers.
Each job worker executes one bench job which reads from the co-located Fuse mount point and gets the local read throughput. Cluster throughput is calculated by aggregating the read throughput of each bench job.
- Two more read operations `RemoteRead` `ClusterRead` are supported besides `LocalRead`.
1. `LocalRead`: Each job worker reads the data written by itself. With the cluster settings, each job worker reads the data from local worker through local Fuse mount point.
2. `RemoteRead`: Each job worker reads the data written by other job workers evenly. With the cluster settings, each job worker reads the data from remote workers through local Fuse mount point.
In a cluster with 3 worker nodes, each worker nodes are written 30MB data. Each job worker will read 15MB data from each of the two remote workers in `RemoteRead` operation.
3. `ClusterRead`: Each job worker reads the data written by all job workers evenly. With the cluster settings, each job worker reads the data from local and remote workers through local Fuse mount point.
In a cluster with 3 worker nodes, each worker node are written 30MB data. Each job worker will read 10MB data from each worker in `ClusterRead` operation.

Here is a sample usage demo, where we have one master and three workers:
[![Watch the video](https://img.youtube.com/vi/UqmQbYYR4NQ/maxresdefault.jpg)](https://youtu.be/UqmQbYYR4NQ)

### Limitations
- Fuse IO Stress Bench only supports reading self-generated test files.
- The `Write` operation is only used for generating test files instead of measuring throughput.
- To prevent caching affecting accuracy, each file is only read at most once by each worker. Therefore, the total size of the test files and duration
need to be tested and tuned such that no thread finish reading its designated files before the test duration passed.
