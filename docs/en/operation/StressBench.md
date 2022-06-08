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
* `StressClientBench` - A benchmark tool measuring the IO performance of Alluxio through the client.
* `StressMasterBench` - A benchmark tool measuring the master performance of Alluxio.
* `StressWorkerBench` - A benchmark tool measuring the IO performance of reading from Alluxio Worker.
* `UfsIOBench` - A benchmark tool measuring the IO throughput between the Alluxio cluster and the UFS.
* `MaxFileBench` - A benchmark tool measuring the maximum number of empty files in Alluxio metastore. 

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
$ bin/allixio runClass alluxio.stress.cli.StreamRegisterWorkerBench
```

To benchmark Alluxio FUSE performance
```console
$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench
```

To benchmark job service
```console
$ bin/alluxio runClass alluxio.stress.cli.StressJobServiceBench
```

### Options

Here is the list of common options that all benchmarks accept:

* `--bench-timeout`: the maximum time a benchmark should run for
* `--cluster`: run the benchmark with job service.
* `--cluster-limit`: specify how many job workers are used to run the benchmark in parallel. 
  Omit to use all available job workers.
  > Note this limit must not be greater than the number of currently running job workers, or the 
  > benchmark will fail.
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
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
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
<iframe width="800" height="350" src="https://www.youtube.com/embed/pKaFQPFvuxo" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

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
<iframe width="800" height="450" src="https://www.youtube.com/embed/cSF0SnL6LB4" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

### Limitations
- Fuse IO Stress Bench only supports reading self-generated test files.
- The `Write` operation is only used for generating test files instead of measuring throughput.
- To prevent caching affecting accuracy, each file is only read at most once by each worker. Therefore, the total size of the test files and duration
need to be tested and tuned such that no thread finish reading its designated files before the test duration passed.

## Job Service Stress Bench

The Job Service Stress Bench is a tool to measure the performance of job service. Testing different aspect of job service performance through different operation.

### Parameters
The parameters for the Job Service Stress Bench(including JobServiceMaxThroughput) are (other than common parameters for any stress bench):

<table class="table table-striped">
    <tr>
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>operation</td>
        <td>Required. No default value.</td>
        <td>The operation that is going to perform. Available operations are [DistributedLoad, CreateFiles, NoOp]</td>
    </tr>
    <tr>
        <td>threads</td>
        <td>256</td>
        <td>The number of concurrent threads that is going to be used for the operation. For createFiles, it would create folders based on this number.</td>
    </tr>
    <tr>
        <td>num-files-per-dir</td>
        <td>1000</td>
        <td>The number of files per directory.</td>
    </tr>
    <tr>
        <td>file-size</td>
        <td>1k</td>
        <td>The files size for created files. (100k, 1m, 1g, etc.)</td>
    </tr>
    <tr>
        <td>warmup</td>
        <td>30s</td>
        <td>The length of time to warmup before recording measurements. (1m, 10m, 60s, 10000ms, etc.)</td>
    </tr>
    <tr>
        <td>duration</td>
        <td>30s</td>
        <td>The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)</td>
    </tr>
    <tr>
        <td>target-throughput</td>
        <td>1000</td>
        <td>The target throughput to issue operations. Used in maxThroughput test</td>
    </tr>
</table>

### Single node testing with operation: DistributedLoad
#### Prerequisite
* A running Alluxio cluster with one master, and one worker.
#### Notice
`warmup`  and `duration` parameters are not valid in distributedLoad test. We just send all distributedLoad jobs at the beginning and record the throughput when all jobs finishes.
#### Create test files
Write the test files by running the benchmark with `--operation CreateFiles` into the test directory
```console
$ bin/alluxio runClass alluxio.stress.cli.StressJobServiceBench --operation CreateFiles --base /path/to/test/directory ...
```

For example, we are creating 10 directories with 1000 files per directory.
```console
$ bin/alluxio runClass alluxio.stress.cli.StressJobServiceBench --base alluxio://localhost:19998/stress-job-service-base --file-size 1k --files-per-dir 1000 --threads 10 --operation CreateFiles
```

#### DistributedLoad test files
Load the test files written by running the benchmark with `--operation DistributedLoad` from the test directory. The parameter should be the same as CreateFiles operation. It would send distributedLoad requests concurrently(one directory per request)
```console
$ bin/alluxio runClass alluxio.stress.cli.StressJobServiceBench --base alluxio://localhost:19998/stress-job-service-base --file-size 1k --files-per-dir 1000 --threads 10 --operation DistributedLoad
```

### Single node testing with operation: NoOp
#### Notice
NoOp is mainly for measuring the throughput of Job Master(job management capability) and doesn't involve any job worker. JobServiceMaxThroughput test with NoOp operation is the recommended way to measure the performance of job master.

#### Single Test
Continuously Sending NoOp jobs and measure the throughput within certain time range.
```console
$ bin/alluxio runClass alluxio.stress.cli.StressJobServiceBench --warmup 30s --duration 30s --threads 10 --operation NoOp
```
#### JobServiceMaxThroughput Test for NoOp operation
JobServiceMaxThroughput is applying MaxThroughput algorithm to job service. Testing the job management capability of job master.
Only NoOp operation is supported for JobServiceMaxThroughput.
```console
$ bin/alluxio runClass alluxio.stress.cli.suite.JobServiceMaxThroughput --duration 30s --threads 16 --warmup 30s --operation NoOp 
```

### Cluster testing
#### Prerequisite
- A running Alluxio cluster. Each worker node contains at least one worker, one job worker.
- Configure Alluxio cluster with `alluxio.job.master.job.capacity = 1000000` and `alluxio.job.master.finished.job.retention.time = 10s` to allow large scale job service stress bench.
- If you are running distributedLoad operation, make sure UFS has enough space to store the test data. The storage size is bigger than
  `thread` * `num-files-per-dir` * `file-size` / `worker-number` / `alluxio.worker.tieredstore.levelX.watermark.low.ratio` which is `0.7` by default.

#### Testing
The cluster testing is similar to single node testing except that
- `--cluster` argument needs to be added to each operation so that the bench jobs will be submitted to job master, distributed to job workers, and executed by job workers.
  Each job worker executes one bench job. You can use `--cluster-limit` to specify how many bench jobs run. Cluster throughput is calculated by aggregating the throughput of each bench job.

### Limitations
- Job Service Stress Bench only supports loading self-generated test files.
- The `CreateFiles` operation is only used for generating test files instead of measuring throughput.

## Client IO Stress Bench

The Client IO Stress Bench is a tool to measure the IO performance of Alluxio through the client.

### Parameters
The parameters for the Client IO Stress Bench are (other than common parameters for any stress bench):

<table class="table table-striped">
    <tr>
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>operation</td>
        <td>Required. No default value.</td>
        <td>The operation to perform. Options are [ReadArray, ReadByteBuffer, ReadFully, PosRead, PosReadFully]</td>
    </tr>
    <tr>
        <td>client-type</td>
        <td>AlluxioHDFS</td>
        <td>The client API type. Alluxio native or hadoop compatible client</td>
    </tr>
    <tr>
        <td>read-type</td>
        <td>CACHE</td>
        <td>The cache mechanism during read. Options are [NONE, CACHE, CACHE_PROMOTE]</td>
    </tr>
    <tr>
        <td>write-type</td>
        <td>value of <code>alluxio.user.file.writetype.default</code></td>
        <td>The write type to use when creating files. Options are [MUST_CACHE, CACHE_THROUGH, 
THROUGH, ASYNC_THROUGH, ALL]. ALL will make the benchmark run with every write type.</td>
    </tr>
    <tr>
        <td>clients</td>
        <td>1</td>
        <td>The number of fs client instances to use</td>
    </tr>
    <tr>
        <td>threads</td>
        <td>[1]</td>
        <td>The comma-separated list of thread counts to test. The throughput for each thread count is benchmarked and measured separately.</td>
    </tr>
    <tr>
        <td>base</td>
        <td>alluxio://localhost:19998/stress-client-io-base</td>
        <td>The base directory path URI to perform operations in. </td>
    </tr>
    <tr>
        <td>base-alias</td>
        <td></td>
        <td>The alias for the base path, unused if empty</td>
    </tr>
    <tr>
        <td>file-size</td>
        <td>1g</td>
        <td>The files size for IO operations. (100k, 1m, 1g, etc.)</td>
    </tr>
    <tr>
        <td>buffer-size</td>
        <td>64k</td>
        <td>The buffer size for IO operations. (1k, 16k, etc.)</td>
    </tr>
    <tr>
        <td>block-size</td>
        <td>64m</td>
        <td>The block size of files. (16k, 64m, etc.)</td>
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
    <tr>
        <td>read-same-file</td>
        <td>false</td>
        <td>If true, all threads read from the same file. Otherwise, each thread reads from its own file.</td>
    </tr>
    <tr>
        <td>read-random</td>
        <td>false</td>
        <td>If true, threads read the file from random offsets. For streaming operations, seek() is called to read random offsets. If false, the file is read sequentially.</td>
    </tr>
    <tr>
        <td>write-num-workers</td>
        <td>1</td>
        <td>The number of workers to distribute the files to. The blocks of a written file will be round-robin across these number of workers.</td>
    </tr>
    <tr>
        <td>conf</td>
        <td>{}</td>
        <td>Any HDFS client configuration key=value. Can repeat to provide multiple configuration values.</td>
    </tr>
  
</table>

### Single node testing
#### Prerequisite
* A running Alluxio cluster with one master, and one worker, with property `alluxio.user.file.writetype.default` set to `MUST_CACHE`

#### Write test files
Write the test files by running the benchmark with `--operation Write`
```console
$ bin/alluxio runClass alluxio.stress.cli.client.StressClientIOBench --operation Write --base /path/to/test/directory ...
```

For example,
```console
$ bin/alluxio runClass alluxio.stress.cli.client.StressClientIOBench --operation Write --base alluxio://localhost:19998/stress-client-io-base \
--write-num-workers 100 --file-size 1m --threads 32
```

`warmup`  and `duration` parameters are not valid in writing. The `Write` operation always writes all the files needed for reading test.

#### Read test files
Read the test files written by running the benchmark with read operations. You can test different kinds of read operation.
```console
$ bin/alluxio runClass alluxio.stress.cli.client.StressClientIOBench --operation ReadArray ...
```

For example, we are testing Streaming read api, using byte buffers with buffer size 512k.
```console
$ bin/alluxio runClass alluxio.stress.cli.client.StressClientIOBench --operation ReadByteBuffer --files-size 1m --buffer-size 512k --warmup 5s --duration 30s 
```

### Cluster testing
#### Prerequisite
- A running Alluxio cluster. Each worker node contains one worker, one job worker.
- Each Alluxio worker has enough space to store the test data. 

#### Testing
The cluster testing is similar to single node testing except that
- `--cluster` argument needs to be added to each operation so that the bench jobs will be submitted to job master, distributed to job workers, and executed by job workers.
  Each job worker executes one bench job which reads from the co-located Fuse mount point and gets the local read throughput. Cluster throughput is calculated by aggregating the read throughput of each bench job.

### Limitations
- Client IO Stress Bench only supports reading self-generated test files.
- The `Write` operation is only used for generating test files instead of measuring throughput.


## Master Stress Bench

The Master Stress Bench is a tool to measure the master performance of Alluxio. MaxThroughput is the recommended way to run the Master Stress Bench.

### Parameters
The parameters for the Master Stress Bench(including MaxThroughput) are (other than common parameters for any stress bench):

<table class="table table-striped">
    <tr>
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>operation</td>
        <td>Required. No default value.</td>
        <td>The operation to perform. Options are [CreateFile, GetBlockLocations, GetFileStatus, OpenFile, CreateDir, ListDir, ListDirLocated, RenameFile, DeleteFile]</td>
    </tr>
    <tr>
        <td>client-type</td>
        <td>AlluxioHDFS</td>
        <td>The client API type. Alluxio native or hadoop compatible client</td>
    </tr>
    <tr>
        <td>clients</td>
        <td>1</td>
        <td>The number of fs client instances to use</td>
    </tr>
    <tr>
        <td>threads</td>
        <td>256</td>
        <td>The number of concurrent threads to use</td>
    </tr>
    <tr>
        <td>base</td>
        <td>alluxio://localhost:19998/stress-client-io-base</td>
        <td>The base directory path URI to perform operations in. </td>
    </tr>
    <tr>
        <td>base-alias</td>
        <td></td>
        <td>The alias for the base path, unused if empty</td>
    </tr>
    <tr>
        <td>create-file-size</td>
        <td>1g</td>
        <td>The size of a file for the Create op, allowed to be 0. (0, 1m, 2k, 8k, etc.)</td>
    </tr>
    <tr>
        <td>target-throughput</td>
        <td>1000</td>
        <td>The target throughput to issue operations. (ops / s)</td>
    </tr>
    <tr>
        <td>stop-count</td>
        <td>-1</td>
        <td>The benchmark will stop after this number of paths. If -1, it is not used and the benchmark will stop after the duration. If this is used, duration will be ignored. This is typically used for creating files in preparation for another benchmark, since the results may not be reliable with a non-duration-based termination condition.</td>
    </tr>
    <tr>
        <td>fixed-count</td>
        <td>100</td>
        <td>The number of paths in the fixed portion. Must be greater than 0. The first 'fixed-count' paths are in the fixed portion of the namespace. This means all tasks are guaranteed to have the same number of paths in the fixed portion. This is primarily useful for ensuring different tasks/threads perform an identically-sized operation. For example, if fixed-count is set to 1000, and CreateFile is run, each task will create files with exactly 1000 paths in the fixed directory. A subsequent ListDir task will list that directory, knowing every task/thread will always read a directory with exactly 1000 paths.</td>
    </tr>
    <tr>
        <td>warmup</td>
        <td>15s</td>
        <td>The length of time to warmup before recording measurements. (1m, 10m, 60s, 10000ms, etc.)</td>
    </tr>
    <tr>
        <td>write-type</td>
        <td>value of <code>alluxio.user.file.writetype.default</code></td>
        <td>The write type to use when creating files. Options are [MUST_CACHE, CACHE_THROUGH, 
THROUGH, ASYNC_THROUGH, ALL]. ALL will make the benchmark run with every write type.</td>
    </tr>
    <tr>
        <td>duration</td>
        <td>30s</td>
        <td>The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)</td>
    </tr>
    <tr>
        <td>conf</td>
        <td>{}</td>
        <td>Any HDFS client configuration key=value. Can repeat to provide multiple configuration values.</td>
    </tr>
    <tr>
        <td>skip-prepare</td>
        <td>false</td>
        <td>If true, skip the preparation phase.</td>
    </tr>
</table>

### Single node testing
#### Prerequisite
* A running Alluxio cluster with one master, and one worker.

#### Single test
Run the test with the operation you want to test.
```console
$ bin/alluxio runClass alluxio.stress.cli.StressMasterBench --operation ListDir ...
```

For example, this would continuously run `ListDir` operation for 30s and record the throughput after 5s warmup.
```console
$ bin/alluxio runClass alluxio.stress.cli.StressMasterBench --operation ListDir --warmup 5s --duration 30s 
```
#### MaxThroughput test
MaxThroughput test is the recommended way to test the master throughput for certain operation.
```console
$ bin/alluxio runClass alluxio.stress.cli.suite.MaxThroughput --operation CreateFile ...
```

For example, we are trying to get max throughput of CreateFile operation using hadoop compatible client.
```console
$ bin/alluxio runClass alluxio.stress.cli.suite.MaxThroughput --operation CreateFile --warmup 5s 
--duration 30s --client-type AlluxioHDFS
```


### Cluster testing
#### Prerequisite
- A running Alluxio cluster. Each worker node contains at least one worker, one job worker.
- UFS has enough space to store the test data if you are testing operations that involve file creation.

#### Testing
The cluster testing is similar to single node testing except that
- `--cluster` argument needs to be added to each operation so that the bench jobs will be submitted to job master, distributed to job workers, and executed by job workers.
  Each job worker executes one bench job. Cluster throughput is calculated by aggregating the read throughput of each bench job.

### Limitations
- Master Stress Bench only supports testing self-generated test files.

## Worker Stress Bench

The Worker Stress Bench is a tool to measure the IO performance of reading from Alluxio Worker.

### Parameters
The parameters for the Worker Stress Bench are (other than common parameters for any stress bench):

<table class="table table-striped">
    <tr>
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>client-type</td>
        <td>AlluxioHDFS</td>
        <td>The client API type. Alluxio native or hadoop compatible client</td>
    </tr>
    <tr>
        <td>clients</td>
        <td>1</td>
        <td>The number of fs client instances to use</td>
    </tr>
    <tr>
        <td>threads</td>
        <td>256</td>
        <td>The number of threads to use</td>
    </tr>
    <tr>
        <td>base</td>
        <td>alluxio://localhost:19998/stress-worker-base</td>
        <td>The base directory path URI to perform operations in. </td>
    </tr>
    <tr>
        <td>base-alias</td>
        <td></td>
        <td>The alias for the base path, unused if empty</td>
    </tr>
    <tr>
        <td>file-size</td>
        <td>128m</td>
        <td>The files size for IO operations. (100k, 1m, 1g, etc.)</td>
    </tr>
    <tr>
        <td>buffer-size</td>
        <td>4k</td>
        <td>The buffer size for IO operations. (1k, 16k, etc.)</td>
    </tr>
    <tr>
        <td>block-size</td>
        <td>32m</td>
        <td>The block size of files. (16k, 64m, etc.)</td>
    </tr>
    <tr>
        <td>free</td>
        <td>false</td>
        <td>If true, free the data from Alluxio before reading. Only applies to Alluxio paths</td>
    </tr>
    <tr>
        <td>conf</td>
        <td>{}</td>
        <td>Any HDFS client configuration key=value. Can repeat to provide multiple configuration values.</td>
    </tr>
    <tr>
        <td>write-type</td>
        <td>value of <code>alluxio.user.file.writetype.default</code></td>
        <td>The write type to use when creating files. Options are [MUST_CACHE, CACHE_THROUGH, 
THROUGH, ASYNC_THROUGH, ALL]. ALL will make the benchmark run with every write type.</td>
    </tr>
</table>

### Single node testing
#### Prerequisite
* A running Alluxio cluster with one master, and one worker.

#### Single test
Run the test with the operation you want to test.
```console
$ bin/alluxio runClass alluxio.stress.cli.StressWorkerBench ...
```


### Cluster testing
#### Prerequisite
- A running Alluxio cluster. Each worker node contains one worker, one job worker.
- Workers and UFS has enough space to store the test data if you are testing operations that involve file creation.

#### Testing
The cluster testing is similar to single node testing except that
- `--cluster` argument needs to be added to each operation so that the bench jobs will be submitted to job master, distributed to job workers, and executed by job workers.
  Each job worker executes one bench job. Cluster throughput is calculated by aggregating the read throughput of each bench job.

### Limitations
- Worker Stress Bench only supports testing self-generated test files.


## Max File Stress Bench

The Max File Stress Bench is designed to measure the maximum number of empty files that can be held in Alluxio metastore.

### Parameters
Unlike other stress benches, the Max File Stress Bench has only a limited number of configurable options:

<table class="table table-striped">
    <tr>
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>base</td>
        <td>alluxio://localhost:19998/stress-master-base</td>
        <td>The base directory path URI to perform operations in. </td>
    </tr>
    <tr>
        <td>base-alias</td>
        <td></td>
        <td>The alias for the base path, unused if empty</td>
    </tr>
    <tr>
        <td>clients</td>
        <td>1</td>
        <td>The number of fs client instances to use</td>
    </tr>
    <tr>
        <td>conf</td>
        <td>{}</td>
        <td>Any HDFS client configuration key=value. Can repeat to provide multiple configuration values.</td>
    </tr>
    <tr>
        <td>create-file-size</td>
        <td>0</td>
        <td>The size of a file for the Create op, allowed to be 0. (0, 1m, 2k, 8k, etc.)</td>
    </tr>
    <tr>
        <td>threads</td>
        <td>256</td>
        <td>The number of concurrent threads to use</td>
    </tr>
</table>

### Single node testing
#### Prerequisite
A running Alluxio cluster with one master and one worker.

#### Single test
```console
$ bin/alluxio runClass alluxio.stress.cli.MaxFileBench ...
```
### Cluster testing
This is only useful if the throughput provided by running this stress bench on the master node directly is insufficient.

#### Prerequisite
A running Alluxio cluster. Each worker node contains one worker, one job worker.

#### Testing
Use the `--cluster` flag. 

### Expected runtime and ending condition
Because this stress bench creates file until the master metastore is full, it takes a long time to run. Expect several hours of runtime depending 
on which metastore you have configured and how much heap space / disk space is available to said metastore. 

The stress bench ends after many create operations fail successively. Each failure is followed by an increasingly long timeout. This means that if
your network is particularly slow, this stress bench could end prematurely. 

## UFS IO Bench

The UFS IO Bench is a tool to measure the IO throughput between the Alluxio cluster and the UFS.This test will measure the I/O throughput between Alluxio workers and the specified UFS path. 
Each worker will create concurrent clients to first generate test files of the specified size then read those files. The write/read I/O throughput will be measured in the process.

### Parameters
The parameters for the Worker Stress Bench are (other than common parameters for any stress bench):

<table class="table table-striped">
    <tr>
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>threads</td>
        <td>4</td>
        <td>The number of threads to use</td>
    </tr>
    <tr>
        <td>io-size</td>
        <td>4G</td>
        <td> Size of data to write and then read for each thread. </td>
    </tr>
    <tr>
        <td>path</td>
        <td>No default, Required</td>
        <td>the UFS Path to write temporary data in</td>
    </tr>
    <tr>
        <td>use-mount-conf</td>
        <td>false</td>
        <td>If true, attempt to load the UFS configuration from an existing mount point to read/write to the base path, it will override the configuration specified through --conf parameter</td>
    </tr>
    <tr>
        <td>conf</td>
        <td>{}</td>
        <td>Any HDFS client configuration key=value. Can repeat to provide multiple configuration values.</td>
    </tr>


</table>

### Single node testing
#### Prerequisite
* A running Alluxio cluster with one master, and one worker.

#### Single test
Run the test with the operation you want to test.
```console
$ bin/alluxio runClass alluxio.stress.cli.UfsIOBench --io-size 1G ...
```

For example, this runs the UFS I/O benchmark in the Alluxio cluster, where each thread is writing and then reading 512MB of data from HDFS:
```console
$ bin/alluxio runUfsIOTest --path hdfs://<hdfs-address> --io-size 512m --threads 2 
```


### Cluster testing
#### Prerequisite
- A running Alluxio cluster. Each worker node contains at least one worker, one job worker.
- UFS has enough space to store the test data if you are testing operations that involve file creation.
#### Testing
The cluster testing is similar to single node testing except that
- `--cluster` argument needs to be added to each operation so that the bench jobs will be submitted to job master, distributed to job workers, and executed by job workers.
  Each job worker executes one bench job. Cluster throughput is calculated by aggregating the read throughput of each bench job.

### Limitations
- Ufs IO Bench only supports testing self-generated test files.

## RPC Stress Bench

The RPC Stress Bench is a set of benchmarks designed to measure the RPC performance of the 
master under heavy load of concurrent client/worker requests.

### Single node mode VS cluster mode

Similar to the Fuse IO Stress Bench, the RPC benchmarks support running in a single node mode, 
or in a cluster mode. 

In the single node mode, the node running the benchmark uses multiple threads to simulate many 
clients/workers which send concurrent RPC requests to the master.

In the cluster mode, the benchmarks leverage the job service to create simulated clients/workers. 
Each job worker hosts many simulated clients/workers on many threads, so with this mode it's 
possible to achieve higher number of parallel RPC requests, as opposed to the single node mode 
where the parallelism width is limited by the number of cores available on that node.

Use the common option `--cluster` to specify the cluster mode, otherwise omit it to use the 
single node mode. Use the `--cluster-limit` option to further specify how many job workers 
should be used to run the benchmark job in parallel.

For example, this runs the `RegisterWorkerBench` in the single node mode, with a total of 8
simulated workers:

```console
$ bin/alluxio runClass alluxio.stress.cli.RegisterWorkerBench --concurrency 8 --tiers "5000,5000"
```

This runs the same benchmark in the cluster mode, on 2 job workers each simulating 4 workers,
so the total number of simulated workers is also 8:

```console
$ bin/alluxio runClass alluxio.stress.cli.RegisterWorkerBench --concurrency 4 --cluster 
--cluster-limit 2 --tiers "5000,5000"
```

### Preparing & Sending RPCs

Each individual RPC benchmark tests a specific RPC service offered by the master. A benchmark 
may perform necessary preparations to create an environment that simulates real use cases before 
the measurement begins.

Then the benchmark sends the RPC request(s) to the master once or repeatedly, depending on 
the benchmark, and waits for the response(s). If the benchmark calls RPCs repeatedly, it will stop 
when the specified measurement duration has been reached. Otherwise, it stops as soon as the 
response is received.
One successful RPC request results in one data point that records the round trip time of the RPC.

For example, the `GetPinnedFileIdsBench` measures the RPC throughput of the `GetPinnedFileIds` RPC. 
It creates test files in a temporary directory in Alluxio and pins them. Then the simulated workers repeatedly call 
the RPC and waits for the pinned file list, records and outputs the data points and some statistics
of the data, until the duration has been reached. The number of data points in the result is 
therefore linear to the duration of the benchmark.

On the other hand, the `RegisterWorkerBench` only calls the RPC once. It first prepares the 
metadata for many fake blocks on the master. Then the simulated workers register themselves with 
the master, reporting the fake blocks in its storage. 
Each worker sends the registration RPC only once. Therefore, the duration parameter is irrelevant 
for this benchmark, and the number of data points is equal to the number of workers (if no 
errors occur).

### Output & Logging

The benchmarks output the results in JSON to stdout.

The benchmarks log progress and errors to
`${alluxio.user.logs.dir}/user_${user.name}.log` by default. 
You can redirect the logs to a different file each time a benchmark is run, 
by setting the following options on the command line when invoking the benchmarks. For example:

```console
$ bin/alluxio runClass <path.to.bench.class> \
    -Dlog4j.appender.USER_LOGGER.File=/path/to/benchmark/results/run1.log [other options]
```

### Common Parameters

These common parameters are available to all RPC benchmarks:

<table class="table table-striped">
    <tr>
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>concurrency</td>
        <td>2</td>
        <td>The number of simulated clients on each node. Typically, benchmarks use 1 thread for 1 
        simulated client.</td>
    </tr>
    <tr>
        <td>duration</td>
        <td>5s</td>
        <td>How long the benchmark should be recording measurements for.</td>
    </tr>
</table>

### Available benchmarks

These RPC benchmarks are currently available:

- [`GetPinnedFileIdsBench`](#getpinnedfileidsbench)
- [`RegisterWorkerBench`](#registerworkerbench)
- [`WorkerHeartbeatBench`](#workerheartbeatbench)
- [`StreamRegisterWorkerBench`](#streamregisterworkerbench)

#### `GetPinnedFileIdsBench`

Simulates workload for the `GetPinnedFileIds` RPC, and calls the RPC repeatedly 
until duration is reached.

`GetPinnedFileIds` is periodically called by block workers on every heartbeat to poll the 
pinned file list. It can be expensive if the pinned file list is huge. 

Parameters:

<table class="table table-striped">
    <tr>
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>num-files</td>
        <td>100</td>
        <td>Number of pinned files. Use this parameter to adjust the load incurred by the RPC. 
            Higher numbers incur higher loads.</td>
    </tr>
    <tr>
        <td>base-dir</td>
        <td>/get-pin-list-bench-base</td>
        <td>The temporary directory to contain the test files.</td>
    </tr>
</table>

#### `RegisterWorkerBench` 

Simulates workload for the `RegisterWorker` RPC.

This benchmark simulates concurrent worker registration using the unary RPC implementation (default 
before Alluxio 2.7).
You may use this command to simulate the pressure when the workers start at once at your scale.

The`RegisterWorker` RPC carries a list of blocks that is currently stored in the worker's storage.
This RPC can be expensive if the list is huge.

Note that Alluxio 2.7 introduced the [register lease]({{ '/en/operation/Scalability-Tuning.html' | relativize_url }}#worker-register-lease)
for the master to perform registration flow control.
If you enable that, the same flow control will be effective in this test too. 

This benchmark calls the RPC only once.

See also: [StreamRegisterWorkerBench](#streamregisterworkerbench).

Parameters:

<table class="table table-striped">
    <tr>
        <th>Parameter</th>
        <th>Default Value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>tiers</td>
        <td>n/a</td>
        <td>A semicolon-separated list of storage tiers. Each tier is a comma-separated 
            list of numbers of blocks in directories on that tier. At most 3 tiers are supported. 
            Example: `100,200,300;1000,1500;2000`</td>
    </tr>
</table>

#### `WorkerHeartbeatBench`

For the `WorkerHeartbeat` RPC.

`WorkerHeartbeat` is called on every worker heartbeat. This RPC carries the block update on this 
worker since the last heartbeat. If the workers at your scale are under high load, the heartbeat 
message will also be large and incurs significant overhead on the master side.

This benchmark calls the RPC repeatedly until duration is reached.

Parameters:

Same as [`RegisterWorkerBench`](#registerworkerbench).

#### `StreamRegisterWorkerBench`

For the `RegisterWorkerStream` RPC.

This benchmark calls the RPC only once.

Simulates concurrent worker registration using the 
[streaming RPC implementation]({{ '/en/operation/Scalability-Tuning.html' | relativize_url }}#streaming-worker-registration)
(default since Alluxio 2.7).
You may use this command to simulate the pressure when the workers start at once at your scale.

Note that Alluxio 2.7 introduced the [register lease]({{ '/en/operation/Scalability-Tuning.html' | relativize_url }}#worker-register-lease)
for the master to perform registration flow control. If you enable that, the same flow control
will be effective in this test too. 

See also: [RegisterWorkerBench](#registerworkerbench).

Parameters:

Same as [`RegisterWorkerBench`](#registerworkerbench).


## Batch Tasks

Many benchmarks support a number of different operations or modes. It can be repetitive to run the
same benchmark many times, each time only with a different operation. Batch tasks are predefined
combinations of common benchmark operations, and they allow you to easily test the 
various aspects of performance of a system. 

Batch tasks are simple wrappers around existing benchmarks. The following batch tasks are available:
* `MasterComprehensiveFileBatchTask` - A preset combination of master file system operations.

To run the batch tasks, run
```console
$ bin/alluxio runClass alluxio.stress.cli.BatchTaskRunner <TASK_NAME> 
```

### Master Comprehensive File Batch Task

The `MasterComprehensiveFileBatchTask` is a task that simulates the common workflow that creates 
a file, gets file status, opens file for reading, and finally deletes the file.
It runs the following master bench operations in order:

1. CreateFile
2. ListDir
3. ListDirLocated
4. GetBlockLocations
5. GetFileStatus
6. OpenFile
7. DeleteFile

#### Parameters

It supports all common parameters, and a subset of the master bench parameters:

* `--base`
* `--threads`
* `--stop-count`
* `--target-throughput`
* `--warmup`
* `--create-file-size`
* `--write-type`
* `--clients`
* `--client-type`
* `--read-type`
* `--fixed-count`

#### Example

This example runs the master comprehensive file batch task which operates on 1000 files with 10 
threads:

```console
bin/alluxio runClass alluxio.stress.cli.BatchTaskRunner MasterComprehensiveFileBatchTask \ 
--num-files 1000 \
--threads 10 \
--create-file-size 1k \ 
--base alluxio:///stress-master-base \
--warmup 0s
```
