---
layout: global
title: Job Service
group: Overview
priority: 3
---

* Table of Contents
{:toc}

## Architecture Overview

The Alluxio Job Service is a task scheduling framework responsible for assigning a
number of different types of operations to Job Workers. 

The Master is responsible for distributing a job into smaller tasks for the Job Workers
to execute and managing the status of the job's completion.

The Job Workers queues tasks from the Job Master and manages a configurable fixed threadpool 
(`alluxio.job.worker.threadpool.size`) to complete these tasks.

## Different Types of Jobs

### Load

Load Job is used as part of the `fs distributedLoad` CLI command and loads a single file to Alluxio 
with specified amount of replication

### Migrate

Migrate Job is used as part of the `fs distributedCp` and `fs distributedMv` CLI command and is responsible
for copying/moving with a specified [WriteType]({{ '/en/overview/Architecture.html#data-flow-write' | relativize_url }}). 

### Persist

Persist Job is used both indirectly as part of the `fs persist` CLI command and background 
persisting when writing to Alluxio with ASYNC_THROUGH 
[WriteType]({{ '/en/overview/Architecture.html#data-flow-write' | relativize_url }}).

The job is responsible for persisting a file in Alluxio to a particular ufs path. 

### Evict

Evict Job is indirectly used by `fs free` CLI command and replication background process.

The job is responsible for evicting a specified number of replicated blocks from Alluxio.

### Move

Move Job is used by the replication background process to move blocks from one worker to another.

### Replicate

Replicate job is used by the replication background process to replicate blocks from a worker 
to a specified number of other workers.

### Data Transformation

The job service also supports data transformation. More details can be found at 
[Data Transformations]({{ '/en/core-services/Transformation.html' | relativize_url }})

## Inspection Commands

The job service provides a number of inspection commands.

### fsadmin report jobservice

`fsadmin report jobservice` will report a summary of the job service.

```console
$ ./bin/alluxio fsadmin report jobservice
Worker: MigrationTest-workers-2  Task Pool Size: 10     Unfinished Tasks: 1303   Active Tasks: 10     Load Avg: 1.08, 0.64, 0.27
Worker: MigrationTest-workers-3  Task Pool Size: 10     Unfinished Tasks: 1766   Active Tasks: 10     Load Avg: 1.02, 0.48, 0.21
Worker: MigrationTest-workers-1  Task Pool Size: 10     Unfinished Tasks: 1808   Active Tasks: 10     Load Avg: 0.73, 0.5, 0.23

Status: CREATED   Count: 4877
Status: CANCELED  Count: 0
Status: FAILED    Count: 1
Status: RUNNING   Count: 0
Status: COMPLETED Count: 8124

10 Most Recently Modified Jobs:
Timestamp: 10-28-2020 22:02:34:001       Id: 1603922371976       Name: Persist             Status: COMPLETED
Timestamp: 10-28-2020 22:02:34:001       Id: 1603922371982       Name: Persist             Status: COMPLETED
(only a subset of the results is shown)

10 Most Recently Failed Jobs:
Timestamp: 10-24-2019 17:15:22:946       Id: 1603922372008       Name: Persist             Status: FAILED

10 Longest Running Jobs:
```

### job ls

`job ls` will list jobs that are running or have run on the job service.

```console
$ ./bin/alluxio job ls
1613673433925   Persist    COMPLETED
1613673433926   Persist    COMPLETED
1613673433927   Persist    COMPLETED
1613673433928   Persist    COMPLETED
1613673433929   Persist    COMPLETED
```

### job stat -v <job_id> 

`job stat -v <job_id>` will list detailed information about a job. 
(Adding -v includes information about the individual tasks on the workers)

```console
bin/alluxio job stat -v 1613673433929
ID: 1613673433929
Name: Persist
Description: PersistConfig{filePath=/test5/lib/alluxio-underfs-cosn-2.5.0-SNAPSHOT.jar, mountId=1, overwrite=false, ufsPath=...
Status: COMPLETED
Task 0
	Worker: 192.168.42.71
	Status: COMPLETED
```
