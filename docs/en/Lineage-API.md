---
layout: global
title: Lineage Client API (alpha)
nickname: Lineage API
group: Features
priority: 2
---

* Table of Contents
{:toc}

Alluxio can achieve high throughput writes and reads, without compromising fault-tolerance by using
*Lineage*, where lost output is recovered by re-executing the jobs that created the output.

With lineage, applications write output into memory, and Alluxio periodically checkpoints the output
into the under file system in an asynchronous fashion. In case of failures, Alluxio launches *job
recomputation* to restore the lost files. Lineage assumes that jobs are deterministic so that the
recomputed outputs are identical. If this assumption is not met, it is up to the application to
handle divergent outputs.

# Enable Lineage

By default, lineage is not enabled. It can be enabled by setting the
`alluxio.user.lineage.enabled` property to `true` in the configuration file.

# Lineage API

Alluxio provides a Java like API for managing and accessing lineage information.

### Getting a Lineage Client

To obtain a Alluxio Lineage Client in Java code, use:

```java
AlluxioLineage tl = AlluxioLineage.get();
```

### Creating a Lineage Record

Lineage can be created by calling
`AlluxioLineage#createLineage(List<AlluxioURI>, List<AlluxioURI>, Job)`. A lineage record takes (1)
a list of URIs of the input files, (2) a list of URIs of the output files, and (3) a *job*. A job
is description of a program that can be run by Alluxio to recompute the output files given the input
files. *Note: In the current alpha version, only a built-in `CommandLineJob` is supported, which
simply takes a command String that can be run in a terminal. The user needs to provide the necessary
configurations and execution environments to ensure the command can be executed both at the client
and at Alluxio master (during recomputation).*

For example,
<!---
NOTE: This code is tested in LineageMasterIntegrationTest, so if you update it here make sure to
update it there as well.
-->

```java
AlluxioLineage tl = AlluxioLineage.get();
// input file paths
AlluxioURI input1 = new AlluxioURI("/inputFile1");
AlluxioURI input2 = new AlluxioURI("/inputFile2");
List<AlluxioURI> inputFiles = Lists.newArrayList(input1, input2);
// output file paths
AlluxioURI output = new AlluxioURI("/outputFile");
List<AlluxioURI> outputFiles = Lists.newArrayList(output);
// command-line job
JobConf conf = new JobConf("/tmp/recompute.log");
CommandLineJob job = new CommandLineJob("my-spark-job.sh", conf);
long lineageId = tl.createLineage(inputFiles, outputFiles, job);
```

The `createLineage` function returns the id of the newly created lineage record. Before creating a
lineage record, make sure that all the input files are either persisted, or specified as an output
file in another lineage record.

### Specifying Operation Options

For all `AlluxioLineage` operations, an additional `options` field may be specified, which allows
users to specify non-default settings for the operation.

### Deleting a Lineage

Lineage records can be deleted by calling `AlluxioLineage#deleteLineage`. The deletion function
takes the lineage id.

```java
AlluxioLineage tl = AlluxioLineage.get();
...
long lineageId = tl.createLineage(inputFiles, outputFiles, job);
...
tl.deleteLineage(lineageId);
```

By default, the lineage record to delete cannot have output files depended on by other lineages.
Optionally, all the downstream lineages can be deleted altogether by setting the cascade delete
flag. For example:

```java
AlluxioLineage tl = AlluxioLineage.get();
DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(true);
tl.deleteLineage(lineageId, options);
```

# Configuration Parameters For Lineage

These are the configuration parameters related to Alluxio's lineage feature.

<table class="table table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
<tr>
  <td>alluxio.master.lineage.checkpoint.interval.ms</td>
  <td>300000</td>
  <td>
  The interval (in milliseconds) between Alluxio's checkpoint scheduling.
  </td>
</tr>
<tr>
  <td>alluxio.master.lineage.checkpoint.class</td>
  <td>alluxio.master.lineage.checkpoint
    .CheckpointLatestPlanner</td>
  <td>
  The class name of the checkpoint strategy for lineage output files. The default strategy is to
  checkpoint the latest completed lineage, i.e. the lineage whose output files are completed.
  </td>
</tr>
<tr>
  <td>alluxio.master.lineage.recompute.interval.ms</td>
  <td>300000</td>
  <td>
  The interval (in milliseconds) between Alluxio's recompute execution. The executor scans all the lost files tracked by lineage, and re-executes the corresponding jobs every 5 minutes.
  </td>
</tr>
<tr>
  <td>alluxio.master.lineage.recompute.log.path</td>
  <td>${alluxio.home}/logs/recompute.log</td>
  <td>
  The path to the log that the recompute executor redirects the job's stdout into.
  </td>
</tr>
<tr>
  <td>alluxio.user.lineage.enabled</td>
  <td>false</td>
  <td>
  Flag to enable lineage feature.
  </td>
</tr>
</table>
