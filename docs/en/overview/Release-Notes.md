---
layout: global
title: Release Notes
group: Overview
priority: 7
---

November 16, 2022

This is the first release on the Alluxio 2.9.X line. This release introduces a feature for fine-grained caching of data, metrics for monitoring the master process health, changes to the default configuration to better handle master failover and journal backups. Multiple improvements and fixes were also made for the S3 API, helm charts, and POSIX API.

* Table of Contents
{:toc}

## Highlights

### Paging Storage on Workers

The Alluxio workers support fine-grained page-level caching, typically at the 1 MB size, as an alternative to the existing block-based tiered caching, which defaults to 64 MB. Through this feature, caching performance will be improved by reducing amplification of data read by applications. See the [documentation]( {{ '/en/core-services/Caching.html#experimental-paging-worker-storage' | relativize_url}}) for more details.

### Master Monitoring Metrics

The Alluxio master periodically checks its resource usage, including CPU and memory usage, and several internal data structures that are performance critical. By inspecting snapshots of resource utilization metrics, the state of the system can be inferred, which can be retrieved by inspecting the `master.system.status` metric. The possible statuses are:
* IDLE
* ACTIVE
* STRESSED
* OVERLOADED

The monitoring indicators describe the system status in a heuristic way to have a basic understanding of its load. See the [documentation]({{ '/en/administration/Troubleshooting.html#master-internal-monitoring' | relativize_url }}) for more information about monitoring.

### Journal and Failover Stability

The default configuration as of 2.9.0 skips the block integrity check upon master startup and failover ([a493b69e2d](https://github.com/Alluxio/alluxio/commit/a493b69e2d){:target="_blank"}). This speeds up failovers considerably to minimize system downtime during master leadership transfers. Instead, block integrity checks will be performed in the background periodically so as not to interfere with normal master operations.

Another default configuration change will delegate the journal backup operation to a standby master ([e3ed7b674f](https://github.com/Alluxio/alluxio/commit/e3ed7b674f){:target="_blank"}) so as to not block the leading master’s operations for an extended period of time. Use the `--allow-leader` flag to allow the leading master to also take a backup or force the leader to take a backup with the `--bypass-delegation` flag. See the [documentation]({{ '/en/operation/Journal.html#backup-delegation-on-ha-cluster' | relativize_url}}) for additional information about backup delegation.

## Improvements and Bugfixes Since 2.8.1

### Notable Configuration Property Changes

<table class="table table-striped">
    <tr>
        <th>Property Key</th>
        <th>Old 2.8.1 value</th>
        <th>New 2.9.0 value</th>
    </tr>
    <tr>
        <td markdown="span">`alluxio.master.metrics.heap.enabled`</td>
        <td markdown="span">true</td>
        <td markdown="span">false</td>
    </tr>
    <tr>
        <td markdown="span">`alluxio.master.periodic.block.integrity.check.repair`</td>
        <td markdown="span">false</td>
        <td markdown="span">true</td>
    </tr>

<tr>
        <td markdown="span">`alluxio.master.startup.block.integrity.check.enabled`</td>
        <td markdown="span">true</td>
        <td markdown="span">false</td>
    </tr>

</table>

### Metadata and Journal
* Add CLI for marking a path as needing sync with UFS ([1c781f7de1](https://github.com/Alluxio/alluxio/commits/1c781f7de1){:target="_blank"})
* Make metadata sync work with merge inode journal feature flag ([7ff9df2789](https://github.com/Alluxio/alluxio/commits/7ff9df2789){:target="_blank"})
* Make journal context thread safe ([f5b2a5f438](https://github.com/Alluxio/alluxio/commits/f5b2a5f438){:target="_blank"})
* Fix error in snapshot-taking when using large group ids ([2803dc4603](https://github.com/Alluxio/alluxio/commits/2803dc4603){:target="_blank"})
* Mark root as needing sync on backup restore ([0fe867ac72](https://github.com/Alluxio/alluxio/commits/0fe867ac72){:target="_blank"})
* Make `MountTable.State` thread safe ([45ce753499](https://github.com/Alluxio/alluxio/commits/45ce753499){:target="_blank"})
* Avoid canceling duplicate metadata sync prefetch job ([aacee53fbc](https://github.com/Alluxio/alluxio/commits/aacee53fbc){:target="_blank"})
* Support multithread checkpointing with compression/decompression ([ae065e34b9](https://github.com/Alluxio/alluxio/commits/ae065e34b9){:target="_blank"})
* Avoid and dedup concurrent metadata sync ([025ca19d09](https://github.com/Alluxio/alluxio/commits/025ca19d09){:target="_blank"})
* Refine add `mPendingPaths` in `InodeSyncStream` new type ([749f70cd1a](https://github.com/Alluxio/alluxio/commits/749f70cd1a){:target="_blank"})
* Fix single master embedded journal checkpoint ([a115fa3d5b](https://github.com/Alluxio/alluxio/commits/a115fa3d5b){:target="_blank"})
* Create inode before updating the `MountTable` ([56d1c6a9bc](https://github.com/Alluxio/alluxio/commits/56d1c6a9bc){:target="_blank"})
* Allow root sync path to use child sync time ([4d6bce3d5f](https://github.com/Alluxio/alluxio/commits/4d6bce3d5f){:target="_blank"})
* Add client operation for partial listing ([bc6e63f7f8](https://github.com/Alluxio/alluxio/commits/bc6e63f7f8){:target="_blank"})
* Determine the primary master address by calling `GetNodeState` endpoint ([c63ec951e4](https://github.com/Alluxio/alluxio/commits/c63ec951e4){:target="_blank"})
* Upgrade Apache Ratis from 2.0.0 to 2.3.0 ([dc0a21daed](https://github.com/Alluxio/alluxio/commits/dc0a21daed){:target="_blank"})
* Merge journals & flush journals before lock release ([8dafc272be](https://github.com/Alluxio/alluxio/commits/8dafc272be){:target="_blank"})
* Add Partial listing of files in `listStatus` ([5f50dd8ab3](https://github.com/Alluxio/alluxio/commits/5f50dd8ab3){:target="_blank"})
* Improve error handling and naming on journal threads ([e69adee025](https://github.com/Alluxio/alluxio/commits/e69adee025){:target="_blank"})
* Add Partial listing of files in `listStatus` ([ec0ce2a656](https://github.com/Alluxio/alluxio/commits/ec0ce2a656){:target="_blank"})
* Fix journal shutdown deadlock ([155a370fbe](https://github.com/Alluxio/alluxio/commits/155a370fbe){:target="_blank"})
* Allow writing to read-only file when creating ([fe27139f6c](https://github.com/Alluxio/alluxio/commits/fe27139f6c){:target="_blank"})
* Avoid checking file permissions in `getFileInfo` method ([54494af052](https://github.com/Alluxio/alluxio/commits/54494af052){:target="_blank"})
* Fix master down when master change to leader ([1ada2ac8c7](https://github.com/Alluxio/alluxio/commits/1ada2ac8c7){:target="_blank"})

### Cache and Storage
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### S3 API and Proxy
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### Kubernetes and Docker
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### FUSE/POSIX API
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### UFS
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### CLI
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### Error and Exception Handling
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### Metrics and Monitoring
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### StressBench and MicroBench
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### Deprecations
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### Miscellaneous
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})


## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.9.3 release. Especially, we would like to thank:

([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
([](){:target="_blank"}),
and ([](){:target="_blank"})

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).