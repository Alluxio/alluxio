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
* ([](){:target="_blank"})

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