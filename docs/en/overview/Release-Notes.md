---
layout: global
title: Release Notes
group: Overview
priority: 7
---

January 20, 2023

* Table of Contents
{:toc}

## Highlights

### Improved Load Command

The load command in the Alluxio CLI ([db9f07](https://github.com/Alluxio/alluxio/commit/db9f07a50e1f7f6c45d511d591c8775ada6b71bb){:target="_blank"}) is updated to use a new infrastructure (different from the existing job service) to asynchronously load all files under the given directory path with better performance and stability. New command line arguments are available to enhance the operation’s usability, such as limiting the UFS bandwidth and running a verify step after the load operation is complete to check that the expected files are loaded correctly.

See the [CLI documentation]( {{'/en/operation/User-CLI.html#load' | relativize_url }}) for the full description of the updated load command.

### Monitor Helm Chart

This helm chart ([ca8132](https://github.com/Alluxio/alluxio/commit/ca81323975bf1910fc4455254f6988460874d975){:target="_blank"}) spawns a monitoring system based on Prometheus and Grafana upon deployment. It is able to monitor the status, metrics, and some other information of an Alluxio cluster on Kubernetes. Users can access the Grafana web UI through the Grafana web port.

See [README](https://github.com/Alluxio/alluxio/blob/release-2.9.1/integration/kubernetes/helm-chart/monitor/README.md){:target="_blank"} for details of deploying this monitoring system.

### Unsafe Flush Option for Embedded Journal

When using the embedded journal, each journal entry must be flushed to disk on all masters before being committed. This operation can be a performance bottleneck on slow or busy disks. The newly added property `alluxio.master.embedded.journal.unsafe.flush.enabled` ([3fe8e0](https://github.com/Alluxio/alluxio/commit/3fe8e090e90f8fac6cde2ced4db9976a82213839){:target="_blank"}) allows the system to continue without waiting for the flush to complete, but at the risk of losing data if half the master nodes fail. The [documentation]( {{ '/en/administration/Performance-Tuning.html#embedded-journal-write-performance' | relativize_url }}) discusses other safer ways to alleviate this performance bottleneck.

## Compression Level Option for RocksDB Checkpoint

In order for the system to recover quickly after failures or restarts, checkpoints of the system are taken at every 2 million journal entries by default. The checkpoints of the metadata in RocksDB are compressed to reduce their size. The property `alluxio.master.metastore.rocks.checkpoint.compression.level` ([61f5af](https://github.com/Alluxio/alluxio/commit/61f5af80c6376f585e2edff0e50a1577597d17fa){:target="_blank"}) allows the user to set a compression level for these checkpoints (0 for no compression, 9 for maximum compression). A value of 1 is recommended as higher levels give little benefit in terms of amount of compression at the cost of a large increase in computation.

## Improvements and Bugfixes Since 2.9.0

Notable configuration property changes:

<table class="table table-striped">
    <tr>
        <th>Property Key</th>
        <th>Old 2.9.0 value</th>
        <th>New 2.9.1 value</th>
    </tr>
    <tr>
        <td markdown="span">`alluxio.worker.fuse.mount.options`</td>
        <td markdown="span">direct_io</td>
        <td markdown="span">attr_timeout=600, entry_timeout=600
</td>
    </tr>

</table>

Master
* ([](){:target="_blank"})([](){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.9.3 release. Especially, we would like to thank:

([](){:target="_blank"})

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).