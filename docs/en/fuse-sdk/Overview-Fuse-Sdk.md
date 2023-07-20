---
layout: global
title: FUSE SDK Overview
---

The Alluxio POSIX API is a feature that allows mounting training datasets
in specific storage services (e.g. S3, HDFS) to the local filesystem
and provides caching capabilities to speed up I/O access to frequently used data.

## Local Cache vs Distributed Cache

There are two kinds of caching capabilities: 1. local caching only 2. local caching + distributed caching.

Differences between the two solutions are listed below, choose your desired solution based on training requirements and available resources.
<table class="table table-striped">
    <tr>
        <th>Category</th>
        <th>Local Caching</th>
        <th>Distributed Caching</th>
    </tr>
    <tr>
        <td>Prerequisite</td>
        <td>N/A</td>
        <td>Require a running Alluxio cluster (master + worker)</td>
    </tr>
    <tr>
        <td>Caching Capability</td>
        <td>Bounded by local storage size</td>
        <td>Bounded by Alluxio cluster storage size</td>
    </tr>
    <tr>
        <td>Suited Workloads</td>
        <td>Single node training with large dataset. Distributed training with no data shuffle between nodes</td>
        <td>Multiple training nodes or training tasks share the same dataset</td>
    </tr>
</table>

### Local Caching Solution

See [Local Cache Quick Start]({{ '/en/fuse-sdk/Local-Cache-Quick-Start.html' | relative_url }}) for quickly setup your FUSE SDK local cache solution
which can connects to your desired storage services.

[Local Cache Overview]({{ '/en/fuse-sdk/Local-Cache.html' | relative_url }}) provides different local cache capabilities
to speed up your workloads and reduce the pressure of storage services.

[Advanced Tuning Guide]({{ '/en/fuse-sdk/Advanced-Tuning.html' | relative_url }}) provides advanced FUSE SDK tuning tips
for performance optimization or debugging.

### Distributed Caching Solution

FUSE SDK can connect to a shared distributed caching service. For more information, please refer to [Distributed Cache Quick Start]({{ '/en/fuse-sdk/FUSE-SDK-Dora-Quick-Start.html' | relative_url }})
