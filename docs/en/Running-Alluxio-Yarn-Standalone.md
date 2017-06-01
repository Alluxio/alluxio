---
layout: global
title: Running Alluxio Standalone with YARN
nickname: Alluxio Standalone with YARN
group: Deploying Alluxio
priority: 6
---

Alluxio should be run alongside YARN so that all YARN nodes have access to a local Alluxio worker.
For YARN and Alluxio to play nicely together on the same nodes, it's important to inform YARN of
the resources used by Alluxio. YARN needs to know how much memory and cpu to leave for Alluxio.

## Allocating Resources for Alluxio

### Alluxio Worker Memory

The Alluxio worker requires some memory for its JVM process and some memory for its ramdisk.
1GB is generally fine for the JVM memory since this memory is only used for buffering and metadata.
If the worker is using a ramdisk for memory-speed data, the memory allocated to the ramdisk should
be added to the 1GB JVM memory to get the total memory needed by the worker. The amount of memory used
by the ramdisk is controlled by `alluxio.worker.memory.size`. Data stored in non-memory tiers such as
SSD or HDD does not need to be included in the memory size calculation.

### Alluxio Master Memory

The Alluxio master stores metadata about every file in Alluxio, so it needs a proportional amount
of memory. This should be at least 1GB, and we recommend using at least 32GB in a production deployment.

### CPU vcores

One vcore should be adequate for Alluxio workers. We recommend at least 4 vcores for Alluxio masters
in a production deployment.

### YARN Configuration

To inform YARN of the resources to reserve for Alluxio on each node, modify the YARN configuration
parameters `yarn.nodemanager.resource.memory-mb` and `yarn.nodemanager.resource.cpu-vcores` in
`yarn-site.xml`. After determining how much memory to allocate to Alluxio on the node, subtract this from
`yarn.nodemanager.resource.memory-mb` and update the parameter with the new value. The same should be done
for `yarn.nodemanager.resource.cpu-vcores`.

After updating the YARN configuration, restart YARN so that it picks up the changes:

```bash
$ ${HADOOP_HOME}/sbin/stop-yarn.sh
$ ${HADOOP_HOME}/sbin/start-yarn.sh
```
