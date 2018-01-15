---
layout: global
title: Running Alluxio Standalone with YARN
nickname: Alluxio Standalone with YARN
group: Deploying Alluxio
priority: 6
---

Alluxio should be run alongside YARN so that all YARN nodes have access to a local Alluxio worker.
For YARN and Alluxio to play nicely together on the same nodes, you must partition the node's
memory and compute resources between YARN and Alluxio. To do this, first determine how much memory
and cpu will be needed by Alluxio, then subtract those resources from what YARN would otherwise use.

## Memory

In `yarn-site.xml`, set

```
yarn.nodemanager.resource.memory-mb = Total RAM - Other services RAM - Alluxio RAM
```

On Alluxio worker nodes, Alluxio RAM usage is 1GB plus the ramdisk size configured by
`alluxio.worker.memory.size`.
On Alluxio master nodes, Alluxio RAM usage is proportional to the number of files. Allocate at
least 1GB. 32GB is recommended for a production deployment.

## CPU vcores

In `yarn-site.xml`, set

```
yarn.nodemanager.resource.cpu-vcores = Total cores - Other services vcores - Alluxio vcores
```

On Alluxio worker nodes, Alluxio needs only one or two vcores.
On Alluxio master nodes, we recommend at least 4 vcores.

## Restart YARN

After updating the YARN configuration, restart YARN so that it picks up the changes:

```bash
$ ${HADOOP_HOME}/sbin/stop-yarn.sh
$ ${HADOOP_HOME}/sbin/start-yarn.sh
```
