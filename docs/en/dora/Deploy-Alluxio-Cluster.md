---
layout: global
title: Deploy Alluxio Cluster
group: Dora
priority: 2
---

* Table of Contents
  {:toc}

This doc covers how to set up a Dora cluster, and perform IO operations. The configurations listed below need
to be the same on all Alluxio nodes.

## Enable Dora Distributed Cache

```properties
alluxio.alluxio.client.read.location.policy.enabled=true
```

This will enable the consistent hashing algorithm to distribute the load among Dora cache nodes.

## Disable short-circuit IO and worker register lease

```properties
alluxio.user.short.circuit.enabled=false
alluxio.master.worker.register.lease.enabled=false
```

These features are not supported in Dora and needs to be disabled for Dora to work.

## Enable client UFS fallback

```properties
alluxio.alluxio.client.ufs.root=<under_fs_uri>
```

This property specifies the UFS clients will fall back to, in the same way as the
`alluxio.master.mount.table.root.ufs` property specifies the UFS of the master root mount point.

To configure additionally UFS specific configurations, simply put them in the `alluxio-site.properties` file. Make sure
the configuration are the same across all Dora nodes.

For example, if the UFS is HDFS, and needs special configurations specified in `core-site.xml` and `hdfs-site.xml`,
specify the Alluxio property `alluxio.underfs.hdfs.configuration` directly. The documentation on
[configuring HDFS](https://github.com/Alluxio/alluxio/blob/dora/docs/en/ufs/HDFS.md#specify-hdfs-configuration-location) suggests using
the Master mount point option starting with `alluxio.master.mount.table.root.option`. This is currently not supported
by Dora nodes.

## Cache storage

Configure the cache storage used by each Dora cache nodes:

```properties
alluxio.worker.block.store.type=PAGE
alluxio.worker.page.store.type=LOCAL
alluxio.worker.page.store.dirs=/mnt/ramdisk
alluxio.worker.page.store.sizes=1GB
alluxio.worker.page.store.page.size=1MB
```

The cache store used by Dora cache nodes is currently hardcoded to be the paged block store. You can refer to the
[documentation](https://github.com/Alluxio/alluxio/blob/dora/docs/en/core-services/Caching.md#experimental-paging-worker-storage)
on how to configure the paged block store.

## Dora on Kubernetes

The existing Alluxio helm chart works with Dora cluster except Alluxio Fuse. Note that:
1. Set your image to `alluxio/alluxio:291-gamma`.
2. Property `alluxio.master.mount.table.root.ufs` is no longer required. It is replaced by `alluxio.alluxio.client.ufs.root`
3. Make sure to include the required configurations specified in previous sections in your helm chart configuration file, including:
    - `alluxio.alluxio.client.read.localtion.policy.enabled: "true"`
    - `alluxio.user.short.circuit.enabled: "false"`
    - `alluxio.master.worker.register.lease.enabled: "false"`
    - `alluxio.alluxio.client.ufs.root: <under_fs_uri>`


See [here](https://docs.alluxio.io/os/user/edge/en/kubernetes/Running-Alluxio-On-Kubernetes.html) 
for details of how to deploy Alluxio with Helm Chart.

The Helm Chart tailored for Dora and supports Alluxo Fuse is under development.

## Tuning

### Optional Dora Server-side Metadata Cache

By default, Dora worker caches metadata and data.
Set `alluxio.alluxio.client.metadata.cache.enabled` to `false` to disable the metadata cache on docker worker if needed.
If disabled, client will always fetch metadata from under storage directly.

### High performance data transmission over Netty

Set `alluxio.user.netty.data.transmission.enabled` to `true` to enable transmission of data between clients and
Dora cache nodes over Netty. This avoids serialization and deserialization cost of gRPC, as well as consumes less
resources on the worker side.

## Known limitations

1. Currently, only one UFS is supported by Dora. Nested mounts are not supported yet.
2. Currently, the Alluxio Master node still needs to be up and running. It is used for Dora worker discovery,
   cluster configuration updates, as well as handling write IO operations.
3. Currently, Alluxio Fuse is not supported with Dora on Kubernetes with the existing helm chart. The helm chart
   supporting Alluxio Fuse is under development.
