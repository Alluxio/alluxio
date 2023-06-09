---
layout: global
title: FUSE SDK with Distributed Cache Quick Start
group: FUSE SDK
priority: 3
---

* Table of Contents
  {:toc}

![POSIX LOCAL CACHE]({{ '/img/posix-distributed-cache.png' | relativize_url }})

If FUSE SDK can optional provide L1 cache (local node metadata/data cache) capability,
Alluxio cluster can provide L2 cache (local/nearby cluster metadata/data cache).

This suits the use cases that
- Need data sharing between nodes or tasks
- The total data accessed by a single node is bigger than the local node cache capability

Limitations:
- Only support one under storage dataset and is not modifiable
- Only support read-only workloads

## Deployment

Please follow [dora distributed cache cluster deployment guide]({{ '/en/overview/Getting-Started.html' | relativize_url}})
to deploy the Alluxio cluster.

### Launch FUSE SDK Connect to the distributed cache cluster

Launch FUSE SDK with the same configuration (same `<ALLUXIO_HOME>/conf/`) as launching the Alluxio cluster.
Other configuration is the same as launching a standalone FUSE SDK.
```console
$ alluxio-fuse mount <under_storage_dataset> <mount_point> -o option
```
`<under_storage_dataset>` should be exactly the same as the configured `alluxio.dora.client.ufs.root`.

All the metadata and data will be cached by Alluxio workers.
Optional disable default FUSE SDK local metadata cache with `-o local_metadata_cache_size=0`.
