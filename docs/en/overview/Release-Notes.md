---
layout: global
title: Release Notes
group: Overview
priority: 7
---

February 28, 2023

* Table of Contents
{:toc}

## Highlights

### Default TTL Behavior Change

The default TTL behavior previously was to delete the data from both Alluxio and the underlying storage. This is now changed to only deleting the data from Alluxio, leaving the persisted data untouched. See more details in the corresponding Github [issue](https://github.com/Alluxio/alluxio/issues/12316){:target="_blank"} and [PR](https://github.com/Alluxio/alluxio/pull/16823){:target="_blank"}.

### Prevent Recursive Metadata Sync from `getStatus` Call on a Directory

If a metadata sync is triggered by a getStatus call for inode type NONE, the previous behavior was to trigger metadata sync recursively through the directory’s child nodes. The new behavior does not recursively perform metadata sync, only syncing the target of the getStatus call. If the previous behavior is desired, set the property key `alluxio.master.metadata.sync.get.directory.status.skip.loading.children` to `false`.


## Improvements and Bugfixes Since 2.9.1

### Notable Configuration Property Changes

<table class="table table-striped">
    <tr>
        <th>Property Key</th>
        <th>Old 2.9.1 value</th>
        <th>New 2.9.2 value</th>
        <th>PR</th>
    </tr>
    <tr>
        <td markdown="span">`alluxio.underfs.eventual.consistency.retry.max.num`</td>
        <td markdown="span">20</td>
        <td markdown="span">0</td>
        <td markdown="span">[#16887](https://github.com/Alluxio/alluxio/pull/16887){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span">`alluxio.user.file.create.ttl.action`</td>
        <td markdown="span">DELETE</td>
        <td markdown="span">DELETE_ALLUXIO</td>
        <td markdown="span">[#16823](https://github.com/Alluxio/alluxio/pull/16823){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span">`alluxio.master.metastore.rocks.checkpoint.compression.level`</td>
        <td markdown="span">-1</td>
        <td markdown="span">1</td>
        <td markdown="span">[#16801](https://github.com/Alluxio/alluxio/pull/16801){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span">`alluxio.job.request.batch.size`</td>
        <td markdown="span">20</td>
        <td markdown="span">1</td>
        <td markdown="span">[#16802](https://github.com/Alluxio/alluxio/pull/16802){:target="_blank"}</td>
    </tr>

</table>

### Benchmark
* Fix the client stressbench concurrency problem ([#16934](https://github.com/Alluxio/alluxio/pull/16934){:target="_blank"})([cbff62](https://github.com/Alluxio/alluxio/commit/cbff62a130e319e11179eea3df0265dfa130acc1){:target="_blank"})
* Add local read benchmarks for `PagedBlockStore` ([#16804](https://github.com/Alluxio/alluxio/pull/16804){:target="_blank"})([f9cb6d](https://github.com/Alluxio/alluxio/commit/f9cb6df62777fc5c9272950dbcd48bec3b0cee4f){:target="_blank"})

### Build
* Fix build error when revision is shorter than 8 chars ([#16888](https://github.com/Alluxio/alluxio/pull/16888){:target="_blank"})([c4b9ec](https://github.com/Alluxio/alluxio/commit/c4b9ecf7a9a4bbbec66116ad82918aabe00b18ea){:target="_blank"})
* Add maven build dockerfile with jdk17 ([#16885](https://github.com/Alluxio/alluxio/pull/16885){:target="_blank"})([aea58c](https://github.com/Alluxio/alluxio/commit/aea58cd82f57e0336ae020a16d2f4a29c099c74e){:target="_blank"})
* Support Maven reproducible builds ([#16881](https://github.com/Alluxio/alluxio/pull/16881){:target="_blank"})([6400f4](https://github.com/Alluxio/alluxio/commit/6400f4cc93ae78e6ee2965fa1941f3cc8bf510a2){:target="_blank"})
* Replace `ManagementFactoryHelper` with `ManagementFactory` for java11 ([#16874](https://github.com/Alluxio/alluxio/pull/16874){:target="_blank"})([782b38](https://github.com/Alluxio/alluxio/commit/782b38c841235aabd81d071322494c5576c30852){:target="_blank"})

### Client
* Remove the restriction of UFS for local cache ([#16819](https://github.com/Alluxio/alluxio/pull/16819){:target="_blank"})([02bc22](https://github.com/Alluxio/alluxio/commit/02bc22c0a24122ad6f5c15f1653e920bb4d2b161){:target="_blank"})
* Add `CapacityBasedDeterministicHashPolicy` ([#16237](https://github.com/Alluxio/alluxio/pull/16237){:target="_blank"})([c14861](https://github.com/Alluxio/alluxio/commit/c148612007e5ba358f3fea7cddc24760dc98b2bf){:target="_blank"})
* Implement unbuffer interface for `HdfsFileInputStream` ([#16017](https://github.com/Alluxio/alluxio/pull/16017){:target="_blank"})([903269](https://github.com/Alluxio/alluxio/commit/903269f077f30d3c274adfbc53ef248be8ff7356){:target="_blank"})
* Change default TTL action from `DELETE` to `DELETE_ALLUXIO` ([#16823](https://github.com/Alluxio/alluxio/pull/16823){:target="_blank"})([7aa8b6](https://github.com/Alluxio/alluxio/commit/7aa8b6c68a3c8a71c4c881adb6c10fa0a365bdc8){:target="_blank"})
* Reduce useless async cache request when read from remote worker ([#16313](https://github.com/Alluxio/alluxio/pull/16313){:target="_blank"})([99923c](https://github.com/Alluxio/alluxio/commit/99923c6b955925fd9bbc0af55a7b30fdc2875f19){:target="_blank"})
* Add cache TTL enforcement ([#16843](https://github.com/Alluxio/alluxio/pull/16843){:target="_blank"})([6ca7d1](https://github.com/Alluxio/alluxio/commit/6ca7d1533479d9c745a214a8239d60c2fda42f4a){:target="_blank"})
* Add the two-choice random cache eviction policy ([#16828](https://github.com/Alluxio/alluxio/pull/16828){:target="_blank"})([530e72](https://github.com/Alluxio/alluxio/commit/530e720a30c51c58f1f2e9651a4543ac1315d01f){:target="_blank"})

### Configuration
* Support dynamic update `HeartbeatThread` tick interval ([#16702](https://github.com/Alluxio/alluxio/pull/16702){:target="_blank"})([7f6baa](https://github.com/Alluxio/alluxio/commit/7f6baa353b5e78576d6a61336b3a77ff2a8e8a77){:target="_blank"})
* Fix updated config not notifying config registry ([#16764](https://github.com/Alluxio/alluxio/pull/16764){:target="_blank"})([407cf1](https://github.com/Alluxio/alluxio/commit/407cf1521b8612319df0ca80e5fa8716918c9505){:target="_blank"})

### Docker and K8s
* Respect `MaxRamPercentage` in container ([#16940](https://github.com/Alluxio/alluxio/pull/16940){:target="_blank"})([492cc7](https://github.com/Alluxio/alluxio/commit/492cc7c8abc9212fa90cba4dd0c6efcf0bb60ab5){:target="_blank"})
* Add buildx support for `build-docker.sh` ([#16822](https://github.com/Alluxio/alluxio/pull/16822){:target="_blank"})([98dba4](https://github.com/Alluxio/alluxio/commit/98dba4d78b201ddaf3d5db428befaf83be5cb618){:target="_blank"})

### Job Service
* Fix cli distributed command hang issue ([#16786](https://github.com/Alluxio/alluxio/pull/16786){:target="_blank"})([59f660](https://github.com/Alluxio/alluxio/commit/59f660d8d35578f635e0e80e539fb5435739afad){:target="_blank"})
* Fix logging to capture `jobControlId` ([#16780](https://github.com/Alluxio/alluxio/pull/16780){:target="_blank"})([412e54](https://github.com/Alluxio/alluxio/commit/412e549f4c17a8561b7a35bdf981ae575a00f561){:target="_blank"})

### Log
* Fix debug log in `FileSystemContext#reinit()` ([#16826](https://github.com/Alluxio/alluxio/pull/16826){:target="_blank"})([fb07ea](https://github.com/Alluxio/alluxio/commit/fb07ea49e7a2e567f562a04c282d93741b7094f1){:target="_blank"})
* Fix logging to capture `jobControlId` ([#16780](https://github.com/Alluxio/alluxio/pull/16780){:target="_blank"})([412e54](https://github.com/Alluxio/alluxio/commit/412e549f4c17a8561b7a35bdf981ae575a00f561){:target="_blank"})

### Master
* Improve LS by allowing to omit UFS and mount info ([#16893](https://github.com/Alluxio/alluxio/pull/16893){:target="_blank"})([3f8c79](https://github.com/Alluxio/alluxio/commit/3f8c79cdc91ce78da675f314c509d55893a143a0){:target="_blank"})
* Fix metadata sync behavior when descendant type is `NONE` ([#16935](https://github.com/Alluxio/alluxio/pull/16935){:target="_blank"})([8ef0a4](https://github.com/Alluxio/alluxio/commit/8ef0a4d9ed7e10a5c5640969f14f12d31f3ea732){:target="_blank"})
* Make standby master keep `MountTable` up-to-date  ([#16908](https://github.com/Alluxio/alluxio/pull/16908){:target="_blank"})([a5d57e](https://github.com/Alluxio/alluxio/commit/a5d57e9f7888fb78c65e45540f2f8ec22da149cc){:target="_blank"})
* Make workers register to all masters ([#16849](https://github.com/Alluxio/alluxio/pull/16849){:target="_blank"})([ebeac4](https://github.com/Alluxio/alluxio/commit/ebeac49c3cfc9a224cf8772ebe1c3e38ae1a0b5a){:target="_blank"})
* Add `allowOnStandbyMasters` option for version grpc endpoint ([#16890](https://github.com/Alluxio/alluxio/pull/16890){:target="_blank"})([da6abf](https://github.com/Alluxio/alluxio/commit/da6abf7ec50e398ac65e15dbbf084a3e61341f86){:target="_blank"})
* Fix file fingerprint to use atomic get content hash of uploaded file ([#16597](https://github.com/Alluxio/alluxio/pull/16597){:target="_blank"})([00da77](https://github.com/Alluxio/alluxio/commit/00da77cf69c0ca29acecc4c37aed6ba53b9e93d2){:target="_blank"})
* Make version service return unavailable on standby masters ([#16854](https://github.com/Alluxio/alluxio/pull/16854){:target="_blank"})([7399c3](https://github.com/Alluxio/alluxio/commit/7399c3877af535fad1a725cf56405b1c04b8cb89){:target="_blank"})
* Support `rm` directory and reset `DirectChildrenLoadedState` ([#15647](https://github.com/Alluxio/alluxio/pull/15647){:target="_blank"})([778f9e](https://github.com/Alluxio/alluxio/commit/778f9ec7ec8a30a55e1002b8da858967760ab266){:target="_blank"})
* Support gRPC on standby masters ([#16839](https://github.com/Alluxio/alluxio/pull/16839){:target="_blank"})([325f36](https://github.com/Alluxio/alluxio/commit/325f36aa0319fc2ea65fb3a520bbe7cacfc618d7){:target="_blank"})
* Add disable are descendants loaded property ([#16813](https://github.com/Alluxio/alluxio/pull/16813){:target="_blank"})([5a7cfe](https://github.com/Alluxio/alluxio/commit/5a7cfe7f68fb393436dda1f4a32143074ae5aec3){:target="_blank"})
* Use async journals for block id journal entries in metadata sync ([#16529](https://github.com/Alluxio/alluxio/pull/16529){:target="_blank"})([9f91f9](https://github.com/Alluxio/alluxio/commit/9f91f95e81d7aa1d1ea4b2b2e3ede92a645fac55){:target="_blank"})

### Metrics
* Add metrics sink to job master ([#16899](https://github.com/Alluxio/alluxio/pull/16899){:target="_blank"})([73f3ce](https://github.com/Alluxio/alluxio/commit/73f3ce83c8a3ef77ac3eebb4579bb7d412784ec9){:target="_blank"})
* Fix `Worker.ActiveClients` is negative when load from ufs ([#16784](https://github.com/Alluxio/alluxio/pull/16784){:target="_blank"})([60d515](https://github.com/Alluxio/alluxio/commit/60d51564167dbc05047d9435f0825f20ed047c0a){:target="_blank"})
* Add metrics for in memory data structures ([#16818](https://github.com/Alluxio/alluxio/pull/16818){:target="_blank"})([ee6c23](https://github.com/Alluxio/alluxio/commit/ee6c23f7c60fef6ccdcc64ecca0e2ade18b62725){:target="_blank"})
* Fix metric `worker.cacheBlockSize` increases when cache already existed ([#16791](https://github.com/Alluxio/alluxio/pull/16791){:target="_blank"})([945b10](https://github.com/Alluxio/alluxio/commit/945b10dc30c4336540c1c1d2c60610780fcf6564){:target="_blank"})

### S3 API
* Fix incorrect flag passing into delete op ([#16941](https://github.com/Alluxio/alluxio/pull/16941){:target="_blank"})([a02c5f](https://github.com/Alluxio/alluxio/commit/a02c5f6ab9d9976aa300101da7679c6eab8846ab){:target="_blank"})
* Add rate limit for s3 proxy ([#16866](https://github.com/Alluxio/alluxio/pull/16866){:target="_blank"})([f0e9a5](https://github.com/Alluxio/alluxio/commit/f0e9a5da89c7d763ae5dc3c2a4bed3fa042316b4){:target="_blank"})
* Support overwrite option in `createFile` ([#16886](https://github.com/Alluxio/alluxio/pull/16886){:target="_blank"})([16ff65](https://github.com/Alluxio/alluxio/commit/16ff653d639d46a3b072b3c0c42a09c914d5e584){:target="_blank"})
* Introduce a rearchitectured S3 proxy service ([#16654](https://github.com/Alluxio/alluxio/pull/16654){:target="_blank"})([f3eca2](https://github.com/Alluxio/alluxio/commit/f3eca21eea7b1af3813b2976d3eeed492f6869e2){:target="_blank"})

### CLI
* Fix head and tail commands read less than expected ([#16928](https://github.com/Alluxio/alluxio/pull/16928){:target="_blank"})([c4a700](https://github.com/Alluxio/alluxio/commit/c4a700720f54107f2d222594880bad2bdd47b54d){:target="_blank"})
* Fix free worker command bugs ([#16458](https://github.com/Alluxio/alluxio/pull/16458){:target="_blank"})([ff0a6d](https://github.com/Alluxio/alluxio/commit/ff0a6da12b8342de6ed5235a3785acecc8fafc1e){:target="_blank"})
* Fix cli distributed command hang issue ([#16786](https://github.com/Alluxio/alluxio/pull/16786){:target="_blank"})([59f660](https://github.com/Alluxio/alluxio/commit/59f660d8d35578f635e0e80e539fb5435739afad){:target="_blank"})

### UFS
* Update retry policy for object stores ([#16887](https://github.com/Alluxio/alluxio/pull/16887){:target="_blank"})([47320b](https://github.com/Alluxio/alluxio/commit/47320b67669360b69403842d4d912a5c977088d0){:target="_blank"})
* Merge delete requests to Optimize move directory perf on objectstore ([#16527](https://github.com/Alluxio/alluxio/pull/16527){:target="_blank"})([7ab257](https://github.com/Alluxio/alluxio/commit/7ab2579ff2a9108fc11a938b10685779f1e3ee3f){:target="_blank"})
* Refactor s3 low level output stream and support OSS and OBS ([#16122](https://github.com/Alluxio/alluxio/pull/16122){:target="_blank"})([0b4796](https://github.com/Alluxio/alluxio/commit/0b4796f5482e362efe454abf2935e88fef1b69a9){:target="_blank"})

### Web UI
* Display more information in WebUI Masters ([#16636](https://github.com/Alluxio/alluxio/pull/16636){:target="_blank"})([8502fb](https://github.com/Alluxio/alluxio/commit/8502fbf3bb4bc51431545db5d54d588a033b721e){:target="_blank"})
* Support display master system status from master UI ([#16779](https://github.com/Alluxio/alluxio/pull/16779){:target="_blank"})([fb9e88](https://github.com/Alluxio/alluxio/commit/fb9e88d86289aedcd49b802e7fc5c6fea1e27924){:target="_blank"})

### Worker
* Support remove blocks on worker for pagestore when free/delete file ([#16895](https://github.com/Alluxio/alluxio/pull/16895){:target="_blank"})([669f80](https://github.com/Alluxio/alluxio/commit/669f80e612f2086d1ded170cfc09cdddc3ffbc2c){:target="_blank"})
* Fix free worker command bugs ([#16458](https://github.com/Alluxio/alluxio/pull/16458){:target="_blank"})([ff0a6d](https://github.com/Alluxio/alluxio/commit/ff0a6da12b8342de6ed5235a3785acecc8fafc1e){:target="_blank"})
* Add local read benchmarks for `PagedBlockStore` ([#16804](https://github.com/Alluxio/alluxio/pull/16804){:target="_blank"})([f9cb6d](https://github.com/Alluxio/alluxio/commit/f9cb6df62777fc5c9272950dbcd48bec3b0cee4f){:target="_blank"})
* Merge local cache invalidation ([#16841](https://github.com/Alluxio/alluxio/pull/16841){:target="_blank"})([506a0f](https://github.com/Alluxio/alluxio/commit/506a0f4b8667ae7e6895ccfb88cbe28ed26edde7){:target="_blank"})
* Split `BlockStoreEventListener.onCommitBlock()` ([#16777](https://github.com/Alluxio/alluxio/pull/16777){:target="_blank"})([6fc070](https://github.com/Alluxio/alluxio/commit/6fc070180928e1696722b50ced7b9ed19f886752){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.9.3 release. Especially, we would like to thank:

Deepak Shivamurthy ([deepak-shivamurthy](https://github.com/deepak-shivamurthy){:target="_blank"}),
Haoning Sun ([Haoning-Sun](https://github.com/Haoning-Sun){:target="_blank"}),
Ce Zhang ([JySongWithZhangCe](https://github.com/JySongWithZhangCe){:target="_blank"}),
Kaijie Chen ([kaijchen](https://github.com/kaijchen){:target="_blank"}),
Nandeeshvar Porko Pandiyan ([nand-porko](https://github.com/nand-porko){:target="_blank"}),
Vimal ([vimalKeshu](https://github.com/vimalKeshu){:target="_blank"}),
Xinran Dong([007DXR](https://github.com/007DXR){:target="_blank"}),
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}),
bingzheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}),
fanyang ([fffanyang](https://github.com/fffanyang){:target="_blank"}),
humengyu ([humengyu2012](https://github.com/humengyu2012){:target="_blank"}),
linda ([wenfang6](https://github.com/wenfang6){:target="_blank"}),
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"}),
Xinyu Deng([voddle](https://github.com/voddle){:target="_blank"}),
Yangchen Ye ([YangchenYe323](https://github.com/YangchenYe323){:target="_blank"}),
yiichan ([YichuanSun](https://github.com/YichuanSun){:target="_blank"}),
and Tianbao Ding ([flaming-archer](https://github.com/flaming-archer){:target="_blank"})

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).