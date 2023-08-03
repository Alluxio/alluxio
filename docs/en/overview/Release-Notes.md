---
layout: global
title: Release Notes
group: Overview
priority: 7
---

March 24, 2023

* Table of Contents
{:toc}

## Highlights

### Revert of Default TTL Behavior Change

In the previous release, the default TTL behavior was modified but a backwards compatibility issue was discovered. This version changes it back to the original behavior of deleting the data from both Alluxio and the underlying storage as opposed to only deleting the data from Alluxio. See more details in the corresponding github [PR](https://github.com/Alluxio/alluxio/pull/17039){:target="_blank"}.

## Improvements and Bugfixes Since 2.9.2

Notable configuration property changes:

<table class="table table-striped">
    <tr>
        <th>Property Key</th>
        <th>Old 2.9.2 value</th>
        <th>New 2.9.3 value</th>
        <th>PR</th>
    </tr>
    <tr>
        <td markdown="span">`alluxio.proxy.s3.multipart.upload.cleaner.enabled`</td>
        <td markdown="span">true</td>
        <td markdown="span">false</td>
        <td markdown="span">[#16937](https://github.com/Alluxio/alluxio/pull/16937){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span">`alluxio.proxy.s3.v2.version.enabled`</td>
        <td markdown="span">true</td>
        <td markdown="span">false</td>
        <td markdown="span">[#16937](https://github.com/Alluxio/alluxio/pull/16937){:target="_blank"}</td>
    </tr>
    <tr>
        <td markdown="span">`alluxio.user.file.create.ttl.action`</td>
        <td markdown="span">DELETE_ALLUXIO</td>
        <td markdown="span">FREE</td>
        <td markdown="span">[#17039](https://github.com/Alluxio/alluxio/pull/17039){:target="_blank"}</td>
    </tr>

</table>

Benchmark
* Add `LoadMetadata` and `CreateTree` operation to `StressMasterBench` ([#17042](https://github.com/Alluxio/alluxio/pull/17042){:target="_blank"})([ace82a](https://github.com/Alluxio/alluxio/commit/ace82a43a75a5bb948b8f4c7e7e109a927a5bbd1){:target="_blank"})

Build
* Fix dependencies conflicts thrown by Trino ([#17129](https://github.com/Alluxio/alluxio/pull/17129){:target="_blank"})([9c1f00](https://github.com/Alluxio/alluxio/commit/9c1f0006ad38f8f6be2658fd88eff5de4e8530b3){:target="_blank"})
* Update ci version ([#16964](https://github.com/Alluxio/alluxio/pull/16964){:target="_blank"})([1bf26f](https://github.com/Alluxio/alluxio/commit/1bf26fa05fb9c524f4d0396f93f62cdc4e03d82d){:target="_blank"})

Client
* Refactor collectMetrics command ([#17111](https://github.com/Alluxio/alluxio/pull/17111){:target="_blank"})([b7866e](https://github.com/Alluxio/alluxio/commit/b7866e44c279b429cd570dd5a8adf28421adafa6){:target="_blank"})
* Set ttlaction back to free ([#17039](https://github.com/Alluxio/alluxio/pull/17039){:target="_blank"})([ce5433](https://github.com/Alluxio/alluxio/commit/ce543363e806dfc3dc02acc01fe2f4a20347969f){:target="_blank"})

FUSE/POSIX
* Fix fuse metadata command `ClassCastException` ([#17035](https://github.com/Alluxio/alluxio/pull/17035){:target="_blank"})([5425c2](https://github.com/Alluxio/alluxio/commit/5425c29020013f993351c04ffa9cc6965221ccc2){:target="_blank"})

Journal and Metadata Sync
* Fix file system merge journal context ([#17071](https://github.com/Alluxio/alluxio/pull/17071){:target="_blank"})([57c078](https://github.com/Alluxio/alluxio/commit/57c078cf47f872db5c610270633b4bb3cd835c20){:target="_blank"})
* Fix metadata sync journalling when using UFS journal ([#17032](https://github.com/Alluxio/alluxio/pull/17032){:target="_blank"})([6bbe71](https://github.com/Alluxio/alluxio/commit/6bbe71358328a3e1557e9a22360db1891a6a4f61){:target="_blank"})
* Merge `JournalContext` for persistence ([#16966](https://github.com/Alluxio/alluxio/pull/16966){:target="_blank"})([cb20cc](https://github.com/Alluxio/alluxio/commit/cb20ccddf8c9442969ae98de7e6737e6bafa650a){:target="_blank"})
* Merge journal context in `LostFileDetector` ([#16971](https://github.com/Alluxio/alluxio/pull/16971){:target="_blank"})([952bd4](https://github.com/Alluxio/alluxio/commit/952bd4ba4a629ca945cbb24144caa84a9a287411){:target="_blank"})

Job Service
* Add abstract job ([#17103](https://github.com/Alluxio/alluxio/pull/17103){:target="_blank"})([71521f](https://github.com/Alluxio/alluxio/commit/71521f631d1bd9c15ffe05634a7251014f4be1a0){:target="_blank"})
* Refactor `LoadManager` to Scheduler ([#16982](https://github.com/Alluxio/alluxio/pull/16982){:target="_blank"})([8edf50](https://github.com/Alluxio/alluxio/commit/8edf508ebd49238f55f64011dcdd1ca40731da3f){:target="_blank"})

Docker and K8s
* Add `UseContainerSupport` flag to default value.yaml of k8s helm chart ([#17055](https://github.com/Alluxio/alluxio/pull/17055){:target="_blank"})([b16a3f](https://github.com/Alluxio/alluxio/commit/b16a3fbc49693b306cebd4beedd675b35b6fb4b5){:target="_blank"})
* Bump CSI package version to fix security alert ([#17117](https://github.com/Alluxio/alluxio/pull/17117){:target="_blank"})([1ad1cc](https://github.com/Alluxio/alluxio/commit/1ad1cc97b523e03a8cc45df7350bff7ad725fc6e){:target="_blank"})

Log
* Add more details in Log4j default settings ([#17072](https://github.com/Alluxio/alluxio/pull/17072){:target="_blank"})([595a55](https://github.com/Alluxio/alluxio/commit/595a55a194fe2d6bc6b8e94043ab64da5fd7ac1c){:target="_blank"})

Master
* Dump metrics + stacks on failover/crash ([#17081](https://github.com/Alluxio/alluxio/pull/17081){:target="_blank"})([7f3354](https://github.com/Alluxio/alluxio/commit/7f335463bb1ef07186cbc302895fdb6631889e05){:target="_blank"})
* Fix `setTtl` workflow and `ttlbucket` bugs ([#16933](https://github.com/Alluxio/alluxio/pull/16933){:target="_blank"})([731385](https://github.com/Alluxio/alluxio/commit/731385377b151b1c995c2d7f48562cd2b61805a9){:target="_blank"})
* Cache Block Location to save memory ([#16953](https://github.com/Alluxio/alluxio/pull/16953){:target="_blank"})([0645ea](https://github.com/Alluxio/alluxio/commit/0645ea804b1b83f457fcd7288abfe43392952116){:target="_blank"})
* Improve state lock tracking and report on error ([#16984](https://github.com/Alluxio/alluxio/pull/16984){:target="_blank"})([654969](https://github.com/Alluxio/alluxio/commit/654969975d7d6cbef076f2a2f2f0ec4c8db8a782){:target="_blank"})
* Add a property to disable file access time ([#16981](https://github.com/Alluxio/alluxio/pull/16981){:target="_blank"})([163b3f](https://github.com/Alluxio/alluxio/commit/163b3ff50d56475b5b69fa9fd54e82df50be7357){:target="_blank"})

Metrics
* Fix `collectMetrics` command in k8s ([#17127](https://github.com/Alluxio/alluxio/pull/17127){:target="_blank"})([9532ce](https://github.com/Alluxio/alluxio/commit/9532ce03c88d18d7e132bc25c74dcb6d6b357b49){:target="_blank"})
* Add metric for cached block location ([#17056](https://github.com/Alluxio/alluxio/pull/17056){:target="_blank"})([73b4d6](https://github.com/Alluxio/alluxio/commit/73b4d67c8b39a6358897ec349370e7037a6fea7f){:target="_blank"})

RPC
* Support `getBlockInfo` excluding mount-related info ([#17006](https://github.com/Alluxio/alluxio/pull/17006){:target="_blank"})([276644](https://github.com/Alluxio/alluxio/commit/2766440e3e35cbb406beb08113d161a3ceeb9809){:target="_blank"})

S3 API
* Make light and heavy thread pool configurable for s3 proxy v2 ([#17082](https://github.com/Alluxio/alluxio/pull/17082){:target="_blank"})([65ed1b](https://github.com/Alluxio/alluxio/commit/65ed1bba309d0af25251ae2554757518d65d59fc){:target="_blank"})
* Fix NPE for s3 proxy v2 ([#17086](https://github.com/Alluxio/alluxio/pull/17086){:target="_blank"})([a5cf2a](https://github.com/Alluxio/alluxio/commit/a5cf2a23300aa94014d54fcb9be0c5cf68193d9a){:target="_blank"})
* Fix retry function and S3 abort upload ([#17094](https://github.com/Alluxio/alluxio/pull/17094){:target="_blank"})([cbf24b](https://github.com/Alluxio/alluxio/commit/cbf24bf4184f188aa5c36aceddf60ea8aaca2a4c){:target="_blank"})
* Enable v2 s3 proxy by default ([#16937](https://github.com/Alluxio/alluxio/pull/16937){:target="_blank"})([7f2f15](https://github.com/Alluxio/alluxio/commit/7f2f15adc8e865abcb66020ceb7524475c8ad19c){:target="_blank"})

CLI
* Enhance the capacity report to show worker registration info ([#16927](https://github.com/Alluxio/alluxio/pull/16927){:target="_blank"})([df0ae1](https://github.com/Alluxio/alluxio/commit/df0ae1e5b3ef03095388466c4580849a4d9ee744){:target="_blank"})

UFS
* Fix property identity typo error ([#17109](https://github.com/Alluxio/alluxio/pull/17109){:target="_blank"})([fd7350](https://github.com/Alluxio/alluxio/commit/fd7350705e5f40e521dd025700341fc6746de573){:target="_blank"})
* Fix content hash for GCS v2 stream ([#17089](https://github.com/Alluxio/alluxio/pull/17089){:target="_blank"})([b59df3](https://github.com/Alluxio/alluxio/commit/b59df3c479c0b82a6d56034d9a4554d970d1046c){:target="_blank"})
* Remove `HdfsUnderFileSystemFactory` from service loading in COSN UFS jar ([#17024](https://github.com/Alluxio/alluxio/pull/17024){:target="_blank"})([c9ed63](https://github.com/Alluxio/alluxio/commit/c9ed6344446457100b6da0965a96ddbcd870f140){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.9.3 release. Especially, we would like to thank:

Haoning Sun ([Haoning-Sun](https://github.com/Haoning-Sun){:target="_blank"}), 
Kaijie Chen ([kaijchen](https://github.com/kaijchen){:target="_blank"}), 
Xinran Dong ([007DXR](https://github.com/007DXR){:target="_blank"}), 
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}), 
humengyu ([humengyu2012](https://github.com/humengyu2012){:target="_blank"}), 
Xinyu Deng ([voddle](https://github.com/voddle){:target="_blank"}), 
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}), 
Tianbao Ding ([flaming-archer](https://github.com/flaming-archer){:target="_blank"}), 
[fengshunli](https://github.com/fengshunli){:target="_blank"}, 
and [jianghuazhu](https://github.com/jianghuazhu){:target="_blank"}.

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).