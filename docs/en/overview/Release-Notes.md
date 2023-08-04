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

### Notable Configuration Property Changes

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

### Master 
* Fix bug for ufs journal dumper when read regular checkpoint ([de4f1b](https://github.com/Alluxio/alluxio/commit/de4f1b2ce71575927c39eb8db2aaa2cb8e788190){:target="_blank"})
* Fix concurrent sync dedup ([12ecbc](https://github.com/Alluxio/alluxio/commit/12ecbc80cecb70a3284d2949645235ab5864485c){:target="_blank"})
* Add more observability on inode tree corruption ([5cb7a9](https://github.com/Alluxio/alluxio/commit/5cb7a9c2d09ba2e2ea72d7a0a5c63d166e970bb8){:target="_blank"})
* Add compression level option for RocksDB checkpoint ([6af5af](https://github.com/Alluxio/alluxio/commit/61f5af80c6376f585e2edff0e50a1577597d17fa){:target="_blank"})
* Support log source ip to rpc debug log ([017078](https://github.com/Alluxio/alluxio/commit/017078481f57e3499855b423740d3c6f227c47ab){:target="_blank"})
* Bump ratis version to 2.4.1 ([1e95ed](https://github.com/Alluxio/alluxio/commit/1e95ed69ab9bec895b3bbfe4a2841238cbedc824){:target="_blank"})
* Improve the `PollingMasterInquireClient` logic ([1d6cb2](){:target="_blank"})
* Refactor simple master services out of main master process classes ([1cbbf8](){:target="_blank"})
* Use RPC hostname as fallback master hostname ([881849](https://github.com/Alluxio/alluxio/commit/881849f0678f7899565d12cb2d3aa3049e41c8d6){:target="_blank"})
* Fix ip is null in audit log ([5fdf51](https://github.com/Alluxio/alluxio/commit/5fdf51e05e4989af3fa78b11062ccc27725fd2e2){:target="_blank"})
* Fix stale `buildVersion` when downgrade workers ([952721](https://github.com/Alluxio/alluxio/commit/952721773baaaaa32ceb938148bdaf62bb4726a5){:target="_blank"})
* Remove file from `UfsAbsentPathCache` after persisting ([9ff756](https://github.com/Alluxio/alluxio/commit/9ff756a4757000156983e875fdd3d3e2b6bd38fb){:target="_blank"})
* Support TTL for synced inode ([79fe43](https://github.com/Alluxio/alluxio/commit/79fe43ab4e8c61d4d8090c0917991c0a4860df96){:target="_blank"})
* Update Raft group only on config change ([ff88f8](https://github.com/Alluxio/alluxio/commit/ff88f865e1eb1af0ec01269bded1b718032a3e3e){:target="_blank"})
* Add unsafe flush option to embedded journal ([3fe8e0](https://github.com/Alluxio/alluxio/commit/3fe8e090e90f8fac6cde2ced4db9976a82213839){:target="_blank"})
* Upgrade Apache Ratis from 2.3.0 to 2.4.0 ([6b5331](https://github.com/Alluxio/alluxio/commit/6b53318db1b458c1e28b381dbe5b24cf2c2c5afb){:target="_blank"})
* Delete worker metadata from master after heartbeat timeout ([8183d1](https://github.com/Alluxio/alluxio/commit/8183d1b976eaa0423014fb0d20985084029416f4){:target="_blank"})
* Support RocksDB inode/block store to different disk paths ([5f3188](https://github.com/Alluxio/alluxio/commit/5f31889dbda8d0a9b9fb96641c419cfb60ce355d){:target="_blank"})
* Support config Ratis configurations through Alluxio config ([62c319](https://github.com/Alluxio/alluxio/commit/62c3192bff5b9feccbdcf62dd716c2c8ccf651fc){:target="_blank"})
* Optimize `MasterWorkerInfo` memory usage by introducing `fastutil` Set ([a1e1e3](https://github.com/Alluxio/alluxio/commit/a1e1e33073b447a5d0944f82ba2b71703961162d){:target="_blank"})

### S3 API
* Reduce redundant calls in `getObject` of S3 API ([db2404](https://github.com/Alluxio/alluxio/commit/db240463955267b321a693e1e34bb758231d7c69){:target="_blank"})
* Eliminate race condition in completempupload and support overwrite ([e041e8](https://github.com/Alluxio/alluxio/commit/e041e804b86e9f794b56d91e0e9c781a231d8cac){:target="_blank"})
* Add Content-Range header for `getObject` ([4b83fa](https://github.com/Alluxio/alluxio/commit/4b83faac2eec0dbcce7b1041a1c19c79783b2c1a){:target="_blank"})
* Sort part files for uploading ([5df5cf](https://github.com/Alluxio/alluxio/commit/5df5cff4b3b1b29d9c658b0fbce3eae10ae23c0b){:target="_blank"})
* Add encoding-type support for S3 `ListObjects` and more logging ([e25cdc](https://github.com/Alluxio/alluxio/commit/e25cdc72a7996a02004e251436175e10d94b6caf){:target="_blank"})
* Fix out of bound error in parsing s3 Authorization header ([0c221f](https://github.com/Alluxio/alluxio/commit/0c221f12c0602a77982b2a38b2d24e7dc75196b8){:target="_blank"})

### CLI
* Restore table command with deprecated status ([6b8887](https://github.com/Alluxio/alluxio/commit/6b888731d8512ac6c9e60069a5e916b492166ede){:target="_blank"})
* Add a command to set `DirectChildrenLoaded` on dir ([822834](https://github.com/Alluxio/alluxio/commit/82283414fb2987bea4c6bf6fe59b343f7ce79856){:target="_blank"})
* Ignore `no_cache` setting for "load" command ([70bcee](https://github.com/Alluxio/alluxio/commit/70bcee1c5bdf1070fcef4f1bedbd8ed7f2abd065){:target="_blank"})
* Add a `removeAll` pathConf cmd to support remove all path conf ([6b8065](https://github.com/Alluxio/alluxio/commit/6b80655b9ae2a44106081b22a42649fa3aa5b8bb){:target="_blank"})
* Add a command to free a worker ([32785f](https://github.com/Alluxio/alluxio/commit/32785f9749152133da4c71c6b28e8c89af4e84d2){:target="_blank"})

### FUSE
* Support fuse sdk seek ([772915](https://github.com/Alluxio/alluxio/commit/772915741a2bf8450859c74271a5a0d6ad5007ae){:target="_blank"})
* Support fuse test to test against S3 ([4fd428](https://github.com/Alluxio/alluxio/commit/4fd4288570ffcbe05b66bf2d3797a79c5cb19850){:target="_blank"})
* Add new Alluxio-FUSE as local cache solution with UFS gateway ([705224](https://github.com/Alluxio/alluxio/commit/7052240f8f4d1cb99a00e4d70a6f40083d7d9cfa){:target="_blank"})
* Add `macFUSE` check for MacOS ([edf309](https://github.com/Alluxio/alluxio/commit/edf3092624be75d01d413abb21739824cbbf3d8c){:target="_blank"})

### Job Service
* Add new distributed load ([db9f07](https://github.com/Alluxio/alluxio/commit/db9f07a50e1f7f6c45d511d591c8775ada6b71bb){:target="_blank"})
* Add `jobName` into audit log for run command ([64f396](https://github.com/Alluxio/alluxio/commit/64f396eb1d9f1c9877818d755eaab8ed384d2fd8){:target="_blank"})
* Fix `nullpointerException` in distributed cmd ([8da4f5](https://github.com/Alluxio/alluxio/commit/8da4f5ac1d8367c9e9283fcbcdae74f0f17fd5f0){:target="_blank"})
* Improve job worker health report ([ef9b76](https://github.com/Alluxio/alluxio/commit/ef9b7695c8b43aef0f3ba9a30a8d4cc563fe1872){:target="_blank"})
* Fix null in distributed load cmd output ([3df564](https://github.com/Alluxio/alluxio/commit/3df5641469e9183cf0ec5a26bf8816ba510642b3){:target="_blank"})

### Worker
* Throw Error after reply error to client ([b13d7d](https://github.com/Alluxio/alluxio/commit/b13d7d32837b7ca0fbbb5fb472fcd44aeae13007){:target="_blank"})

### Client
* Fix client side config using wrong hash ([e69e3e](https://github.com/Alluxio/alluxio/commit/e69e3e0ce88963b99a54ffa5ce5fb575c44ab1a8){:target="_blank"})
* Remove caching for `CapacityBaseRandomPolicy` ([8a66a6](https://github.com/Alluxio/alluxio/commit/8a66a64a014bdb51bf38f34b04b1b825f94d1ccf){:target="_blank"})

### Metrics
* Make client send version to server and audit log contain version ([bd74a8](https://github.com/Alluxio/alluxio/commit/bd74a890abd3d2fcff18e86983835d51143df712){:target="_blank"})
* Fix `Master.JournalSequenceNumber` metrics in `RaftJournalSystem` ([bd30e2](https://github.com/Alluxio/alluxio/commit/bd30e25d7b0103e30dd8de611da250ced85cdf27){:target="_blank"})
* Clear metrics when closing `JournalStateMachine` ([b9d2e7](https://github.com/Alluxio/alluxio/commit/b9d2e79e48524cf784727ff0390aea4add6defd8){:target="_blank"})
* Configure block and inode metastore separately ([a8090b](https://github.com/Alluxio/alluxio/commit/a8090b447695106abba7b32fa4a4159e7e290d72){:target="_blank"})

### K8s and Deployment
* Support monitor helm chart ([0f4e59](https://github.com/Alluxio/alluxio/commit/0f4e5978d3267d5fa6f81a07396477202a347417){:target="_blank"})
* Build and symlink to shaded client jar within client/build ([ca8132](https://github.com/Alluxio/alluxio/commit/ca81323975bf1910fc4455254f6988460874d975){:target="_blank"})

### UFS
* Support STS for OSS ufs through `RAMRole` ([bbe99b](https://github.com/Alluxio/alluxio/commit/bbe99b30a523caf9029da13ccf7ab3ff99a1e34c){:target="_blank"})
* Add verbose mode to `fs` mount ([562e2c](https://github.com/Alluxio/alluxio/commit/562e2ccdab7e9110a94d97ae00ab8931d402e79a){:target="_blank"})
* Support multiple versions of COSN lib jars in Alluxio tarball ([a7b33a](https://github.com/Alluxio/alluxio/commit/a7b33a3c04a9c50264d89d7d78824366cea8b6f4){:target="_blank"})
* Add Hadoop dependencies into Ozone ufs connector ([d0d298](https://github.com/Alluxio/alluxio/commit/d0d2980854562567a1c92ac6032031ec7c98217a){:target="_blank"})
* Support `getUnderFSType` for Ozone, COSN ,Cephfs-hadoop ([29c70c](https://github.com/Alluxio/alluxio/commit/29c70c6f377f01f33452c51bf69ebf4715b1a948){:target="_blank"})

### Web UI
* Support display revision in WebUI ([54094d](https://github.com/Alluxio/alluxio/commit/54094d6d35dbfae9a8a4b2d68a7deda2a54cf62b){:target="_blank"})
* Add more cors config and make cors handle all http request ([9ba799](https://github.com/Alluxio/alluxio/commit/9ba7998f314a19f702a8a95d2e5ca7600f031863){:target="_blank"})
* Add a UI page for masters ([fdf8d3](https://github.com/Alluxio/alluxio/commit/fdf8d386606bafb031637eede71dbbec92598792){:target="_blank"})
* Display build version of workers in WebUI and capacity command ([7d8ad9](https://github.com/Alluxio/alluxio/commit/7d8ad9b12ab366ef85c05a7cedfa3a60a562de12){:target="_blank"})

### Stress Bench
* Fix `MultiOperation` Stress Master Bench ([1bb53f](https://github.com/Alluxio/alluxio/commit/1bb53f4e5595f641b9a0ad2c1ab0147c16fb6bd0){:target="_blank"})
* Add multi operation master stress bench ([d90eef](https://github.com/Alluxio/alluxio/commit/d90eef7d2d379bc1ec9a6ad28ddc572c1b019c9f){:target="_blank"})
* Support specify write type for `StressWorkerBench` ([12c85b](https://github.com/Alluxio/alluxio/commit/12c85b72feab96c63ba5839e4968641b9fe0926e){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.9.1 release. Especially, we would like to thank:

Haoning Sun ([Haoning-Sun](https://github.com/Haoning-Sun){:target="_blank"}),
Kaijie Chen ([kaijchen](https://github.com/kaijchen){:target="_blank"}),
Ling Bin ([lingbin](https://github.com/lingbin){:target="_blank"}),
Lucas ([lucaspeng12138](https://github.com/lucaspeng12138){:target="_blank"}),
Shuaibing Zhao ([StephenRi](https://github.com/StephenRi){:target="_blank"}),
Vimal ([vimalKeshu](https://github.com/vimalKeshu){:target="_blank"}),
XiChen ([xichen01](https://github.com/xichen01){:target="_blank"}),
Xinran Dong ([007DXR](https://github.com/007DXR){:target="_blank"}),
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}),
Zihao Zhao ([zhezhidashi](https://github.com/zhezhidashi){:target="_blank"}),
Bing Zheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}),
[chunxiaozheng](https://github.com/chunxiaozheng){:target="_blank"},
Wei Deng ([dengweisysu](https://github.com/dengweisysu){:target="_blank"}),
Tianbao Ding ([flaming-archer](https://github.com/flaming-archer){:target="_blank"}),
humengyu ([humengyu2012](https://github.com/humengyu2012){:target="_blank"}),
[jianghuazhu](https://github.com/jianghuazhu){:target="_blank"},
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"}),
Zhaoqun Deng ([secfree](https://github.com/secfree){:target="_blank"}),
Yanbin Zhang ([singer-bin](https://github.com/singer-bin){:target="_blank"}),
Xinyu Deng ([voddle](https://github.com/voddle){:target="_blank"}),
wuzhenhua ([wuzhenhua01](https://github.com/wuzhenhua01){:target="_blank"}),
[xpbob](https://github.com/xpbob){:target="_blank"},
yiichan ([YichuanSun](https://github.com/YichuanSun){:target="_blank"}),
and zhigang huang ([zerorclover](https://github.com/zerorclover){:target="_blank"})

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).