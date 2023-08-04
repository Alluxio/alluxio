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
* `IDLE`
* `ACTIVE`
* `STRESSED`
* `OVERLOADED`

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
* Make journal context thread safe ([f5b2a5f438](https://github.com/Alluxio/alluxio/commits/f5b2a5f438){:target="_blank"})
* Fix error in snapshot-taking when using large group ids ([2803dc4603](https://github.com/Alluxio/alluxio/commits/2803dc4603){:target="_blank"})
* Mark root as needing sync on backup restore ([0fe867ac72](https://github.com/Alluxio/alluxio/commits/0fe867ac72){:target="_blank"})
* Make `MountTable.State` thread safe ([45ce753499](https://github.com/Alluxio/alluxio/commits/45ce753499){:target="_blank"})
* Avoid canceling duplicate metadata sync prefetch job ([aacee53fbc](https://github.com/Alluxio/alluxio/commits/aacee53fbc){:target="_blank"})
* Support multithread checkpointing with compression/decompression ([ae065e34b9](https://github.com/Alluxio/alluxio/commits/ae065e34b9){:target="_blank"})
* Avoid and dedup concurrent metadata sync ([025ca19d09](https://github.com/Alluxio/alluxio/commits/025ca19d09){:target="_blank"})
* Refine add `mPendingPaths` in `InodeSyncStream` new type ([749f70cd1a](https://github.com/Alluxio/alluxio/commits/749f70cd1a){:target="_blank"})
* Fix single master embedded journal checkpoint ([a115fa3d5b](https://github.com/Alluxio/alluxio/commits/a115fa3d5b){:target="_blank"})
* Create inode before updating the `MountTable` ([56d1c6a9bc](https://github.com/Alluxio/alluxio/commits/56d1c6a9bc){:target="_blank"})
* Allow root sync path to use child sync time ([4d6bce3d5f](https://github.com/Alluxio/alluxio/commits/4d6bce3d5f){:target="_blank"})
* Add client operation for partial listing ([bc6e63f7f8](https://github.com/Alluxio/alluxio/commits/bc6e63f7f8){:target="_blank"})
* Determine the primary master address by calling `GetNodeState` endpoint ([c63ec951e4](https://github.com/Alluxio/alluxio/commits/c63ec951e4){:target="_blank"})
* Upgrade Apache Ratis from 2.0.0 to 2.3.0 ([dc0a21daed](https://github.com/Alluxio/alluxio/commits/dc0a21daed){:target="_blank"})
* Merge journals & flush journals before lock release ([8dafc272be](https://github.com/Alluxio/alluxio/commits/8dafc272be){:target="_blank"})
* Add Partial listing of files in `listStatus` ([5f50dd8ab3](https://github.com/Alluxio/alluxio/commits/5f50dd8ab3){:target="_blank"})
* Improve error handling and naming on journal threads ([e69adee025](https://github.com/Alluxio/alluxio/commits/e69adee025){:target="_blank"})
* Add Partial listing of files in `listStatus` ([ec0ce2a656](https://github.com/Alluxio/alluxio/commits/ec0ce2a656){:target="_blank"})
* Fix journal shutdown deadlock ([155a370fbe](https://github.com/Alluxio/alluxio/commits/155a370fbe){:target="_blank"})
* Allow writing to read-only file when creating ([fe27139f6c](https://github.com/Alluxio/alluxio/commits/fe27139f6c){:target="_blank"})
* Avoid checking file permissions in `getFileInfo` method ([54494af052](https://github.com/Alluxio/alluxio/commits/54494af052){:target="_blank"})
* Fix master down when master change to leader ([1ada2ac8c7](https://github.com/Alluxio/alluxio/commits/1ada2ac8c7){:target="_blank"})

### Cache and Storage
* Implement Byte array pool ([d071d5ef7c](https://github.com/Alluxio/alluxio/commits/d071d5ef7c){:target="_blank"})
* Add block size for paged blocks ([fd865e24a1](https://github.com/Alluxio/alluxio/commits/fd865e24a1){:target="_blank"})
* Make worker init tiers parallel ([2af80f6e19](https://github.com/Alluxio/alluxio/commits/2af80f6e19){:target="_blank"})
* Fix early release of buffer ([5e8d62c8a7](https://github.com/Alluxio/alluxio/commits/5e8d62c8a7){:target="_blank"})
* Separate page store configuration from client cache ([f6cce53631](https://github.com/Alluxio/alluxio/commits/f6cce53631){:target="_blank"})
* Optimize `getFileBlockLocations` performance ([e5692d2261](https://github.com/Alluxio/alluxio/commits/e5692d2261){:target="_blank"})
* Ignore parent path `NoSuchFileException` when `localPageStore` delete `pageId` ([6a64a178b2](https://github.com/Alluxio/alluxio/commits/6a64a178b2){:target="_blank"})
* Support size encoding for clock cuckoo filter in shadowcache ([7395bed318](https://github.com/Alluxio/alluxio/commits/7395bed318){:target="_blank"})
* Do not add the worker to failed list for client exception ([951f3568a2](https://github.com/Alluxio/alluxio/commits/951f3568a2){:target="_blank"})
* Fix the leak of block lock ([b97677e78e](https://github.com/Alluxio/alluxio/commits/b97677e78e){:target="_blank"})
* Fix failures in mem page store when zero copy enabled ([cbc2008b19](https://github.com/Alluxio/alluxio/commits/cbc2008b19){:target="_blank"})
* Fix load `sessionId` ([d763b4077e](https://github.com/Alluxio/alluxio/commits/d763b4077e){:target="_blank"})
* Fix paged block reader transfer offset ([463506e178](https://github.com/Alluxio/alluxio/commits/463506e178){:target="_blank"})
* Implement locking and pinning for paged block store ([9c67f34682](https://github.com/Alluxio/alluxio/commits/9c67f34682){:target="_blank"})
* Allocate buffer in load api ([b93535cb6a](https://github.com/Alluxio/alluxio/commits/b93535cb6a){:target="_blank"})
* Fix worker stream register forget release lease ([2df21b9aee](https://github.com/Alluxio/alluxio/commits/2df21b9aee){:target="_blank"})
* Fix paged block store tier name ([92c0a4fb73](https://github.com/Alluxio/alluxio/commits/92c0a4fb73){:target="_blank"})
* Fix potential deadlock in tier store and refine the code ([d1efe52f44](https://github.com/Alluxio/alluxio/commits/d1efe52f44){:target="_blank"})
* Fix `MaxFreeAllocator.allocateBlock` ([21be94c923](https://github.com/Alluxio/alluxio/commits/21be94c923){:target="_blank"})
* Disable passive cache for pinned files ([0e53f84b7c](https://github.com/Alluxio/alluxio/commits/0e53f84b7c){:target="_blank"})
* Fix out of bound read in `PagedBlockReader` ([63318bb3bb](https://github.com/Alluxio/alluxio/commits/63318bb3bb){:target="_blank"})

### S3 API and Proxy
* Fix out of bound error in parsing s3 Authorization header ([5f95931723](https://github.com/Alluxio/alluxio/commits/5f95931723){:target="_blank"})
* Implement S3 headbucket API ([0236480f58](https://github.com/Alluxio/alluxio/commits/0236480f58){:target="_blank"})
* Fix double checked locking in S3 uploader ([cdbfa096b2](https://github.com/Alluxio/alluxio/commits/cdbfa096b2){:target="_blank"})
* Fix special char support ([1ea5a840ac](https://github.com/Alluxio/alluxio/commits/1ea5a840ac){:target="_blank"})
* Fix aws s3 cp with source object having special characters in it ([7eba438a6f](https://github.com/Alluxio/alluxio/commits/7eba438a6f){:target="_blank"})
* Respect prefix param to avoid recursive `ls` on root dir ([619089a7ea](https://github.com/Alluxio/alluxio/commits/619089a7ea){:target="_blank"})
* Fix S3 API file mode bits to prevent unauthorized reads ([87602c7a71](https://github.com/Alluxio/alluxio/commits/87602c7a71){:target="_blank"})
* Add empty string check for delimiter ([de0bebfeed](https://github.com/Alluxio/alluxio/commits/de0bebfeed){:target="_blank"})
* Extract Authentication and common logging into the specific filters ([e5f9b8d7a0](https://github.com/Alluxio/alluxio/commits/e5f9b8d7a0){:target="_blank"})
* Fix access control issues with S3 API metadata directory ([7ab414ed06](https://github.com/Alluxio/alluxio/commits/7ab414ed06){:target="_blank"})
* Update `ListMultipartUploads` to prevent leaking other users’ upload IDs ([9364fd0190](https://github.com/Alluxio/alluxio/commits/9364fd0190){:target="_blank"})
* Require valid "Authorization" header in S3 API Proxy ([2f3c42cf8e](https://github.com/Alluxio/alluxio/commits/2f3c42cf8e){:target="_blank"})
* Fix S3 API writing objects yields `BucketNotFound 404` ([72f68208d4](https://github.com/Alluxio/alluxio/commits/72f68208d4){:target="_blank"})
* Add s3 rest service audit log ([fdcba75c7e](https://github.com/Alluxio/alluxio/commits/fdcba75c7e){:target="_blank"})

### Kubernetes and Docker
* Improve Helm Chart ([71260634f8](https://github.com/Alluxio/alluxio/commits/71260634f8){:target="_blank"})
* Fix generating proxy templates in helm-chart ([4bcde90623](https://github.com/Alluxio/alluxio/commits/4bcde90623){:target="_blank"})
* Enable java 11 in Alluxio base image ([c1c40bf442](https://github.com/Alluxio/alluxio/commits/c1c40bf442){:target="_blank"})
* Remove CSI client in Helm chart ([a65627debb](https://github.com/Alluxio/alluxio/commits/a65627debb){:target="_blank"})
* Change Dockerfile to use CentOS instead of Alpine ([298df80a47](https://github.com/Alluxio/alluxio/commits/298df80a47){:target="_blank"})
* Remove alluxio-fuse-client ([fa34430b5e](https://github.com/Alluxio/alluxio/commits/fa34430b5e){:target="_blank"})

### FUSE/POSIX API
* Modify Libfuse version configuration ([496e91069b](https://github.com/Alluxio/alluxio/commits/496e91069b){:target="_blank"})
* Fix fuse mount options and refactor path cache loader ([9cfba09aa0](https://github.com/Alluxio/alluxio/commits/9cfba09aa0){:target="_blank"})
* Make configuration source of truth in AlluxioFuse ([3a26baeabc](https://github.com/Alluxio/alluxio/commits/3a26baeabc){:target="_blank"})
* Fix alluxio-fuse unmount to get the right pid when no options ([49022a5443](https://github.com/Alluxio/alluxio/commits/49022a5443){:target="_blank"})
* Support setting sleep time for alluxio-fuse mount ([ca8db364cf](https://github.com/Alluxio/alluxio/commits/ca8db364cf){:target="_blank"})
* Avoid `chown` if the file already has correct owner and group ([a4a84ee0bf](https://github.com/Alluxio/alluxio/commits/a4a85ee0bf){:target="_blank"})
* Fix fuse check file name length method name ([d3dee12ce4](https://github.com/Alluxio/alluxio/commits/d3dee12ce4){:target="_blank"})

### UFS
* Add support for Azure Data lake Gen2 MSI ([5dfa1789c6](https://github.com/Alluxio/alluxio/commits/5dfa1789c6){:target="_blank"})
* Add support for OFS schema name ([cecaa37744](https://github.com/Alluxio/alluxio/commits/cecaa37744){:target="_blank"})
* Add configuration for kerberos authentication for ufs HDFS ([72f6763f13](https://github.com/Alluxio/alluxio/commits/72f6763f13){:target="_blank"})
* Delete temporary files when uploading files to OBS fails ([65a8084709](https://github.com/Alluxio/alluxio/commits/65a8084709){:target="_blank"})
* Fix Ozone mount failure ([4cc34dff2b](https://github.com/Alluxio/alluxio/commits/4cc34dff2b){:target="_blank"})

### CLI
* Support ignore delete mount point directory by ttl action ([2603b609ed](https://github.com/Alluxio/alluxio/commits/2603b609ed){:target="_blank"})
* Support strict version match option for mount ([2ed18f54a3](https://github.com/Alluxio/alluxio/commits/2ed18f54a3){:target="_blank"})
* Support `getMountTable` without invoke ufs ([86e2f8210f](https://github.com/Alluxio/alluxio/commits/86e2f8210f){:target="_blank"})
* Support record audit log for `getMountTable` op ([82aa6633ff](https://github.com/Alluxio/alluxio/commits/82aa6633ff){:target="_blank"})

### Error and Exception Handling
* Make worker error propagate to client ([9cee334eb2](https://github.com/Alluxio/alluxio/commits/9cee334eb2){:target="_blank"})
* Fix worker swallow OOM ([aae5a02a5f](https://github.com/Alluxio/alluxio/commits/aae5a02a5f){:target="_blank"})
* Support failover worker while reading ([5de0314361](https://github.com/Alluxio/alluxio/commits/5de0314361){:target="_blank"})
* Filter exception that need to be retried in `ObjectUnderFileSystem` ([d5ea085afc](https://github.com/Alluxio/alluxio/commits/d5ea085afc){:target="_blank"})
* Force metadata sync when data read fails due to out-of-range error ([badca18e3f](https://github.com/Alluxio/alluxio/commits/badca18e3f){:target="_blank"})
* Catch runtime exception in rpc ([4b3fac5dd1](https://github.com/Alluxio/alluxio/commits/4b3fac5dd1){:target="_blank"})
* Update worker exception ([6982d6c759](https://github.com/Alluxio/alluxio/commits/6982d6c759){:target="_blank"})

### Metrics and Monitoring
* Fix gauges when creating a new rpc server ([7a4e35240f](https://github.com/Alluxio/alluxio/commits/7a4e35240f){:target="_blank"})
* Add an overloaded check according to the JVM pause time ([d064cbff66](https://github.com/Alluxio/alluxio/commits/d064cbff66){:target="_blank"})
* Add direct mem used metrics ([fea89c61e1](https://github.com/Alluxio/alluxio/commits/fea89c61e1){:target="_blank"})
* Initialize `AuditLog` writer in `WebServer` for proxy ([1c24dc3425](https://github.com/Alluxio/alluxio/commits/1c24dc3425){:target="_blank"})
* Add some metrics of threads and docs of worker `CacheManager` threads ([e07e17d510](https://github.com/Alluxio/alluxio/commits/e07e17d510){:target="_blank"})

### StressBench and MicroBench
* Fix misuse of variable in `computeMaxThroughput` ([5e544aa421](https://github.com/Alluxio/alluxio/commits/5e544aa421){:target="_blank"})
* Add POSIX API to `StressClientIOBench` ([f6345919a1](https://github.com/Alluxio/alluxio/commits/f6345919a1){:target="_blank"})
* Fix `clientIO` stressbench throughput calculation ([7422dfb209](https://github.com/Alluxio/alluxio/commits/7422dfb209){:target="_blank"})
* Make `clientIO` a multi-node test ([94f3703c7f](https://github.com/Alluxio/alluxio/commits/94f3703c7f){:target="_blank"})
* Support multiple files random and sequential read in `StressWorkerBench` ([8e1a25df2c](https://github.com/Alluxio/alluxio/commits/8e1a25df2c){:target="_blank"})
* Implement Alluxio POSIX API master stressbench test ([b4af5969ae](https://github.com/Alluxio/alluxio/commits/b4af5969ae){:target="_blank"})
* Add microbenchmarks for multiple implementations of `BlockStore` ([5828d56a82](https://github.com/Alluxio/alluxio/commits/5828d56a82){:target="_blank"})

### Deprecations
* Clean up ignored table unit/integration tests and maven ([8875c66ed4](https://github.com/Alluxio/alluxio/commits/8875c66ed4){:target="_blank"})
* Remove ufs extension ([9fb093c10f](https://github.com/Alluxio/alluxio/commits/9fb093c10f){:target="_blank"})
* Remove conf & doc for tiered locality ([11c1c7c5bf](https://github.com/Alluxio/alluxio/commits/11c1c7c5bf){:target="_blank"})
* Remove Configuration and CLI of Alluxio table ([c0571e72a7](https://github.com/Alluxio/alluxio/commits/c0571e72a7){:target="_blank"})

### Miscellaneous
* Add `LANG` to `alluxio-env.sh` ([e842df2719](https://github.com/Alluxio/alluxio/commits/e842df2719){:target="_blank"})
* Allow dot in `chown` username or group ([9b04c8040f](https://github.com/Alluxio/alluxio/commits/9b04c8040f){:target="_blank"})
* Support Long type config values ([d61aee0992](https://github.com/Alluxio/alluxio/commits/d61aee0992){:target="_blank"})
* Ensure the HadoopFS default port is the same across all Hadoop fs ([25e4301a7b](https://github.com/Alluxio/alluxio/commits/25e4301a7b){:target="_blank"})
* Bump up maven frontend plugin for m1 arm support ([b69bf4df59](https://github.com/Alluxio/alluxio/commits/b69bf4df59){:target="_blank"})


## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.9.0 release. Especially, we would like to thank:

Bob Bai ([bobbai00](https://github.com/bobbai00){:target="_blank"}),
Haoning Sun ([Haoning-Sun](https://github.com/Haoning-Sun){:target="_blank"}),
Jie Fu ([DamonFool](https://github.com/DamonFool){:target="_blank"}),
Li Simian ([LDawns](https://github.com/LDawns){:target="_blank"}),
[LiuJiahao0001](https://github.com/liujiahao0001){:target="_blank"},
Shuai Wuyue ([shuaiwuyue](https://github.com/shuaiwuyue/){:target="_blank"}),
XiChen ([xichen01](https://github.com/xichen01){:target="_blank"}),
Xinli Shang ([shangxinli](https://github.com/shangxinli){:target="_blank"}),
[XuanlinGuan](https://github.com/XuanlinGuan){:target="_blank"},
[adol001](https://github.com/adol001){:target="_blank"},
[bigxiaochu](https://github.com/bigxiaochu){:target="_blank"},
dangxiaodong ([smdxdxd](https://github.com/smdxdxd){:target="_blank"}),
Tianbao Ding ([flaming-archer](https://github.com/flaming-archer){:target="_blank"}),
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"}),
Zhaoqun Deng ([secfree](https://github.com/secfree){:target="_blank"}),
Yanbin Zhang ([singer-bin](https://github.com/singer-bin){:target="_blank"}),
Xinyu Deng ([voddle](https://github.com/voddle){:target="_blank"}),
Yangchen Ye ([YangchenYe323](https://github.com/YangchenYe323){:target="_blank"}),
and Zhigang Huang ([zerorclover](){:target="_blank"})

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).