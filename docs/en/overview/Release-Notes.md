---
layout: global
title: Release Notes
group: Overview
priority: 7
---

November 16, 2021

This is the first release on the Alluxio 2.7.X line. Alluxio 2.7 further enhances the functionality and performance of machine learning and AI training workloads, serving as a key component in data pre-processing and training pipelines. Alluxio 2.7 also introduces improved scalability of job service operations by batching data management jobs. In addition, enhancements in master RPCs were added to enable launching clusters with a massive number of workers. We also improved fault tolerance capabilities in Alluxio 2.7.

* Table of Contents
{:toc}

## Highlights

### Deeper Integration with Machine Learning Workloads
Alluxio 2.7 emphasizes the integration with machine learning and AI training workloads. This release includes a set of optimizations specifically to support a large number of small files and highly concurrent data access, as well as improved POSIX compatibility. We were able to improve training-related tasks such as data preloading to reduce the time and achieve higher scalability.

To ensure the integration is solid for this type of workloads, there are multiple testing frameworks added in this release, including Alluxio embedded fuse stressbench, to prevent regression in functionality and performance in training workloads ([documentation]({{ '/en/operation/StressBench.html#fuse-io-stress-bench' | relativize_url }})). 

Alluxio 2.7 also contains improvements in Helm charts, CSI, and Fluid integration which help deploy Alluxio in Kubernetes clusters co-located with training jobs. More metrics and detailed docs are added to help users monitor, debug, and investigate issues with Alluxio POSIX API.

### Improved Job Service Scalability

Job Service has become an essential service in many deployments and data processing pipelines involving Alluxio. Alluxio 2.7 introduces Batched Job to reduce the management overhead in the job master and allows the job service to handle an order of magnitude more total jobs and concurrent jobs (see [configuration]({{ '/en/reference/Properties-List.html' | relativize_url }}) and [documentation]({{ 'en/operation/User-CLI.html#distributedload' | relativize_url }})). This is necessary as users are increasingly using Job Service to perform tasks like `distributedLoad` and `distributedCopy` on a very large number of files.

In addition, on the client side, more efficient polling is implemented so that pinning and `setReplication` commands on large directories place less stress on the job service. This allows users to pin a larger set of files.

### Better Master RPC Scalability

Alluxio 2.7 introduces better cluster scalability by improving the efficiency in how workers register to the master upon startup ([documentation]({{ '/en/operation/Scalability-Tuning.html#worker-registration' | relativize_url }})). When a large number of workers register with the master at the same time, this can overwhelm the master by overconsuming memory, leading to the master being unresponsive. 

Alluxio 2.7 introduces a master-side flow control mechanism, called “worker registration lease”. The master can now control how many workers may register at the same time, forcing additional workers to wait for their turn. This ensures a stable consumption of resources on the master process to avoid catastrophic situations.

Alluxio 2.7 also introduces a new option for the worker to break the request into a stream of smaller messages. Streaming will further smooth the master-side memory consumption registering workers, allowing higher concurrency and is more GC-friendly.

### Increase Fault Tolerance

Impact of sudden network partitions and node crashes within the Alluxio HA cluster can be fatal to running applications. With intelligent retry support, Alluxio 2.7 now efficiently tracks each RPC in the journal in order for applications to have improved fault tolerance.

## Improvements and Bugfixes Since 2.6.2

### Docker, CSI, and K8s
* Add toggle to Helm chart to enable or disable master & worker resources ([17e0542](https://github.com/Alluxio/alluxio/commit/17e0542aa4c356214b0bd7da99d4515453600044){:target="_blank"})
* Add `ImagePullSecrets` to Helm chart ([c775c70](https://github.com/Alluxio/alluxio/commit/c775c70328f504624e29e2fcffadb06eadab69ea){:target="_blank"})
* Fix url address of the aliver workers when workers start with kubernetes ([3239740](https://github.com/Alluxio/alluxio/commit/3239740f44bbfd3601134902a365936da4aa5bfa){:target="_blank"})
* Add probe improvements to Helm chart ([d7c252d](https://github.com/Alluxio/alluxio/commit/d7c252da36a2c185f46209e67be066b36afaa50e){:target="_blank"})
* Fix load command trigger cache block fail in container env ([a25f0a1](https://github.com/Alluxio/alluxio/commit/a25f0a1ee4c9e4bb2900e1fa628719cf91617627){:target="_blank"})
* Add fail back logic for counting cpu in container is 1 ([fe0420c](https://github.com/Alluxio/alluxio/commit/fe0420c6c4bc65310637351aae2df3d01ca11ff2){:target="_blank"})
* Support CSI in both docker images ([575d205](https://github.com/Alluxio/alluxio/commit/575d205c93c6bd773251f8774fb700b4cc46a4c9){:target="_blank"})

### Metrics
* Fix master lost file count metric is always 0 ([91fb695](https://github.com/Alluxio/alluxio/commit/91fb6955a25fa19f83e06ead34a679a0dc03f86c){:target="_blank"})
* Fix worker cannot report worker cluster aggregated metrics issue ([aa823e4](https://github.com/Alluxio/alluxio/commit/aa823e465cbb1b62fd4ab27eb5a5e33794ecdb62){:target="_blank"})
* Add worker and client metrics size to log info ([cc50847](https://github.com/Alluxio/alluxio/commit/cc5084720bb870d7b208bd2683939750c1a2bd41){:target="_blank"})
* Add worker `threadPool` and task metrics ([fed20cd](https://github.com/Alluxio/alluxio/commit/fed20cde9377d24791055055ea722e20bad3cae0){:target="_blank"})
* Add active job size metric ([23f3a5d](https://github.com/Alluxio/alluxio/commit/23f3a5d21feaf7157a1090f29b1e4317f8049007){:target="_blank"})
* Fix the `Worker.BlocksEvicted` is zero all the time issue ([492647b](https://github.com/Alluxio/alluxio/commit/492647bb02fe44d6a08578d19899aeb7e97d9032){:target="_blank"})
* Add metric of audit log ([a06d374](https://github.com/Alluxio/alluxio/commit/a06d374bfd82a138553a5a8c3418a1959bc56b95){:target="_blank"})
* Add metric of raft journal index ([3df0ef4](https://github.com/Alluxio/alluxio/commit/3df0ef424a95c3a766a755ec337b884fa0f7a0bb){:target="_blank"})
* Add lock pool metrics ([fb1f1b8](https://github.com/Alluxio/alluxio/commit/fb1f1b87fa755c4b9392e32269598d7439609d58){:target="_blank"})

### S3 API
* Fix S3 bucket region ([e76258f](https://github.com/Alluxio/alluxio/commit/e76258f4d4d9a17ca4a1cb709ff37b4ee6803922){:target="_blank"})
* Implement S3 API features required to use basic Spark operations ([23be470](https://github.com/Alluxio/alluxio/commit/23be4702e03d076f10ddb79814cf0bd9b6451609){:target="_blank"})
* Support `listObjectV2` for S3 REST API ([23f5b85](https://github.com/Alluxio/alluxio/commit/23f5b85e69b5eeab5622149321ae356f728bcedb){:target="_blank"})
* Fix S3 REST API `listObjects` delimiter process erorr ([0892405](https://github.com/Alluxio/alluxio/commit/089240538124b8acd836a11144a391711271ff7f){:target="_blank"})
* Set S3 endpoint region and support OCI object storage ([36a4d5c](https://github.com/Alluxio/alluxio/commit/36a4d5c93c2756bcaae5c76cc295a5442d6abad8){:target="_blank"})
* Fix S3 `listBuckets` REST API non-standard response issue ([bf11633](https://github.com/Alluxio/alluxio/commit/bf11633d40292cfcc5aeff715302a9f3f3ec998b){:target="_blank"})
* Add application/octet-stream to consumed content types in S3 Handler ([0901eb6](https://github.com/Alluxio/alluxio/commit/0901eb6160368c9441f444e2f27f0b4388927f89){:target="_blank"})

### StressBench
* Add lease support to StressBench tests ([a6e2a14](https://github.com/Alluxio/alluxio/commit/a6e2a14dc642580a20faddca24b53393b62e656e){:target="_blank"})
* Fix RPC benchmarks extra prepartion and low concurrency ([99cb44d](https://github.com/Alluxio/alluxio/commit/99cb44d007d7f4d13396bda9e9182c505655cb90){:target="_blank"})
* Add `maxThroughput` for job service ([0355f12](https://github.com/Alluxio/alluxio/commit/0355f12056e726bf8db81c3d9f75feb118e20735){:target="_blank"})
* Fix `-create-file-size` param in Stress Master Bench ([bd44e98](https://github.com/Alluxio/alluxio/commit/bd44e9838cce07eaed4b16348e6b55db52ef5fcf){:target="_blank"})
* Add recursive to create a file in case of missing parent directory ([0ba1baf](https://github.com/Alluxio/alluxio/commit/0ba1bafb339fededf331d3a8e453637683155415){:target="_blank"})
* Add Alluxio native API for max throughput in StressBench ([bfccb96](){:target="_blank"})
* Add Alluxio native API call for `ClintIO` bench ([872e68b](https://github.com/Alluxio/alluxio/commit/872e68b28b9d20df2d933d65dadbd011f397112b){:target="_blank"})
* Update Job service bench ([b6ce813](https://github.com/Alluxio/alluxio/commit/b6ce8137340fe81f93f9d575d93a7c1465f9b6c4){:target="_blank"})
* Add three types of read and cluster mode in Fuse Stress Bench ([af1477d](https://github.com/Alluxio/alluxio/commit/af1477d6ddaa4581d30fb31ed3fb3b24061fcf39){:target="_blank"})

### General Improvements
* Web UI pages and displayed metrics ([33b8a24](https://github.com/Alluxio/alluxio/commit/33b8a24cf41543a203b9b231625987803edb9f8a){:target="_blank"})([b20ef39](https://github.com/Alluxio/alluxio/commit/b20ef39d68113e6a05278fa3064df808eba0a742){:target="_blank"})([73bf2ce](https://github.com/Alluxio/alluxio/commit/73bf2ceb6ba0d450a8ce9fd471f3ecd237efb22b){:target="_blank"})([4087a38](https://github.com/Alluxio/alluxio/commit/4087a38eb877f351e05059c72746e73c56ca732e){:target="_blank"})
* Intellij support for Alluxio processes ([71165a5](https://github.com/Alluxio/alluxio/commit/71165a5da66df06425265247f32d631c34692489){:target="_blank"})([2458871](https://github.com/Alluxio/alluxio/commit/24588710dc19eaf1d2bd09b5c64665185c21382e){:target="_blank"})([0a5669c](https://github.com/Alluxio/alluxio/commit/0a5669cc1f7989ed8f895b0ead8165f60e879f9f){:target="_blank"})([69273af](https://github.com/Alluxio/alluxio/commit/69273af8dfcff617b9a29ea120f7daec2a199631){:target="_blank"})
* Implement configurable `ExecutorService` creation for Alluxio processes ([0eb8e89](https://github.com/Alluxio/alluxio/commit/0eb8e89af2db9c3014cf1cafef64188cefa41645){:target="_blank"})
* Bump RocksDB version to 6.25.3 ([b41b519](https://github.com/Alluxio/alluxio/commit/b41b51951b13f1236e9a39908e6262234737dc3b){:target="_blank"})
* Convert worker registration to a stream ([7c020f6](https://github.com/Alluxio/alluxio/commit/7c020f6f277fcc2fd74a91240f612e12ed1c6e96){:target="_blank"})
* Add master-side flow control for worker registration with `RegisterLease` ([e883136](https://github.com/Alluxio/alluxio/commit/e883136af0aa545fe46c83849560782f10766e30){:target="_blank"})
* Replace `ForkJoinPool` with `FixedThreadPool` ([0cd3b7e](https://github.com/Alluxio/alluxio/commit/0cd3b7e3240f4c27d27191e1d8fe1a2e76a61d04){:target="_blank"})
* Add Batched job for job service ([dc95b3b](https://github.com/Alluxio/alluxio/commit/dc95b3bb0ad18179140970d666237b12b53806a8){:target="_blank"})
* Reset priorities after transfer for leadership ([6a18760](https://github.com/Alluxio/alluxio/commit/6a1876002e276c1a86d22b49f212e385f574f086){:target="_blank"})
* Implement Resource Leak Detection ([ab5e505](https://github.com/Alluxio/alluxio/commit/ab5e505a7ad6df477e5b23e0c43cd06c511b4580){:target="_blank"})
* Add `ReconfigurableRegistry` and `Reconfigurable` ([5e5b3e6](https://github.com/Alluxio/alluxio/commit/5e5b3e6384a4a014162d87fff924484d0b1111c9){:target="_blank"})
* Remove fall-back logic from journal appending ([4f870de](https://github.com/Alluxio/alluxio/commit/4f870de77caaceaa5dc30229330a95d0e4eb4939){:target="_blank"})
* Add retry suport for intializing `AlluxioFileOutStream` ([2134192](https://github.com/Alluxio/alluxio/commit/2134192a1debbe8cee1107cdce51ac84eeffc072){:target="_blank"})
* Allow higher keep-alive frequency for worker data server ([bd7a958](https://github.com/Alluxio/alluxio/commit/bd7a958fa7f0c15119098f41f9b8b02f1d49d3b3){:target="_blank"})
* Enable more settings on job-master's RPC server ([b14f6e6](https://github.com/Alluxio/alluxio/commit/b14f6e6fc031352eb07df252731dae602be61652){:target="_blank"})
* Increase allowed keep-alive rate for master RPC server ([c061b93](https://github.com/Alluxio/alluxio/commit/c061b93cb5825916c33b2e282667d23ae38e374a){:target="_blank"})
* Improve exception message in block list merger function ([62a9958](https://github.com/Alluxio/alluxio/commit/62a9958b1c381b189eaa774f181b11486bf0ec62){:target="_blank"})
* Implement FS operation tracking for improving partition tolerance ([a9f121c](https://github.com/Alluxio/alluxio/commit/a9f121c2e0d7444e168f336f5981e4938626a19a){:target="_blank"})
* Enable additional keep-alive configuration on gRPC servers ([3f94a5b](https://github.com/Alluxio/alluxio/commit/3f94a5bc5225613e2b01f4b88be18a6a10b7204b){:target="_blank"})
* Introduce client-side keep-alive monitoring for channels of RPC network ([e6cded4](https://github.com/Alluxio/alluxio/commit/e6cded491eabb439fbda0195ba650ee6a1a4c35d){:target="_blank"})
* Support logical name as HA address ([0526fae](https://github.com/Alluxio/alluxio/commit/0526fae640b724bd4fbda7813ab65750c410ab87){:target="_blank"})
* Support Huawei Cloud (PFS & OBS) ([0cf6b91](https://github.com/Alluxio/alluxio/commit/0cf5b91714954508c4d31957aa3dfd83922abf17){:target="_blank"})
* Introduce deadlines for sync RPC stubs ([70112cb](https://github.com/Alluxio/alluxio/commit/70112cb226119c7bb73f8ce0c25252d099cdd840){:target="_blank"})
* Use `jobMasterRpcAddresses` to judge `ServiceType` ([b0eba95](https://github.com/Alluxio/alluxio/commit/b0eba958adf408245d6962055cadf9218cc97a7f){:target="_blank"})
* Support shuffle master addresses for RPC client ([665960e](https://github.com/Alluxio/alluxio/commit/665960e5d526bfcbd3c2f18a2a77c902447054e8){:target="_blank"})

### Bugs
* Resolve lock leak during recursive delete ([ae281b5](https://github.com/Alluxio/alluxio/commit/ae281b551ee15dd61b6fef1ecbf36843a934811b){:target="_blank"})
* Fix `startMasters()` from receiving unchecked exceptions ([8fce673c](https://github.com/Alluxio/alluxio/commit/8fce673cb5cbe32d504959df17f553e34a688f5b){:target="_blank"})
* Close UFS in `UfsJournal` close ([b2c256e3](https://github.com/Alluxio/alluxio/commit/b2c256e332b1cbf3615ede0559d22dd34946092d){:target="_blank"})
* Add filtering to `MergeJournalContext` ([f8b2cd1](https://github.com/Alluxio/alluxio/commit/f8b2cd1ab3b4a1fe5efa0c796c6bb7da358eeb98){:target="_blank"})
* Fix `distributedLoad` after file is already loaded ([924199f](https://github.com/Alluxio/alluxio/commit/924199f033755adade9facfab0422f12db2b0744){:target="_blank"})
* Save transfer of leadership from unchecked exceptions ([146bbce](https://github.com/Alluxio/alluxio/commit/146bbceafa71d1bf1c9858268db712f2e334cc47){:target="_blank"})
* Fix null pointer exception for `AuditLog` ([6d2ce19](https://github.com/Alluxio/alluxio/commit/6d2ce19a3eb6294b7d2b997fbf0daf024101ad71){:target="_blank"})
* Return directly when an exception is thrown during transfer leader ([bddb70e](https://github.com/Alluxio/alluxio/commit/bddb70e9c75b6945c4212f6054872894c033fef5){:target="_blank"})
* Fix `SnapshotLastIndex` update error ([fa99e3873](https://github.com/Alluxio/alluxio/commit/fa99e3873c7554d6e3333efe55dbf59a8cf0a821){:target="_blank"})
* Fix `RetryHandlingJournalMasterClient` ([4de3acd](https://github.com/Alluxio/alluxio/commit/4de3acd5e84549182f27dbf1eb8679c0082d9f6b){:target="_blank"})
* Fix `copyFromLocal` command ([1662dcf](https://github.com/Alluxio/alluxio/commit/1662dcf9fd934194c33e7cb5b1cd410806bb5483){:target="_blank"})
* Fix the semantic ambiguity of `LoadMetadataCommand` ([b7e67d5](https://github.com/Alluxio/alluxio/commit/b7e67d5b0ca1c561b72c791b1d1c2e64d22bca33){:target="_blank"})
* Fix recursive path cannot be created bug when init multipart upload ([28ee226c](https://github.com/Alluxio/alluxio/commit/28ee226ca48950efdf769374c038a36ff921a1af){:target="_blank"})
* Catch errors during explicit cache cleanup ([5339349](https://github.com/Alluxio/alluxio/commit/5339349a77e7d315b1afecdb2519dd1c8e03d277){:target="_blank"})
* Fix leader master priority always 0 ([01347b2](https://github.com/Alluxio/alluxio/commit/01347b2329c99e875d8e0da10ccb3edb891983d8){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.9.3 release. Especially, we would like to thank:

Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Bing Zheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}),
Binyang Li ([Binyang2014](https://github.com/Binyang2014){:target="_blank"}), 
Chenliang Lu ([yabola](https://github.com/yabola){:target="_blank"}), 
Jieliang Li ([ljl1988com](https://github.com/ljl1988com){:target="_blank"}), 
Kevin Cai ([kevincai](https://github.com/kevincai){:target="_blank"}), 
Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"}),
Liwei Wang ([jffree](https://github.com/jffree){:target="_blank"}),
Ryan Zang ([ryantotti](https://github.com/ryantotti){:target="_blank"}),
Steven Choi ([Stevenchooo](https://github.com/Stevenchooo){:target="_blank"}),
Tao Li,
Tianbao Ding ([flaming-archer](https://github.com/flaming-archer){:target="_blank"}),
XiChen ([xichen01](https://github.com/xichen01){:target="_blank"}),
Yang Yu ([yuyang733](https://github.com/yuyang733){:target="_blank"}),
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}),
and Zac Blanco ([ZacBlanco](https://github.com/ZacBlanco){:target="_blank"})

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).