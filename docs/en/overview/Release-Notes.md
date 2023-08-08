---
layout: global
title: Release Notes
group: Overview
priority: 7
---

June 23, 2021

This is the first release on the Alluxio 2.6.X line.

Due to the improvements in the 2.5 release, a much larger number of users were able to leverage Alluxio’s ability to accelerate AI/ML workloads as a Data Orchestration framework. We have taken the feedback and contributions from the community and further improved the end-to-end AI/ML workflows with Alluxio. This has taken the form of simplified deployment, data management capabilities, performance optimizations, and enhanced visibility into system states.

The improvements in Alluxio 2.6 further enable the recommended architecture of running AI/ML workloads with Alluxio on any storage, and also improves the general maintainability and visibility of Alluxio clusters, especially for large scale deployments.

* Table of Contents
{:toc}

## Highlights

### Unified Alluxio Worker and FUSE Process

Alluxio 2.6 supports running FUSE as a part of a worker process ([documentation]({{ '/en/api/POSIX-API.html#fuse-on-worker-process' | relativize_url }})). Coupling the two helps reduce interprocess communication, which is especially evident in AI/ML workloads where file sizes are small and RPC overheads make up a significant portion of the I/O time. In addition, containing both components in a single process greatly improves the deployability of the software in containerized environments, such as Kubernetes.

### Improved Distributed Load

Alluxio 2.6 improves the efficiency of distributed load and its user experience ([documentation]({{ '/en/operation/User-CLI.html#distributedload' | relativize_url }})), in addition to more traceability and metrics for easier operability. Distributed load command is a key portion commonly used for many users to pre-load data and speed up subsequent training or analytics jobs.

### Instrumentation Framework

Alluxio 2.6 adds a large set of metrics and traceability features enabling users to drill into the system’s operating state ([documentation]({{ '/en/reference/Metrics-List.html' | relativize_url }})). These range from aggregated throughput of the system to summarized metadata latency when serving client requests. The new metrics can be used to measure the current serving capacity of the system and identify potential resource bottlenecks. Request level tracing and timing information can also be obtained for deep performance analysis.

## Improvements Since 2.5.0
* Add and improve metrics ([8069336](https://github.com/Alluxio/alluxio/commit/80693363229763cdbaa0c5e6ae3571b4e67c292d){:target="_blank"})
([42729d3](https://github.com/Alluxio/alluxio/commit/42729d319385ba3641e10138b342ed842149f13f){:target="_blank"})
([a7f5349](https://github.com/Alluxio/alluxio/commit/a7f53499d67e29e35fd469cfc9d61015c4474b6f){:target="_blank"})
([b689333](https://github.com/Alluxio/alluxio/commit/b6893335f2aacefb2778d9721986fa670dc09b1c){:target="_blank"})
([a2fdf20](https://github.com/Alluxio/alluxio/commit/a2fdf2079cf7b2c43791886850c4feb7d142262b){:target="_blank"})
([abfd6e1](https://github.com/Alluxio/alluxio/commit/abfd6e1c19aac27b2e68b9cc2c20681e0292fd0f){:target="_blank"})
([e0478e8](https://github.com/Alluxio/alluxio/commit/e0478e8eb5d202f58cdff5f3ed50aa5d5e41f0c3){:target="_blank"})
([82d155c](https://github.com/Alluxio/alluxio/commit/82d155cdbbf330ca1d96024c8d4971a7c082154b){:target="_blank"})
([49bc315](https://github.com/Alluxio/alluxio/commit/49bc315556d47cff1db9d07179b549d2f438c90a){:target="_blank"})
([6966476](https://github.com/Alluxio/alluxio/commit/696647645c30d3a63b4a197aa0dfce9e11df3c39){:target="_blank"})
([35c4189](https://github.com/Alluxio/alluxio/commit/35c4189d8fc1672ea33c33e5dba8722ac8c959eb){:target="_blank"})
([942e905](https://github.com/Alluxio/alluxio/commit/942e905ef94940de3ce863b895ab778b1ec3ef56){:target="_blank"})
([00867dd](https://github.com/Alluxio/alluxio/commit/00867ddc9bf6493462420d60f30f4a6f7fb74e5b){:target="_blank"})
([2202370](https://github.com/Alluxio/alluxio/commit/220237085593d731756e24249bcf88a4d6ea5710){:target="_blank"})
* Improve log and error message ([647c671](https://github.com/Alluxio/alluxio/commit/647c671f31b211f26d9580f26cbd97721d1f349d){:target="_blank"})
([c83557a](https://github.com/Alluxio/alluxio/commit/c83557aa83a2baac34901316679ed23bb43370dd){:target="_blank"})
([030968d](https://github.com/Alluxio/alluxio/commit/030968da8f664405fd28a6f3f5cf09867701ef6d){:target="_blank"})
([97faa15](https://github.com/Alluxio/alluxio/commit/97faa15b6784bcce043a6a3495c01581ec79b2b8){:target="_blank"})
([7b4abe1](https://github.com/Alluxio/alluxio/commit/7b4abe1dd481de7cce997b254e3b48e9c47fe75d){:target="_blank"})
([fe561c3](https://github.com/Alluxio/alluxio/commit/fe561c3aa2ef26e50f0cc547fef6e20bd8e57276){:target="_blank"})
([25e1882](https://github.com/Alluxio/alluxio/commit/25e1882baad606a813a84a15646746a06590df49){:target="_blank"})
* Improve test coverage and code stability ([431cafa](https://github.com/Alluxio/alluxio/commit/431cafa67afdcc9333d9eb5c68571286dbdaeeff){:target="_blank"})
([308992c](https://github.com/Alluxio/alluxio/commit/308992c0122c2d6723165e56cdf7b350087e73e5){:target="_blank"})
([702e061](https://github.com/Alluxio/alluxio/commit/702e0610225948afe5ecaec48c5ef6e666404338){:target="_blank"})
([22c2e51](https://github.com/Alluxio/alluxio/commit/22c2e518a0a0dd27232a3cdfe73dc4395ac19920){:target="_blank"})
([9e3e9df](https://github.com/Alluxio/alluxio/commit/9e3e9df4bb73b6aeb2c9ef1217d1071e65677cbb){:target="_blank"})
* Improve Rocks metastore ([0203366](https://github.com/Alluxio/alluxio/commit/02033663b9f6b20ebb8c262f5f93c343f887147e){:target="_blank"})
([ceaaef4](https://github.com/Alluxio/alluxio/commit/ceaaef4d2a6229a286279c6e599178c85a1a341a){:target="_blank"})
* Update dependency version ([abe1eec](https://github.com/Alluxio/alluxio/commit/abe1eec961467dc9d27b2442a45860c049e82b4b){:target="_blank"})
([b96b035](https://github.com/Alluxio/alluxio/commit/b96b03548ca43860481acc0baa64902a5d6f1263){:target="_blank"})
([be4ed06](https://github.com/Alluxio/alluxio/commit/be4ed06a4424fcb9ac5bc54ba1942b2192dc6d01){:target="_blank"})
([37bf0ad](https://github.com/Alluxio/alluxio/commit/37bf0ad51eb9efa1a6207c84a7bcd1e2d3176691){:target="_blank"})
([dcfe0c0](https://github.com/Alluxio/alluxio/commit/dcfe0c044d3b04c426d2363eee2ed30d7dd9624e){:target="_blank"})
([830d4a8](https://github.com/Alluxio/alluxio/commit/830d4a8dcdf468b54ac5a2fab2dad3b86511182f){:target="_blank"})
([a581793](https://github.com/Alluxio/alluxio/commit/a581793aa003dadc9614da67782517971d305cdd){:target="_blank"})
([bebe53e](https://github.com/Alluxio/alluxio/commit/bebe53e84390b051e30ac2040abff198ce5298a2){:target="_blank"})
([ee131ef](https://github.com/Alluxio/alluxio/commit/ee131efc279e141a151c00f3c201fb934efc7b46){:target="_blank"})
([7cc0982](https://github.com/Alluxio/alluxio/commit/7cc09828ee62894605c66c857ab19ab3cc843729){:target="_blank"})
([ef00fcd](https://github.com/Alluxio/alluxio/commit/ef00fcd1b59f0bc81ba30679fb3cdc4f89cfee97){:target="_blank"})
* Improve code style and structure ([ab17c50](https://github.com/Alluxio/alluxio/commit/ab17c506ae480f78dd79fd21d0c246744229a63e){:target="_blank"})
([91f33de](https://github.com/Alluxio/alluxio/commit/91f33defcc3485f5bcf58af0b63cfb09662bdc0f){:target="_blank"})
([4d40d65](https://github.com/Alluxio/alluxio/commit/4d40d6590b1bf14aeda13f45f8a748611c99f40d){:target="_blank"})
([14155df](https://github.com/Alluxio/alluxio/commit/14155df25c9366a8a1c6e310cb611bc7bcb8872c){:target="_blank"})
([27d9b2a](https://github.com/Alluxio/alluxio/commit/27d9b2ab48755f36c7d7efafb9ea4828ba5b4260){:target="_blank"})
([498ef0f](https://github.com/Alluxio/alluxio/commit/498ef0f9f62a83c62a43b7d285d56a1e10fd7e88){:target="_blank"})
([681076f](https://github.com/Alluxio/alluxio/commit/681076f483f780ae1291a499fca938babbd1a6b2){:target="_blank"})
* Add or improve configurations ([a272616](https://github.com/Alluxio/alluxio/commit/a272616e0ddf3bc27ea778f06482b54a3c8b8a5f){:target="_blank"})
([bccbda8](https://github.com/Alluxio/alluxio/commit/bccbda8d39ef8e4a9aa10b0061855310412ed91b){:target="_blank"})
([aab820c](https://github.com/Alluxio/alluxio/commit/aab820c7f589fdb8714baf2581142e136f6b36b0){:target="_blank"})
([ebe719b](https://github.com/Alluxio/alluxio/commit/ebe719bd9b2b309f74ffc38005de2b0e1f5145a0){:target="_blank"})
([d4890f9](https://github.com/Alluxio/alluxio/commit/d4890f96f5b804337eaac1d6fcf184491c1374c2){:target="_blank"})
([f0602ec](https://github.com/Alluxio/alluxio/commit/f0602ecae1c568dee0320354736c73e4fbf311dd){:target="_blank"})
([33f2697](https://github.com/Alluxio/alluxio/commit/33f2697ddb151636e0b305b74669b2ddfc5d5c4e){:target="_blank"})
* Improve distributed job commands ([c939e33](https://github.com/Alluxio/alluxio/commit/c939e33a5ca3c4decd27fe7b6080a0feb82d3b9d){:target="_blank"})
([281430f](https://github.com/Alluxio/alluxio/commit/281430f30dd6da2cee7181abecf52362c347f8a7){:target="_blank"})
([a07403f](https://github.com/Alluxio/alluxio/commit/a07403f4970c7283cb99a7d40236e9c5262c61df){:target="_blank"})
([2b429fc](https://github.com/Alluxio/alluxio/commit/2b429fc10644c4f5cae495b12c9b247eee787a2e){:target="_blank"})
([fc5a280](https://github.com/Alluxio/alluxio/commit/fc5a280504c73e01bd1a505a2b92391d85546afe){:target="_blank"})
([1f2c0fd](https://github.com/Alluxio/alluxio/commit/1f2c0fd5408cd9a367dc85197c7d501de33455fe){:target="_blank"})
([e3fd127](https://github.com/Alluxio/alluxio/commit/e3fd1275befb7b09186258ea9b4782fcfa9a8a44){:target="_blank"})
* Improve worker registration ([0374c9e](https://github.com/Alluxio/alluxio/commit/0374c9e5378619ecc2c02d47b348e1a3cf713026){:target="_blank"})
([d4f01ef](https://github.com/Alluxio/alluxio/commit/d4f01ef8f3773eb794e53bb537c5db8c70e2c4a0){:target="_blank"})
([412524c](https://github.com/Alluxio/alluxio/commit/412524c13357663f884e5206b3beb14d862491cf){:target="_blank"})
([23a31c0](https://github.com/Alluxio/alluxio/commit/23a31c0fd4d4714c38fd953c941fbb3df2381772){:target="_blank"})
([7fb8409](https://github.com/Alluxio/alluxio/commit/7fb84094a6075bcef5a103b40adcac4b26b724ca){:target="_blank"})
* Improve Kubernetes integration ([8fd77c4](https://github.com/Alluxio/alluxio/commit/8fd77c41cee3c1b0bf9c99f4ff443e1c550cbbe4){:target="_blank"})
([7964c9f](https://github.com/Alluxio/alluxio/commit/7964c9f6769621e1d961e7bd88caa4af6bf330ed){:target="_blank"})
([fac8df1](https://github.com/Alluxio/alluxio/commit/fac8df1de8c9c0701a4be35f3630e706ece6cf7e){:target="_blank"})
* Enhance worker side RPC handling ([4395b54](https://github.com/Alluxio/alluxio/commit/4395b546313a6144cce1ea0b18bfa082f9d80ec6){:target="_blank"})
* Move container level `securityContext` to pod level in Kubernetes ([076248d](https://github.com/Alluxio/alluxio/commit/076248d28edf9246594febdcc57f227d6b0b4455){:target="_blank"})
* Improve the metadata sync ([d41fb8d](https://github.com/Alluxio/alluxio/commit/d41fb8da2f314f3c85112e37aed3bdfba5731da0){:target="_blank"})
* Support stop master process on demotion ([72fb5d4](https://github.com/Alluxio/alluxio/commit/72fb5d42b48bce67da13061d429a0e5274dab9d6){:target="_blank"})
* Improve journal and add monitor for journal space usage ([a067b94](https://github.com/Alluxio/alluxio/commit/a067b949aff5b577340095d6cc270d116301413d){:target="_blank"})
([ae96f32](https://github.com/Alluxio/alluxio/commit/ae96f3274ddd242cc1179c1a7552bd3caae2acfa){:target="_blank"})
* Improve block reader ([3061a69](https://github.com/Alluxio/alluxio/commit/3061a69f900f4bc37ee25392fa8bf7195c1f7f3d){:target="_blank"})
* Improve Worker Interface and implementation ([2ad1e1e](https://github.com/Alluxio/alluxio/commit/2ad1e1e0b8a59671177a83d712754197be273667){:target="_blank"})
* Implement nondeterministic LRU ([feb4aa0](https://github.com/Alluxio/alluxio/commit/feb4aa08ea675ef1c49005c2f6c401e07bab80cf){:target="_blank"})
* Support open file for override in Fuse ([55cc3b7](https://github.com/Alluxio/alluxio/commit/55cc3b70278672ca0768634c9d1439350faa7cf5){:target="_blank"})
* Use Generic type to make `CreateFileEntry` extensible ([f8fe02d](https://github.com/Alluxio/alluxio/commit/f8fe02d8476702b367c0bf9256b1ba24689bcf64){:target="_blank"})
* Add `hostAliases` in Masters and Worker Pods ([9c64bf0](https://github.com/Alluxio/alluxio/commit/9c64bf046568402aa115b90e1f096e22431af703){:target="_blank"})
* Reduce Mem Copy in client ([32f64b6](https://github.com/Alluxio/alluxio/commit/32f64b63c5c64542054725d6b28c57bb7e485451){:target="_blank"})
* Reduce unnecessary creation of breadcrumbs ([d946128](https://github.com/Alluxio/alluxio/commit/d94612891d5fb8eeebced4a6d8c95fb8f6e4dba8){:target="_blank"})
* Improve Web UI ([3cfa340](https://github.com/Alluxio/alluxio/commit/3cfa34044953d5054bf138542f62ef016f69e20f){:target="_blank"})
([a734955](https://github.com/Alluxio/alluxio/commit/a73495546d58ff189d7ee17fd7d22447899eb5cd){:target="_blank"})
([71bdf3d](https://github.com/Alluxio/alluxio/commit/71bdf3d13384a622ac205855876181a54f1dd705){:target="_blank"})
* Support CephFS direct and native as underfs of Alluxio ([3ef4728](https://github.com/Alluxio/alluxio/commit/3ef4728df196c17e4eb397f6612e41b68fe26791){:target="_blank"})
* Prevent loading child inode metadata when not required ([6f18745](https://github.com/Alluxio/alluxio/commit/6f18745157e3e8d972cc1f6451dc0c2907de7d7f){:target="_blank"})
* Support CephFS as underfs of Alluxio ([d46003d](https://github.com/Alluxio/alluxio/commit/d46003d76f3fb49cd7d53a2b2717fa337c3dc1cc){:target="_blank"})
* Change default logserver PVC selectors to empty ([f9cc6fb](https://github.com/Alluxio/alluxio/commit/f9cc6fb09544f8fbb5532f565df8b6d251dacd85){:target="_blank"})
* Make connection pools per connection instead of per db for hive udb ([d6d7e78](https://github.com/Alluxio/alluxio/commit/d6d7e78a3e5fe5de0e26ea129e44c12d3d9181bc){:target="_blank"})
* Support bypass table when attach and sync db ([ef55f4a](https://github.com/Alluxio/alluxio/commit/ef55f4a3ee94f2f7d7df8226dc4f2e564dbc7a57){:target="_blank"})

## Bugfixes Since 2.5.0
* Fix regression in `AbsetUfsPathCache` ([55a3b28](https://github.com/Alluxio/alluxio/commit/55a3b28b753c954c94618888b4fc65ae4dc7ba34){:target="_blank"})
* Open the source file before creating target file during migrate ([c74924d](https://github.com/Alluxio/alluxio/commit/c74924ddb6b9d260f25936134e91289bcaef477a){:target="_blank"})
* Add a boundary check before Fuse read ([3aaf75b](https://github.com/Alluxio/alluxio/commit/3aaf75b713120561e4df50dcf7ba0937efb88295){:target="_blank"})
* Add resource loading fallback logic to `ExtensionClassLoader` ([342f17f](https://github.com/Alluxio/alluxio/commit/342f17f06ea777f73e8bf2988d6204d5bc6c2c54){:target="_blank"})
* Fix potential `directBuffer` leak ([6231603](https://github.com/Alluxio/alluxio/commit/6231603c766665dbedd528107fa19d326875242c){:target="_blank"})
* Fix `listStatus` on open file stream will complete the stream unexpectedly ([eeb4193](https://github.com/Alluxio/alluxio/commit/eeb4193461b65041f7cf26fe5d9aff1160ec0f9e){:target="_blank"})
* Fix the rejection of legitimate async caching requests because of the fulfill of queue ([1e7c2f7](https://github.com/Alluxio/alluxio/commit/1e7c2f7ed45ffe3f3e37c515a90ea3db04659a0e){:target="_blank"})
* Cancel sync job in middle of ufs calls if client cancelled ([a906c87](https://github.com/Alluxio/alluxio/commit/a906c87d7ec8f6f208839cd8d0102ae381d6a17b){:target="_blank"})
* Fix Fuse write then read problem in async release and support umount ([b1b3f40](https://github.com/Alluxio/alluxio/commit/b1b3f409ab3acfcd66d0e47b97c59f795c368837){:target="_blank"})
* Prevent NPE for `AbstractFileSystemShellTest` ([4c4ec92](https://github.com/Alluxio/alluxio/commit/4c4ec92c9a117dc5cebd668a0d3e5803c645e87e){:target="_blank"})
* Fix helm-chart Fuse hostpath type from `File` to `CharDevice` ([34eb7cb](https://github.com/Alluxio/alluxio/commit/34eb7cbe0a08212c7819f9851053c0977200bdd7){:target="_blank"})
* Fix master crash due to journal flush error ([70d6d0b](https://github.com/Alluxio/alluxio/commit/70d6d0b6965d99baed2ca09dad8b67a22f309608){:target="_blank"})
* Fix the worker hang issue ([c668ae8](https://github.com/Alluxio/alluxio/commit/c668ae80eb1206994320dc993b65e142205b57b4){:target="_blank"})
* Add null check for bucket column in Glue UDB ([3618aa2](https://github.com/Alluxio/alluxio/commit/3618aa2cd410a052e350e05463407c4ece73bd93){:target="_blank"})
* Fix `alluxio-fuse` stat for Alluxio-Worker-FUSE ([386a9f4](https://github.com/Alluxio/alluxio/commit/386a9f40a73f005a9eee2d5424b83ce323576115){:target="_blank"})
* Improve Ceph `InputStream` to be thread safe ([265c24f](https://github.com/Alluxio/alluxio/commit/265c24f618a9b3190347961ff495190ab05f79f4){:target="_blank"})
* Handle client cache failure on "no space left on device" ([89f8971](https://github.com/Alluxio/alluxio/commit/89f897169fb7823f24caebf0a7dc8cb97003fcca){:target="_blank"})
* Fix replicate job loading non-persisted files ([ce249e3](https://github.com/Alluxio/alluxio/commit/ce249e3876e8c6deed44fbcea36d6ee036a0e8b2){:target="_blank"})
* Fix a regression of uncleaned directories ([6ae85e6](https://github.com/Alluxio/alluxio/commit/6ae85e6a66602698635724d8cbe5e9c65e5d0dfa){:target="_blank"})
* Fix process local block in stream ([d605580](https://github.com/Alluxio/alluxio/commit/d60558040c18bed761a74b0f2c5e1418d66bc5a0){:target="_blank"})
* Fix packaging distributed load code into `DistributedLoadUtils` ([40af661](https://github.com/Alluxio/alluxio/commit/40af661d15104813f74eeda3790b7a366f7cb28f){:target="_blank"})
* Fix instrumentation setup ([837a41c](https://github.com/Alluxio/alluxio/commit/837a41c68b956ded56ae0fd41f0bf26b52ffbf49){:target="_blank"})
* Fix xstream security issue ([78f1ec2](https://github.com/Alluxio/alluxio/commit/78f1ec285076fa10230cbaa097de9b8ac06b9e15){:target="_blank"})
* Fix throw CCE when using Fuse while enable client cache bug ([f34b615](https://github.com/Alluxio/alluxio/commit/f34b615626f4d564d8bad00cb1ad4646086264a8){:target="_blank"})
* Fix possible `NullPointerException` when calling `e.getCause` ([81fe34e](https://github.com/Alluxio/alluxio/commit/81fe34ecfce67b81b7859abefe0e798efc18dd94){:target="_blank"})
* Fix NPE in Allocator and add some UT for Reviewer ([f8518d0](https://github.com/Alluxio/alluxio/commit/f8518d047e4464689141c20fcd9dffe6b331415e){:target="_blank"})
* Fix variable reference and exit status ([8de3ea2](https://github.com/Alluxio/alluxio/commit/8de3ea2a143f1eca6257a88d756654e5b3a83f11){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.6.0 release. Especially, we would like to thank:

Arthur Jenoudet ([jenoudet](https://github.com/jenoudet){:target="_blank"}),
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Bing Zheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}),
Binyang Li ([Binyang2014](https://github.com/Binyang2014){:target="_blank"}), 
Ce Zhang ([JySongWithZhangCe](https://github.com/JySongWithZhangCe){:target="_blank"}),
Chenliang Lu ([yabola](https://github.com/yabola){:target="_blank"}), 
Jinpeng Chi ([cutiechi](https://github.com/cutiechi){:target="_blank"}),
Daniel Pham ([danielcpham](https://github.com/danielcpham){:target="_blank"}),
Eugene Ma ([Eugene-Mark](https://github.com/Eugene-Mark){:target="_blank"}),
Haifeng Wang ([yuexingri](https://github.com/yuexingri){:target="_blank"}),
Jieliang Li ([ljl1988com](https://github.com/ljl1988com){:target="_blank"}), 
ja725,
Junfan Zhang ([zuston](https://github.com/zuston){:target="_blank"}),
leewish,
Mingchao Zhao ([captainzmc](https://github.com/captainzmc){:target="_blank"}),
Pan Liu ([liupan664021](https://github.com/liupan664021){:target="_blank"}), 
Peter Roelants ([horasal](https://github.com/horasal){:target="_blank"}),
Pramesh Gupta ([pramesh94](https://github.com/pramesh94){:target="_blank"}),
Xiang Chen ([cdmikechen](https://github.com/cdmikechen){:target="_blank"}),
Zhenwei Wu ([wuzhenwei-xx](https://github.com/wuzhenwei-xxx){:target="_blank"}),
Xiang Li ([waterlx](https://github.com/waterlx){:target="_blank"}),
Yantao Xue ([jhonxue](https://github.com/jhonxue){:target="_blank"}),
Yue Shao ([ys270](https://github.com/ys270)),
Yun Wang,
Zac Blanco ([ZacBlanco](https://github.com/ZacBlanco){:target="_blank"}), 
and Zhenyu Song ([sunnyszy](https://github.com/sunnyszy){:target="_blank"})

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).