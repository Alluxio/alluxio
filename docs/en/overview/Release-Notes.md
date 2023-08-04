---
layout: global
title: Release Notes
group: Overview
priority: 7
---

August 16, 2022

* Table of Contents
{:toc}

## Highlights

### Alluxio Stability Enhancement

Alluxio 2.8.1 added new test frameworks and conducted comprehensive system testing. A bunch of system vulnerabilities has been detected and fixed: 

* Fix RocksDB iterator leak for local cache ([a04c9a9](https://github.com/Alluxio/alluxio/commit/a04c9a97f19fd2d8f3d7aa071dfe12e830545456){:target="_blank"})
* Fix snapshot from follower thread leak ([94316d](https://github.com/Alluxio/alluxio/commit/94316d7ab9155e9b8a05b60702c173d5a59078cb){:target="_blank"})
* Fix BlockWorkerClient resource leak ([e32bada](https://github.com/Alluxio/alluxio/commit/e32bada550c76955b0e936dc003d140d7bba914a){:target="_blank"})
* Fix leaking threads in backup ([ff7c229](https://github.com/Alluxio/alluxio/commit/ff7c22977120f536eb297af7f920e129b35b7cb2){:target="_blank"})
* Fix lost block leak ([32b1aa](https://github.com/Alluxio/alluxio/commit/32b1aa0c765f15b93865c5a7f46226dac2d58d66){:target="_blank"})
* Bump junit version again to fix the security vulnerability ([e7818e6](https://github.com/Alluxio/alluxio/commit/e7818e6d2bebda3f476dc023bccc2d193989446e){:target="_blank"})

### Alluxio POSIX API Enhancement

Alluxio 2.8.1 ran a bunch of functionality verification and data correctness tests and fixed a bunch of issues found including Alluxio POSIX API in Kubernetes high resource consumption issue ([2f7cc8e](https://github.com/Alluxio/alluxio/commit/2f7cc8e0fcf36d747e86ba786f3c8e0daba85b81){:target="_blank"}), Fuse rename behavior mismatch with Linux ([938a92](https://github.com/Alluxio/alluxio/commit/938a9253968dbdcc3eeca05cfde7f7e71f2679a7){:target="_blank"}), FIO failure ([49ae5d0](https://github.com/Alluxio/alluxio/commit/49ae5d0b44a641ff7372f907124ed2f2599afdc9){:target="_blank"}), truncate issue ([370984](https://github.com/Alluxio/alluxio/commit/3709849766d1b3d2e7bd93075b226b4deba29275){:target="_blank"}), Fuse user group issue ([fccd691](https://github.com/Alluxio/alluxio/commit/fccd691c96fd123b876c0e1cf3406492d6465d54){:target="_blank"}), unmount pid ([34d46](https://github.com/Alluxio/alluxio/commit/34d46608538ed89a49c7a5ca5843bbb4c97f1d9c){:target="_blank"}), open flag ([65a151](https://github.com/Alluxio/alluxio/commit/65a15129e772d7c09fcb2b6de53565ab34ac1534){:target="_blank"}), remove directory error code ([e54b911](https://github.com/Alluxio/alluxio/commit/e54b91184986b1413bb7e51e8ebe3c29adef8e27){:target="_blank"}), write then overwrite ([001ee73](https://github.com/Alluxio/alluxio/commit/001ee73a569daa5eac7f877817c18c47327cf182){:target="_blank"}) and etc.

### New Alluxio CSI Driver

Alluxio CSI driver can mount Alluxio into application containers and thus applications can access data stored in Alluxio or UFSes through the mounted path.

CSI can couple the life cycle of applications and Alluxio Fuse processes to avoid unneeded waste of resources to keep AlluxioFuse processes always running. Only when application pods are ready will the AlluxioFuse processes be launched.

The new Alluxio CSI driver implementation further supports starting the AlluxioFuse process in its own pod. The lifecycle of the Fuse process can thus be independent from CSI Nodeserver to improve its stability. See [CSI documentation]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html#csi' | relativize_url }}) for more information about Alluxio CSI driver and how to enable this functionality.

### Improve Alluxio Metrics
Alluxio 2.8.1 further improves the Alluxio metrics for better monitoring the Alluxio system.

* Enable monitoring Alluxio metrics through JMX ([4e57dea](https://github.com/Alluxio/alluxio/commit/4e57dea7705359e08286548183a387e3f6186268){:target="_blank"})
* Add more GRPC metrics ([385cd9](https://github.com/Alluxio/alluxio/commit/385cd9b81b7612ac31aba6b9adcae4ac78f83e2a){:target="_blank"}) 
* Fix max reservoir metric initialization ([8a7ee7d](https://github.com/Alluxio/alluxio/commit/8a7ee7d8f3afed5fd272360baa5ce16c10fae5a9){:target="_blank"})
* Expose RockDB metrics as Alluxio metrics ([89f9087](https://github.com/Alluxio/alluxio/commit/89f90873531a63fe4237fd7df178afd2fd0c8558){:target="_blank"})

## Improvements and Bugfixes Since 2.8.0

### General Improvements
* Create automatic backup when shutting down Alluxio master ([eaf803c](https://github.com/Alluxio/alluxio/commit/eaf803c96a5971e7ad214fa3e39f4613cdb87160){:target="_blank"})
* Support stat a file through file id ([837c8c](https://github.com/Alluxio/alluxio/commit/837c8c2f4d356943ff709b0121ad02c106af274c){:target="_blank"})
* Improve eviction on cache retrieval if cache is full ([3b23f34](https://github.com/Alluxio/alluxio/commit/3b23f341552b57e407453d448f3c2537dbee53f1){:target="_blank"})
* Add new API to support loading batch of blocks into Worker efficiently ([fe54e6f](https://github.com/Alluxio/alluxio/commit/fe54e6fe5bbcaa1d385125e36a11293e88c89905){:target="_blank"})
* Implement load API ([50a282e](https://github.com/Alluxio/alluxio/commit/50a282e5f48aa00b6d3e83dd1b3d60079162dcea){:target="_blank"})
* Optimize the access speed of the `inAlluxioData` UI page ([c2218d1b](https://github.com/Alluxio/alluxio/commit/c2218d1b4cb55fb1639ddc36f62a25f8d8dc6f92){:target="_blank"})
* Add cache for `CapacityBaseRandomPolicy` ([3b1bdb3](https://github.com/Alluxio/alluxio/commit/3b1bdb3b80e56d3528bc6f41a923da6e28e808ee){:target="_blank"})
* Improve Alluxio CSI ([6e57678](https://github.com/Alluxio/alluxio/commit/6e57678c27525277656db1bc3aecb1b37520b964){:target="_blank"})

### Testing
* Add RocksDB and inode JMH benchmark ([6d9d50](https://github.com/Alluxio/alluxio/commit/6d9d503d8ca17f9f12478eda870a5073a19af7d6){:target="_blank"})
* Add compaction stress benchmark ([b3d5c4](https://github.com/Alluxio/alluxio/commit/b3d5c4318f3bc824f1a30633dcfcc3d7ca369162){:target="_blank"})

## Bug Fixes
* Fix S3 UFS thread number bug ([074870](https://github.com/Alluxio/alluxio/commit/0748708eeaa7a92ee631f5b80c940e25e9023c5e){:target="_blank"})
* Fix 2GB file size limitatio with S3 REST API ([htca824c6](https://github.com/Alluxio/alluxio/commit/ca824c6061226ee7d8b0542a125f0bfd8b696050){:target="_blank"})
* Support parsing resolved `InetSocketAddress` string ([c45c4a](https://github.com/Alluxio/alluxio/commit/c45c4a83404095ba392beccb6c002c07cf62358f){:target="_blank"})
* Support KiB, MiB, and more in Alluxio to embrace Kubernetes ([5dbaa96](https://github.com/Alluxio/alluxio/commit/5dbaa96a4811d914ea2e4533c3f6fc26c3df0a16){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.8.1 release. Especially, we would like to thank:

Haoning Sun ([Haoning-Sun](https://github.com/Haoning-Sun){:target="_blank"}), 
Zhaoqun Deng ([secfree](https://github.com/secfree){:target="_blank"}),
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Bing Zheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}), 
Ce Zhang ([JySongWithZhangCe](https://github.com/JySongWithZhangCe){:target="_blank"}), 
Chenliang Lu ([yabola](https://github.com/yabola){:target="_blank"}), 
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}), 
Luo Yili ([iluoeli](https://github.com/iluoeli){:target="_blank"}), 
Bob Bai ([bobbai00](https://github.com/bobbai00){:target="_blank"}), 
Deepak Shivamurthy ([deepak-shivamurthy](https://github.com/deepak-shivamurthy){:target="_blank"}), 
Jiaming Mai ([JiamingMai](https://github.com/JiamingMai){:target="_blank"}), 
Ling Bin ([lingbin](https://github.com/lingbin){:target="_blank"}), 
Jiahao Liu ([LiuJiahao0001](https://github.com/liujiahao0001){:target="_blank"}), 
Pan Liu ([liupan664021](https://github.com/liupan664021){:target="_blank"}), 
Shuai Wuyue ([shuaiwuyue](https://github.com/shuaiwuyue/){:target="_blank"}), 
XiChen ([xichen01](https://github.com/xichen01){:target="_blank"}),
Xuanlin Guan ([XuanlinGuan](https://github.com/XuanlinGuan){:target="_blank"}), 
Yuedong Wu ([lunarwhite](https://github.com/lunarwhite){:target="_blank"}), 
[adol001](https://github.com/adol001){:target="_blank"}, 
[cangyin](https://github.com/cangyin){:target="_blank"}, 
Wei Deng ([dangweisysu](https://github.com/dengweisysu){:target="_blank"}), 
Tianbao Ding ([flaming-archer](https://github.com/flaming-archer){:target="_blank"}), 
Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"}),
Yanbin Zhang ([singer-bin](https://github.com/singer-bin){:target="_blank"}),
[xpbob](https://github.com/xpbob){:target="_blank"}, 
Yangchen Ye ([YangchenYe323](https://github.com/YangchenYe323){:target="_blank"}),
and yuyang wang ([Jacksong-Wang-7](https://github.com/Jackson-Wang-7){:target="_blank"}).

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).