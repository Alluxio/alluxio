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

## Improvements

### General Improvements
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

### Testing
* ([](){:target="_blank"})
* ([](){:target="_blank"})

## Bug Fixes
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})
* ([](){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.8.1 release. Especially, we would like to thank:

Haoning Sun, secfree, Baolong Mao, Bing Zheng, Ce Zhang, Chenliang Lu, Yaolong Liu, Luo Yili, Bob Bai, Deepak Shivamurthy, Jiaming Mai, Bin Ling, Jiahao Liu, Pan Liu, Shuai Wuyue, Xi Chen, Xinli Shang, Xuanlin Guan, Yuedong Wu, adol001, cangyin, dangweisysu, flaming archer, qian0817, singer-bin, xpbob, yangchenye, and yuyang wang.

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).