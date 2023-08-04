---
layout: global
title: Release Notes
group: Overview
priority: 7
---

April 19, 2022

* Table of Contents
{:toc}

## Highlights

We added a strong `PropertyType` in `PropertyKey` ([4f78836](https://github.com/Alluxio/alluxio/commit/4f7883665ca0a6c0b490a57fe41ca7b98028071c){:target="_blank"}), which will enforce property key type for all property keys, and validate values when they are set rather than retrieved.  By doing so, we’re able to store values in the expected format which will make the get operations become more efficient.

We added multi-disk support ([31ba1c](https://github.com/Alluxio/alluxio/commit/31ba1cfd8e2599231f7ce8b40cd02253e1b65a7c){:target="_blank"}) and in-heap cache support ([d102512](https://github.com/Alluxio/alluxio/commit/d102512ab7380e8f3fa5761246f5b8a51c714c22){:target="_blank"}) for the local cache.

We also fixed a bunch of flaky tests and improved our documentation and metrics.

## Improvements Since 2.7.3 
* Improve state-lock acquisition defaults ([6accf7](https://github.com/Alluxio/alluxio/commit/6accf76ff7915a94e78c88d8e0f96a0951d379eb){:target="_blank"})
* Include RocksDB into shaded client jar ([e552fa](https://github.com/Alluxio/alluxio/commit/e552fa727325f8ae8d570eb7cfd6b03c14dd1801){:target="_blank"})
* Improve logging and misc in FUSE integration ([061519](https://github.com/Alluxio/alluxio/commit/061519fcbe25d0b3fbed05c91e2e8acf33ef9eb7){:target="_blank"})
* Improve JNIFUSE JVM initialization and error handling ([677bb0](https://github.com/Alluxio/alluxio/commit/677bb0826e94fd6c564a49e91e4425fa15d9ca8b){:target="_blank"})
* Stop copying Alluxio configs in `cacheThroughRead` ([bc104a](https://github.com/Alluxio/alluxio/commit/bc104a99f02cff1850b64b45d50309ee07de4486){:target="_blank"})

## Bugfixes Since 2.7.3
* Fix UFS version check in hdfs `supportsPath` ([cb3c8b](https://github.com/Alluxio/alluxio/commit/cb3c8baceaf537704d398deff0e696f35bf5ddf5){:target="_blank"})
* Fix utimens in JNIFuse ([47cfde7](https://github.com/Alluxio/alluxio/commit/47cfde7c0e26d9063c0ca31673bb58b474e7ac95){:target="_blank"})
* Fix networkIO exception in `distributedCP` ([75210e](https://github.com/Alluxio/alluxio/commit/75210e0f9477811cd778df5bfe798e1c33924049){:target="_blank"})
* Fix for handling incomplete file ([31ba1c](https://github.com/Alluxio/alluxio/commit/31ba1cfd8e2599231f7ce8b40cd02253e1b65a7c){:target="_blank"})
* Fix RocksDB iterator leak on backup ([e2559a](https://github.com/Alluxio/alluxio/commit/e2559a6919573f86a078ec82f272e705d5175bd9){:target="_blank"})
* Fix `isSetByUser` reports incorrect config source ([2df1da](https://github.com/Alluxio/alluxio/commit/2df1da2d4f6caa47574640bf54c52447e8c0f3ea){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.7.4 release. Especially, we would like to thank:

Byron Zhang ([packageman](https://github.com/packageman){:target="_blank"}), 
Chen Junda, 
Haoning Sun ([Haoning-Sun](https://github.com/Haoning-Sun){:target="_blank"}),
Hoang Pham ([PhVHoang](https://github.com/PhVHoang){:target="_blank"}), 
Jack, 
Jason Tieu ([tieujason330](https://github.com/tieujason330)), 
Jiacheng Liu ([jiacheliu3](https://github.com/jiacheliu3)), 
Kevin Cai ([kevincai](https://github.com/kevincai)), 
Shiqi Liu, 
XiChen ([xichen01](https://github.com/xichen01){:target="_blank"}),
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}),
Yuang Zhang ([YuangZhang](https://github.com/YuangZhang){:target="_blank"}), 
Zac Blanco ([ZacBlanco](https://github.com/ZacBlanco){:target="_blank"}), 
Zhichao Zhang ([zzcclp](https://github.com/zzcclp){:target="_blank"}), 
Zijie Lu ([TszKitLo40](https://github.com/TszKitLo40{:target="_blank"})), 
[abcyhq](https://github.com/abcyhq){:target="_blank"}, 
Bing Zheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}),
Chenliang Lu ([yabola](https://github.com/yabola){:target="_blank"}), 
Wei Deng ([dengweisysu](https://github.com/dengweisysu){:target="_blank"}),
[heyingquan0030](https://github.com/heyingquan0030){:target="_blank"}, 
kyotom ([kyo-tom](https://github.com/kyo-tom){:target="_blank"}), 
[jja725](https://github.com/jja725){:target="_blank"}, 
l-shen ([ljl1988com](https://github.com/ljl1988com){:target="_blank"}), 
Peijie Zhou ([lilyzhoupeijie](https://github.com/lilyzhoupeijie){:target="_blank"}),
Lucas ([lucaspeng12138](https://github.com/lucaspeng12138){:target="_blank"}),
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"}),
[witheredwood](https://github.com/witheredwood){:target="_blank"}, 
xuankun zheng ([Rianico](https://github.com/Rianico){:target="_blank"}), 
Xu Weize ([xwzbupt](https://github.com/xwzbupt){:target="_blank"}), 
yangy, 
and 张庚昕

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).