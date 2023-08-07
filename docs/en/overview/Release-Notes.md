---
layout: global
title: Release Notes
group: Overview
priority: 7
---

December 7, 2021

* Table of Contents
{:toc}

## Highlights

### Improve S3 API
* Fix S3 API `ListStatus` return value ([635ceb](https://github.com/Alluxio/alluxio/commit/635cebb7724d60bd676d0bba5c08d2a8b30e826f){:target="_blank"})
* Clean `tmp` directory and file when initate multipart download ([3edc44](https://github.com/Alluxio/alluxio/commit/3edc44542fc78a376cb8507a6def0026b9ad9d4e){:target="_blank"})
* Use virtual functions instead of AmazonS3 for temp token refresh ([bc84a90a1](https://github.com/Alluxio/alluxio/commit/bc84a90a1d845e10a0d8b85810750cdea031e43e){:target="_blank"})
* Modify `S3ALowLevelOutputStream` for potential AmazonS3 client refresh ([566ecd26](https://github.com/Alluxio/alluxio/commit/566ecd260b7424d42a40dfcaf9349e8940d1fa2c){:target="_blank"})
* Fix S3 complete multipart upload request timeout ([2e046aa](https://github.com/Alluxio/alluxio/commit/2e046aae83facd12cd2776b960e42d7a66cead8c){:target="_blank"})
* Modify the variable access modifier for usage in S3A ufs stream sublcass ([e5e53e08](https://github.com/Alluxio/alluxio/commit/e5e53e08600764843a78a0b4d9ea2bc4f5110471){:target="_blank"})
* Refactor `s3client` creation ([24cd14d5e](https://github.com/Alluxio/alluxio/commit/24cd14d5e003c8df1b8ba8685d2738ab0645c8d8){:target="_blank"})
* Fix S3 API `ListObject` and `ListObjectV2` ([6a2f4ed](https://github.com/Alluxio/alluxio/commit/6a2f4edc3b2a51eaf09aef0909024fae59c0e9ad){:target="_blank"})

### Avoid Pitfull of `ForkJoinPool.commonPool`
* Change the thread pool for `CompletableFuture.runAsync` ([2e2914](https://github.com/Alluxio/alluxio/commit/2e2914dc473435cbaae8924286fb59b8d195d39c){:target="_blank"})
* Change `JournalStateMachine`'s executor service ([7a2aaa](https://github.com/Alluxio/alluxio/commit/7a2aaae3c605349a1e2c8d9f530a54b70c3305c4){:target="_blank"})

## Metrics
* Add metric to monitor checkpoint ([da3697b7](https://github.com/Alluxio/alluxio/commit/da3697b7164ca2c948f48abc7bb14bbe305f701b){:target="_blank"})
* Add Job service metrics ([6a9794218c](https://github.com/Alluxio/alluxio/commit/6a9794218cd1edfd88e7e159a1801419fa98da7d){:target="_blank"})

## Improvements Since 2.7.0 
* Support Job master audit log ([b7b239](https://github.com/Alluxio/alluxio/commit/b7b239658dd3daf0fc998835dd78d39de3f22b1b){:target="_blank"})
* Refactor `ExecutorServiceBuilder` and add to Worker gRPC server ([635ceb](https://github.com/Alluxio/alluxio/commit/635cebb7724d60bd676d0bba5c08d2a8b30e826f){:target="_blank"})
* Enable standalone Fuse JVM pause monitoring service ([1dabaeb](https://github.com/Alluxio/alluxio/commit/1dabaeba80c4c547fe4d32c0da429b1e7377479d){:target="_blank"})
* Add API to resolve mount id from ufs path in FileSystem ([4f6336a6](https://github.com/Alluxio/alluxio/commit/4f6336a6fa5423d0751819ef6b43944785e4292b){:target="_blank"})
* Add mask in logging request ([3bf31e7](https://github.com/Alluxio/alluxio/commit/3bf31e7067a7d757184e205177d9869145456922){:target="_blank"})
* Enable mount specific `dir` in Alluxio in helm chart ([ea84a93](https://github.com/Alluxio/alluxio/commit/ea84a93d53dd7215043975be59716960126748da){:target="_blank"})
* Correct the property name in the helm charts ([cc3ed9e](https://github.com/Alluxio/alluxio/commit/cc3ed9e319c1c90706a9a8ab7bb54d25a435222d){:target="_blank"})
* Make short-circuit can fall back to gRPC when path `notfound` ([11e47c](https://github.com/Alluxio/alluxio/commit/11e47c48990a4703c606621a3ef1013ac0598419){:target="_blank"})
* Display unescape the ufs URI ([4acb22](https://github.com/Alluxio/alluxio/commit/4acb22031385ac05c6c9788ae25f9eb271fb4174){:target="_blank"})
* Fix missing placeholder in metric names ([998e6ca](https://github.com/Alluxio/alluxio/commit/998e6ca51c94c6bb0570159c22acd2c9982a61f7){:target="_blank"})
* Add another call to ensure compatibility with zk 3.4 ([b9a472](https://github.com/Alluxio/alluxio/commit/b9a4726c6081ac8c8e86ec015530658aa59d216a){:target="_blank"})
* Enable Zookeeper 3.4 compatibility mode for `ZKMasterInquireClient` ([48cdef9e](https://github.com/Alluxio/alluxio/commit/48cdef9e1ebb0c359a94c0def3d63d064739f37e){:target="_blank"})
* Update register stream timestamp more often ([62f980](https://github.com/Alluxio/alluxio/commit/62f980a9756e2f83ab1f5801e9040b3bee2d3a99){:target="_blank"})
* Support retry until policy give up ([15f8b3b](https://github.com/Alluxio/alluxio/commit/15f8b3b40bf5c580ed0879e8ca7c7174348f7ee8){:target="_blank"})
* Add `gitignore` for intermediate go binaries ([2a56d12](https://github.com/Alluxio/alluxio/commit/2a56d12759e4e1eada57ea87525cfb0946569b42){:target="_blank"})
* Improve/fix/upgrade CSI template ([051186f](https://github.com/Alluxio/alluxio/commit/051186fdb42a2308843b83f89e14cde6099925d0){:target="_blank"})
* Add clear metadata cache logic ([b892c64](https://github.com/Alluxio/alluxio/commit/b892c642ecbce3dd465eb46f634cc419c2c07430){:target="_blank"})
* Update Code Conventions doc ([f7b7558](https://github.com/Alluxio/alluxio/commit/f7b755887c0e4986558cfd359113c882e5da4192){:target="_blank"})
* Add limitation for S3 API Doc ([e01081a](https://github.com/Alluxio/alluxio/commit/e01081adc7c696f18162034e7001f1d2093ec980){:target="_blank"})
* Update Contributor Tools Doc ([432577](https://github.com/Alluxio/alluxio/commit/4325774f94b2acb2019adeaf3d53713529209e5d){:target="_blank"})

## Bugfixes Since 2.7.0
* Fix `distributedCp` test ([35639](https://github.com/Alluxio/alluxio/commit/35639fa14856539f49f55bad2786b66596737494){:target="_blank"})
* Use Powermock annotation to support `javax.crypto` ([ea4d76](https://github.com/Alluxio/alluxio/commit/ea4d7668b6bf00a480ee61c49963047bdb057db8){:target="_blank"})
* Fix redundant free when `inAlluxio` percentage is 0 ([e931e1f8](https://github.com/Alluxio/alluxio/commit/e931e1f8ae01d215d92d1b4287243adfb38fec32){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.7.1 release. Especially, we would like to thank:

Kevin Cai ([kevincai](https://github.com/kevincai){:target="_blank"}), 
Liwei Wang ([jffree](https://github.com/jffree){:target="_blank"}),
Tao He ([sighingnow](https://github.com/sighingnow){:target="_blank"}),
XiChen ([xichen01](https://github.com/xichen01){:target="_blank"}),
Yantao xue ([jhonxue](https://github.com/jhonxue){:target="_blank"})
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}),
Bing Zheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}),
Tianbao Ding ([flaming-archer](https://github.com/flaming-archer){:target="_blank"}),
Zheng Han, 
Jieliang Li ([ljl1988com](https://github.com/ljl1988com){:target="_blank"}), 
Tom Lee, ([tomscut](https://github.com/tomscut){:target="_blank"}),
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"})

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).