---
layout: global
title: Release Notes
group: Overview
priority: 7
---

February 4, 2022

* Table of Contents
{:toc}

## Highlights

### Address Some Memory Leaks & Crashes Involving RocksDB
* Areas of the codebase utilizing classes from the RocksDB library were updated to properly handle memory management ([918e739](https://github.com/Alluxio/alluxio/commit/918e739cba89b3bed54a24fde0890ba726b1164b){:target="_blank"}).

### Update Package Dependencies to Address Security Vulnerabilities
* Update hadoop dependencies ([af1e33e](https://github.com/Alluxio/alluxio/commit/af1e33ed14bce9e24cfce88228610294a6ea994a){:target="_blank"})
* Bump gson version ([dd017dc](https://github.com/Alluxio/alluxio/commit/dd017dcf1b2ab06f08348bdf461dce6a9bef6ba8){:target="_blank"})
* Update protobuf version ([176152c](https://github.com/Alluxio/alluxio/commit/176152c4d30649e2a70866fc081da0ee0653e16e){:target="_blank"})([1e234dd](https://github.com/Alluxio/alluxio/commit/1e234ddf201e1fd474fe8ca1b5fd691a917fc562){:target="_blank"})
* Update kubernetes packages ([7d2f142](https://github.com/Alluxio/alluxio/commit/7d2f142d5af7e04814c18a14952737d1de715b1f){:target="_blank"})
* Update jersey version ([2f3dffe](https://github.com/Alluxio/alluxio/commit/2f3dffefffcf492373e3590e2d52b3d635e9d6bb){:target="_blank"})
* Update guava to fix some CVEs ([fcb0b4c](https://github.com/Alluxio/alluxio/commit/fcb0b4c9d4f3b787a091a2721d0a7d807055caaa){:target="_blank"})
* Update log4j to 2.17.1 ([a25b0ca](https://github.com/Alluxio/alluxio/commit/a25b0ca49c38263d92d716e99fb4c29c6f414939){:target="_blank"})
* Address CVE-2022-23848 ([44d591b](https://github.com/Alluxio/alluxio/commit/44d591bd1045a4b829583592fa6652c584a5f398){:target="_blank"})([221f0ed](https://github.com/Alluxio/alluxio/commit/221f0ed5851417ffc6cae4758d112d490e25f930){:target="_blank"})

## Improvements Since 2.7.2 
* Add worker startup timeout property key ([067e9d4](https://github.com/Alluxio/alluxio/commit/067e9d432166d44a82fb26aa1ffa8660b665e2f0){:target="_blank"})
* Add stacks page in worker web UI ([fa54b1b](https://github.com/Alluxio/alluxio/commit/fa54b1bcd9fc891413cbd168862706d0fad0ad02){:target="_blank"})
* Support `statfs` for JNIFuse ([d3e231a](https://github.com/Alluxio/alluxio/commit/d3e231a02ea4ef1415e623cfbc742c5f69e8ba8c){:target="_blank"})
* Support web server/metrics sink in standby masters ([a10823a](https://github.com/Alluxio/alluxio/commit/a10823a9523e200f1665228cd4eb3ea7659a0d15){:target="_blank"})
* Support Alluxio allowed Fuse truncates ([4c8b555](https://github.com/Alluxio/alluxio/commit/4c8b555920da6a844f3f30cc70493b563178518d){:target="_blank"})
* Add `LoadTableCommand` ([f645405](https://github.com/Alluxio/alluxio/commit/f64540541748ba088eafbbac37fbd8c0458c410e){:target="_blank"})
* Support build Ozone 1.2.1, remove shaded-ozone module ([8993e46](https://github.com/Alluxio/alluxio/commit/8993e461b23a73713e62d81ac904161e59fc0562){:target="_blank"})
* Make the `checkConsistency` command check more cases ([20a7f4e](https://github.com/Alluxio/alluxio/commit/20a7f4edfea39bc17bfe17b067cf94d5034ac526){:target="_blank"})
* Make recursive options consistent in the filesystem shell ([99e76a8](https://github.com/Alluxio/alluxio/commit/99e76a85141cdbdd7a96d13a6cc4a58c15728277){:target="_blank"})

## Bugfixes Since 2.7.2
* Fix the bug which load metadata failed when the path is special ([ea1cdc5](https://github.com/Alluxio/alluxio/commit/ea1cdc5aa97ba0e4a36ee12f22a5f981cf7d7958){:target="_blank"})
* Fix Hub + k8s issues due to bad formatting and missing `log4j.properties` ([f49dc8b](https://github.com/Alluxio/alluxio/commit/f49dc8bc531e324a52d4d6986e5d4e621e7ef931){:target="_blank"})
* Fix metrics `Worker.ActiveClients` counter error ([7b64a95](https://github.com/Alluxio/alluxio/commit/7b64a953e4791e3e6c2a79274363f432b7775371){:target="_blank"})
* Fix argument in alluxio-fuse unmount when argument has trailing slash ([85ac996](https://github.com/Alluxio/alluxio/commit/85ac996f9f753154745ac598a655b2e484a51bce){:target="_blank"})
* Fix wrong method call to get username and wrong parameter assignment ([afe74e0](https://github.com/Alluxio/alluxio/commit/afe74e0a5489eac0fad1e78ce5b52b1a7b2a9754){:target="_blank"})
* Fix the concurrent read/write in `Fuse.open(READ_WRITE)` ([bb37aa8](https://github.com/Alluxio/alluxio/commit/bb37aa884ebd836f4679f6fcbcdd0cb2e1aa1688){:target="_blank"})
* Fix journal dumper not work ([3cb47b4](https://github.com/Alluxio/alluxio/commit/3cb47b45f2f1921c9a7735ef7087c874eabb5be4){:target="_blank"})
* Fix a NPE error when running test ([7f164c8](https://github.com/Alluxio/alluxio/commit/7f164c8b91ddbfaa60af036b0d04292431523fe3){:target="_blank"})
* Fix incorrect calc of `cacheHitRatio` ([80cbd8d](https://github.com/Alluxio/alluxio/commit/80cbd8de31e8b0697112156d5a8463945f252541){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.7.3 release. Especially, we would like to thank:

Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Haoning Sun ([Haoning-Sun](https://github.com/Haoning-Sun){:target="_blank"}),

([](){:target="_blank"})

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).