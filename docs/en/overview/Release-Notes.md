---
layout: global
title: Release Notes
group: Overview
priority: 7
---

December 29, 2021

* Table of Contents
{:toc}

## Highlights

### Address log4j Vulernabilities
* Alluxio 2.7.2 upgraded log4j to 2.17.0 to address known log4j security vulnerabilities.

### Improve Alluxio POSIX Compatibility

Alluxio 2.7.2 emphasizes the POSIX compatibility of Alluxio POSIX API. This release includes a set of POSIX compatibility improvements to support more POSIX workloads.
* Fix Fuse JNI code segment fault error, avoid Fuse crash when running `mv` linus command ([5cf3359](https://github.com/Alluxio/alluxio/commit/5cf33595cb8c6e6d2738a211e12d445b5e98b663){:target="_blank"})
* Add Fuse shell and support clearing client side metadata cache through Fuse mount point ([10e808c](https://github.com/Alluxio/alluxio/commit/10e808c55951eba6260d0d4dccee914d42fce00f){:target="_blank"})([b892c6](https://github.com/Alluxio/alluxio/commit/b892c642ecbce3dd465eb46f634cc419c2c07430){:target="_blank"})
* Support using NFS sharing Alluxio Fuse mount point ([e205ed3](https://github.com/Alluxio/alluxio/commit/e205ed32bcccf21da148505ef5dbb45dc50a3b90){:target="_blank"})([cf54b82](https://github.com/Alluxio/alluxio/commit/cf54b829f38b6032c58821dd0b10c90c489cabdd){:target="_blank"})
* Fuse error out when random write detected ([b0f421d](https://github.com/Alluxio/alluxio/commit/b0f421d0a151b788553ce6600923209d76be7206){:target="_blank"})
* Fuse get file status return correct file size when writing the file through the same Fuse mount point ([f49ed5f](https://github.com/Alluxio/alluxio/commit/f49ed5f85ea2e9fdc60097f99421cd14362c6ccc){:target="_blank"})
* Add Fuse and client alarm metrics ([108cf73](https://github.com/Alluxio/alluxio/commit/108cf73194038461e50335ea07fcce955d189be8){:target="_blank"})

### Reduce Wait Time When Changing Leadership
Alluxio 2.7.2 reduces the wait time when changing leadership in high availability mode and thus reduces the potential system downtime. ([63853ce](https://github.com/Alluxio/alluxio/commit/63853ce68973e7c367379910a4bc4ae089bafd2d){:target="_blank"})

## Improvements Since 2.7.1 
* Avoid slowness when checkng whether ufs is mounted, faster launching metrics web UI page ([94389c](https://github.com/Alluxio/alluxio/commit/94389c7c83980c48dad549af2521729146f0bd9d){:target="_blank"})
* Add web UI information in Kubernetes envronment ([a021cffa](https://github.com/Alluxio/alluxio/commit/a021cffa8d0e4a4e86e4058679e8a7c6ea26256e){:target="_blank"})
* Display worker block count in worker web UI ([93437fa](https://github.com/Alluxio/alluxio/commit/93437fa5763fb9e3f0fccd508208bf23b30041cc){:target="_blank"})
* Make UFS Cephfs seekable ([6fdbbe5](https://github.com/Alluxio/alluxio/commit/6fdbbe59d08900703ac76a29d7d6352fc6608765){:target="_blank"})
* Improve total blocks hashset to be sized ([cd19023](https://github.com/Alluxio/alluxio/commit/cd1902383555d65ea85a561d1840137052b9ff4e){:target="_blank"})
* Add S3 UFS unit tests ([f8a138](https://github.com/Alluxio/alluxio/commit/f8a13809668ec1bd97e4e394a23cd7d7593bf686){:target="_blank"})
* Implement Fuse IO stressbench write throughput test ([32f1cd3](https://github.com/Alluxio/alluxio/commit/32f1cd38bcfb4dbcd674b93bf6f43e616facc9d1){:target="_blank"})

## Bugfixes Since 2.7.1
* Fix NPE in object storage get object status ([6d62b8a](https://github.com/Alluxio/alluxio/commit/6d62b8ad64ac9b875b4a8769887fe828edc3dd2b){:target="_blank"})
* Avoid creating empty object for object storage root ([95d69b](https://github.com/Alluxio/alluxio/commit/95d69b2e7f0651f2c5fb01dca3fe54bde861c993){:target="_blank"})
* Fix bytebuf memory leak in worker embedded Fuse application ([1dd5df2](https://github.com/Alluxio/alluxio/commit/1dd5df245a4bbbd5ef06312c68fa08da8827ac59){:target="_blank"})
* Correct the timer and meter metrics after clearing them ([f28258](https://github.com/Alluxio/alluxio/commit/f2825833fbf5d473fe7265aae749e8b3d52df143){:target="_blank"})
* Upgrade log4j to 2.17.0 ([2454088](https://github.com/Alluxio/alluxio/commit/24540885c8dac5053f783b3e81a64ca023e44e62){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.7.2 release. Especially, we would like to thank:

Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
Bing Zheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}),
chuanying chen ([ccy00808](https://github.com/ccy00808){:target="_blank"}),
Kevin Cai ([kevincai](https://github.com/kevincai){:target="_blank"}), 
Shiqi Liu,
Tao He ([sighingnow](https://github.com/sighingnow){:target="_blank"}),
Tianbao Ding ([flaming-archer](https://github.com/flaming-archer){:target="_blank"}),
Xu Weize ([xwzbupt](https://github.com/xwzbupt){:target="_blank"}), 
Yantao xue ([jhonxue](https://github.com/jhonxue){:target="_blank"})
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}),
and Zhoujin

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).