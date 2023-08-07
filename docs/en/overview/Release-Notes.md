---
layout: global
title: Release Notes
group: Overview
priority: 7
---

August 9, 2021

* Table of Contents
{:toc}

## Improvements Since 2.6.0

### Metrics Improvements
* Add read time metrics for client local cache ([c4bd024](https://github.com/Alluxio/alluxio/commit/c4bd02438f7593d6b65c28780a0f7520c0343a87){:target="_blank"})
* Improve the metric of shadow cache for relatively small working set ([3422cb6](https://github.com/Alluxio/alluxio/commit/3422cb6d159a25951b2a8fe6181b86a92bf7dbc7){:target="_blank"})
* Add Fuse data structure size metrics and improve metrics docs ([d588684c](https://github.com/Alluxio/alluxio/commit/d588684c2801d1fb704e696734c29e8b868e96db){:target="_blank"})
* Add user info to ufs block read metrics ([fceea2a](https://github.com/Alluxio/alluxio/commit/fceea2a7684d47914996936527fb836845281b59){:target="_blank"})
* Support export JVM metric ([cd6fc34](https://github.com/Alluxio/alluxio/commit/cd6fc3448799bdbfb5c7980a4c62d2589962a7f2){:target="_blank"})
* Add `cluster_cache_hit_rate` metric ([0d8d17b](https://github.com/Alluxio/alluxio/commit/0d8d17bf40bc357e9578ce7d66e6c43f069f3207){:target="_blank"})
* Add `CacheContext` to `URIStatus` to enable per-read metrics ([3ce5298](https://github.com/Alluxio/alluxio/commit/3ce52983e6f50bfb7880b5a2cb13a18e4272170b){:target="_blank"})
* Implement Shadow Cache ([d135ce1](https://github.com/Alluxio/alluxio/commit/d135ce1c8f4147cb4ecebe2f0e7732f55d7ef713){:target="_blank"})

### UI Improvement
* Support display `clusterId` from master UI ([af54be3](https://github.com/Alluxio/alluxio/commit/af54be30dcc229e4c0903f6c6582ec67ce578331){:target="_blank"})
* Add journal checkpoint warnings on UI ([11ae654](https://github.com/Alluxio/alluxio/commit/11ae65457bc858b0405c25803cfcd24d2d243705){:target="_blank"})

### Other Improvements
* Add soft kill option to all processes ([df5dcab](https://github.com/Alluxio/alluxio/commit/df5dcab8bc308dfd2bf650a895865b13120a9866){:target="_blank"})
* Invalidate cache after write operation ([183552b](https://github.com/Alluxio/alluxio/commit/183552b789c23cc2dce28e16da120daffb43ce7b){:target="_blank"})
* Add Container Storage Interface (CSI) ([3ec7ef9](https://github.com/Alluxio/alluxio/commit/3ec7ef9ff14b1109357340aa4ce1600c08e8fa11){:target="_blank"})
* Improve server-side logging ([1bd9345](https://github.com/Alluxio/alluxio/commit/1bd9345615aac5791902787f5984aa258c509834){:target="_blank"})
* Disable RATIS's RPC level safety ([efbec94](https://github.com/Alluxio/alluxio/commit/efbec94a720e9afa6f968989cc8c92c1bb242cc6){:target="_blank"})
* Support range header for S3 `getObject` API ([1d0c089](https://github.com/Alluxio/alluxio/commit/1d0c0890e70a1a7835cd03b081acc506557f9bce){:target="_blank"})
* Support using regex stand for `schemeAuthority` for udb mount options ([62265d1](https://github.com/Alluxio/alluxio/commit/62265d1d451e652c828c41b5f4c17dee81180fa4){:target="_blank"})

## Bugfixes Since 2.6.0
* Fix `IndexOutOfBoundsException` on async cache ([6ad1e4f](https://github.com/Alluxio/alluxio/commit/6ad1e4fe77445e8689f6d3975b26e52165c9c3e6){:target="_blank"})
* Fix client pool leak ([9682207](https://github.com/Alluxio/alluxio/commit/968220711d7592cd6712712c75ca7cdd94ed79a3){:target="_blank"})
* Fix async cache error ([7d70714](https://github.com/Alluxio/alluxio/commit/7d70714388fb723e4adfe1ebcbb723f673025d53){:target="_blank"})
* Fix NPE during swap-restore plan generation ([030ce84](https://github.com/Alluxio/alluxio/commit/030ce84f87908b587efccbdb99cd9b150986bd2a){:target="_blank"})
* Fix integer overflow in `usedPercentage` ([3f73cf9](https://github.com/Alluxio/alluxio/commit/3f73cf9d28903a90b023db3c92c6c08bd96b902a){:target="_blank"})
* Fix exception propagation for RPC connection retries ([4705d65](https://github.com/Alluxio/alluxio/commit/4705d652a04397158d44fea668bcc2aa720228c0){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.9.3 release. Especially, we would like to thank:

Arthur Jenoudet ([jenoudet](https://github.com/jenoudet){:target="_blank"}),
Yunlong Kong ([BUPTAnderson](https://github.com/BUPTAnderson){:target="_blank"}),
Binyang Li ([Binyang2014](https://github.com/Binyang2014){:target="_blank"}), 
Eugene Ma ([Eugene-Mark](https://github.com/Eugene-Mark){:target="_blank"}),
Houliang Qi ([neuyilan](https://github.com/neuyilan){:target="_blank"}),
Kaijie Chen ([kaijchen](https://github.com/kaijchen){:target="_blank"}),
Song Juchao ([juchaosong](https://github.com/juchaosong){:target="_blank"}),
[WonderJingle](https://github.com/WonderJingle){:target="_blank"},
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}),
Zac Blanco ([ZacBlanco](https://github.com/ZacBlanco){:target="_blank"}), 
Zhan Yuan ([yuanzhanhku](https://github.com/yuanzhanhku){:target="_blank"}),
Zhenyu Song ([sunnyszy](https://github.com/sunnyszy){:target="_blank"}),
Zhichao Zhang ([zzcclp](https://github.com/zzcclp){:target="_blank"}), 
Bing Zheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}),
Chenliang Lu ([yabola](https://github.com/yabola){:target="_blank"}), 
[jayzhenghan](https://github.com/jayzhenghan){:target="_blank"},
Yantao xue ([jhonxue](https://github.com/jhonxue){:target="_blank"})
kqhzz ([kuszz](https://github.com/kuszz){:target="_blank"}),
Jieliang Li ([ljl1988com](https://github.com/ljl1988com){:target="_blank"}), 
[langlaile1221](https://github.com/langlaile1221){:target="_blank"},
Tom Lee ([tomscut](https://github.com/tomscut){:target="_blank"}),
[lzhcool](https://github.com/lzhcool){:target="_blank"},
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
[ns7381](https://github.com/ns7381){:target="_blank"},
Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"}),
and 林伟-161220078

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).