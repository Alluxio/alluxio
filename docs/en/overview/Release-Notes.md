---
layout: global
title: Release Notes
group: Overview
priority: 7
---

September 17, 2021

* Table of Contents
{:toc}

## Highlights

### Add Alluxio Stress Test Framework

[Alluxio StressBench]({{ '/en/operation/StressBench.html' | relativize_url }}) is a built-in tool to benchmark the performance of an Alluxio deployment without any extra services. Alluxio 2.6.2 supports the following suites to benchmark: 
* Master RPC throughput ([430e4a](https://github.com/Alluxio/alluxio/commit/430e4a8df3cf99ed03cf4a050c7064d550b2c0ad){:target="_blank"})
    * `bin/alluxio runClass alluxio.stress.cli.RegisterWorkerBench`
    * `bin/alluxio runClass alluxio.stress.cli.WorkerHeartbeatBench`
    * `bin/alluxio runClass alluxio.stress.cli.GetPinnedFileIdsBench`
* Alluxio POSIX API read throughput ([634bd32](https://github.com/Alluxio/alluxio/commit/634bd32d1f589c91e58578f931b5abbeb8fcd348){:target="_blank"})
    * `bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench`
* Job Service throughput ([0cae910](https://github.com/Alluxio/alluxio/commit/f0cae91086e904d75f103a5edde50108dd5905a9){:target="_blank"}) 
    * `bin/alluxio runClass alluxio.stress.cli.StressJobServiceBench`

### Support Transfer Alluxio Leaderhsip During Runtime

When deploying a High Availability cluster using the Embedded Journal, users can now manually specify the leading master. This is useful when users want to debug or do maintenance on a server without killing an existing running master process. This new functionality transfers the leadership of the quorum gracefully to another master specified. 

The [docs]({{ '/en/operation/Admin-CLI.html#journal' | relativize_url }}) show how to use this new feature. ([d6c6733](https://github.com/Alluxio/alluxio/commit/d6c673323348b3ff3c1951d1d7c9a0d4071d69ff){:target="_blank"})([d67996c1](https://github.com/Alluxio/alluxio/commit/d67996c142b54a0e8ae6ebbbc01fc96a1c9efc73){:target="_blank"})([e79fcdd](https://github.com/Alluxio/alluxio/commit/e79fcddf8739faeda15365be99f508873278a7cb){:target="_blank"})

### Docker Images for Production and Development

In 2.6.2, users can pull  two separate docker images: `alluxio/alluxio:2.6.2` and `alluxio/alluxio-dev:2.6.2`. `alluxio/alluxio:2.6.2` is a docker image for production usage optimized for image size and `alluxio/alluxio-dev:2.6.2` installs extra tools for development usage. ([71f62c36](https://github.com/Alluxio/alluxio/commit/71f62c369e3dafd0fd910dd8762eec02da4c1d30){:target="_blank"})([768d45c](https://github.com/Alluxio/alluxio/commit/768d45c2984e83639bf85ce76abf2f51f180b7ab){:target="_blank"})

### Improve Alluxio Load Command

The [load command]({{ '/en/operation/User-CLI.html#load' | relativize_url }}) is improved to use the new worker API to avoid extra data copy to the client. ([05e081d1](https://github.com/Alluxio/alluxio/commit/05e081d15c0a98ac9939b9301f6f771652d6ab9){:target="_blank"})

## Metrics
A bunch of new metrics are added for users to better understand the Alluxio cluster status.
* Expose Prometheus metrics from all servers ([1a6054ad](https://github.com/Alluxio/alluxio/commit/1a6054ad8178d1ff8765949c0879f571d7e63d5c){:target="_blank"})
* Add metrics of Alluxio logging ([0fba8bb](https://github.com/Alluxio/alluxio/commit/0fba8bb168b10ab5cd8b9d15e8300687e708ad12){:target="_blank"})
* Print web metrics servlet page as human-readable format ([b1db0716](https://github.com/Alluxio/alluxio/commit/b1db0716971c094300d4d4157746630f4db9138e){:target="_blank"})
* Support export ratis metrics ([a684440](https://github.com/Alluxio/alluxio/commit/a684440b9cbfb911b82e43abdaa9759c22218477){:target="_blank"})
* Add master `LostFile` and lost blocks metric ([2238a64b](https://github.com/Alluxio/alluxio/commit/2238a64b5c42e9819c6235b654cf2a2cb455b4d7){:target="_blank"})
* Add metric of JVM pause monitor ([ef4aaab2](https://github.com/Alluxio/alluxio/commit/ef4aaab265c8d309f3a67e9d946bd9fa5cc72e05){:target="_blank"})
* Add metric of Operating System ([67e568ff4](https://github.com/Alluxio/alluxio/commit/67e568ff4cc773b001d564200f4d8e968af5fb2d){:target="_blank"})
* Add metadata cache metric ([e2ee953](https://github.com/Alluxio/alluxio/commit/e2ee95319076ba22eab88c04feb1dd8c39aa0666){:target="_blank"})
* Register journal sequence number metric ([449d1ae9](https://github.com/Alluxio/alluxio/commit/449d1ae93fe3d9e22cb9aad30fba277c8db8ba85){:target="_blank"})
* Support total block replica count metric ([13ec038b](https://github.com/Alluxio/alluxio/commit/13ec038b5a1e1feee05449cb759e7f9a08444ffb){:target="_blank"})
* Add metrics to track master RPC throughput ([b2a40192](https://github.com/Alluxio/alluxio/commit/b2a4019276980c456a76d1692b77aee61820b0d4){:target="_blank"})

## Improvements Since 2.6.1
* Improve documentation surrounding worker tiered stores ([c93e61e](https://github.com/Alluxio/alluxio/commit/c93e61e3c4c432a4f23dd9171725e5e2194e7bea){:target="_blank"})
* Avoid redundant query for conf address ([6012721](https://github.com/Alluxio/alluxio/commit/601272157bf8558c2e9bf0315ba10a3815e585b6){:target="_blank"})
* Add container host information on worker page ([e5e53e08](https://github.com/Alluxio/alluxio/commit/e5e53e08600764843a78a0b4d9ea2bc4f5110471){:target="_blank"})
* Release `workerInfoList` when a job completes ([a0c3c6a4](https://github.com/Alluxio/alluxio/commit/a0c3c6a49a3940b51411219ab3a5f3da025a2556){:target="_blank"})
* Support web server for Fuse process ([83c16f67c](https://github.com/Alluxio/alluxio/commit/83c16f67c04c854ba731a638be989154d9885925){:target="_blank"})
* Update system tuning docs ([735973](https://github.com/Alluxio/alluxio/commit/73597322ee4b19e5b83d51cedc4162ecf9ef236e){:target="_blank"})
* Make unmount Fuse properly ([2df83726](https://github.com/Alluxio/alluxio/commit/2df837261962fccd2c5816d7821c6341ef23f81d){:target="_blank"})
* Provide entry points for providing java-based TLS security to gRPC channels ([ea49f3b31](https://github.com/Alluxio/alluxio/commit/ea49f3b31da42de16c6777573c31a2c4b7e9b73b){:target="_blank"})
* Count the number of successful and failed jobs in distributed job commands ([2c792f987](https://github.com/Alluxio/alluxio/commit/2c792f987193cfa89d4c3798c898e942b7b85185){:target="_blank"})
* Allow Probes to configure in Helm Chart ([4991e84](https://github.com/Alluxio/alluxio/commit/4991e849fe6b14e32aa9ec4a119c5919272ada9f){:target="_blank"})
* Support list a specific status of job ([fdf9d4f4](https://github.com/Alluxio/alluxio/commit/fdf9d4f408dc93f7ddcbf2f85cffed85e7d7951b){:target="_blank"})
* Add docs on Presto and Iceberg ([2a56d12](https://github.com/Alluxio/alluxio/commit/2a56d12759e4e1eada57ea87525cfb0946569b42){:target="_blank"})
* Reduce the risk of sensitive information leak in RPC debug/error log ([ea00090](https://github.com/Alluxio/alluxio/commit/ea000903cf998b491e86d1a89463e5f681d0b339){:target="_blank"})
* Add configuration of min and max election timeout ([b26d200ca](https://github.com/Alluxio/alluxio/commit/b26d200ca1500a67d184842bb30e05af25c6c949){:target="_blank"})
* Support Fuse on Worker process in Kubernetes helm yaml files ([d2e947243](https://github.com/Alluxio/alluxio/commit/d2e9472438057146216374b46ece361c9acd4723){:target="_blank"})
* Create smaller alpine and centos development docker image ([22ecb2c2](https://github.com/Alluxio/alluxio/commit/22ecb2c2aa7764191588720945571934d410c568){:target="_blank"})
* Add property to skip listing broken symlinks on local UFS ([b5f318e7a](https://github.com/Alluxio/alluxio/commit/b5f318e7ac1cfb3effc73c285f500de29c10acdb){:target="_blank"})
* Update `evictor(LRU)` reference when get a page in `LocalCacheManager` ([c9e396a3](https://github.com/Alluxio/alluxio/commit/c9e396a3ca8945aad80a1cc026858972755675e0){:target="_blank"})
* Close gRPC input stream when finished reading to speed up data loading in ML/DL workloads ([4f7a8877](){:target="_blank"})

## Bugfixes Since 2.6.1
* Fix the button of logs tab page cannot work issue ([ffbb7395](https://github.com/Alluxio/alluxio/commit/ffbb7395b8382db56eb69b82ad227506d8290d88){:target="_blank"})
* Fix process local read write client side logics and add unit tests ([87e08e2](https://github.com/Alluxio/alluxio/commit/87e08e27f8e875bd57f2b3a01502beafc50058b5){:target="_blank"})
* Stop leaking state-lock when journal is closed ([ef2d38f6](https://github.com/Alluxio/alluxio/commit/fef2d38f6f27ee9fb02f035c383ef9db74f7fcc8){:target="_blank"})
* Fix `ArrayIndexOutOfBoundsException` when using `shared.caching.reader` ([f1f49e5ea](https://github.com/Alluxio/alluxio/commit/f1f49e5eadd1b764d255645d8a99d20c0c1c9f87){:target="_blank"})
* Fix the job server or job worker starts failed ([3f5b76da](https://github.com/Alluxio/alluxio/commit/3f5b76da926fe3ef76e5f5b7e78a4dd2d8eb091a){:target="_blank"})
* Fix job completion logging ([80cf7ca](https://github.com/Alluxio/alluxio/commit/80cf7ca85c9cc5fb198edd0c21af74d90610e533){:target="_blank"})
* Fix block count metrics ([edb5169](https://github.com/Alluxio/alluxio/commit/edb51690fc3f9f7b0f768e0205fb4036c3bec79c){:target="_blank"})
* Fix race condition in `StressMasterBench` ([50a4738](https://github.com/Alluxio/alluxio/commit/50a473819893349d09cafe62f0bdd8f84819219e){:target="_blank"})
* Fix last snapshot index in delegated backup ([15c0838a](https://github.com/Alluxio/alluxio/commit/15c0838aeb1eb1236223ec0d59854e3e97acc24d){:target="_blank"})
* Make quorum info command more expressive ([8704ea1](https://github.com/Alluxio/alluxio/commit/8704ea1481c62abfdc6e3e66d89feb2709ceda9e){:target="_blank"})
* Handle some exceptional cases to prevent leaks ([bd2f945e3](https://github.com/Alluxio/alluxio/commit/bd2f945e3e9b5da0b3c2cd469fc25f2adc190687){:target="_blank"})
* Remove `ramfs` from size-checking condition ([257da58](https://github.com/Alluxio/alluxio/commit/257da5887ffe441bbd4e742765a5f229b556d5e8){:target="_blank"})
* Make the stopwatch thread-safe in `readInternal` ([8e03d6d1c](https://github.com/Alluxio/alluxio/commit/8e03d6d1c7a7cbfa1e45cfd0afddaf86a9b307ea){:target="_blank"})
* Fix the job server service hangs on when setting a no privileged path ([6a0c01d](https://github.com/Alluxio/alluxio/commit/6a0c01d85971a5c294b343151702761fba71075b){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.6.2 release. Especially, we would like to thank:

Curt Hu ([BlueStalker](https://github.com/BlueStalker){:target="_blank"}),
Peter Roelants ([horasal](https://github.com/horasal){:target="_blank"}),
Nan Lu ([lunan517](https://github.com/lunan517){:target="_blank"}),
Nirav Chotai ([nirav-chotai](https://github.com/nirav-chotai){:target="_blank"}),
Yaolong Liu ([codings-dan](https://github.com/codings-dan){:target="_blank"}),
Bing Zheng ([bzheng888](https://github.com/bzheng888){:target="_blank"}),
Chenliang Lu ([yabola](https://github.com/yabola){:target="_blank"}), 
kqhzz ([kuszz](https://github.com/kuszz){:target="_blank"}),
Jieliang Li ([ljl1988com](https://github.com/ljl1988com){:target="_blank"}), 
Tom Lee ([tomscut](https://github.com/tomscut){:target="_blank"}),
Baolong Mao ([maobaolong](https://github.com/maobaolong){:target="_blank"}),
and Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"}).

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).