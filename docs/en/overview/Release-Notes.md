---
layout: global
title: Release Notes
group: Overview
priority: 7
---

May 4, 2022

This is the first release on the Alluxio 2.8.X line. Alluxio 2.8 further enhances the S3 API functionality, job service observability, and system scalability.

* Table of Contents
{:toc}

## Highlights

### Enhanced S3 API with Metadata Tagging

Alluxio 2.8 commences a slew of ongoing improvements to the S3-compatible REST API. [Object and bucket tagging](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html){:target="_blank"} APIs have been added ([f3d2477](https://github.com/Alluxio/alluxio/commit/f3d24778d33a51d1fac302e545e975bee939ac29){:target="_blank"}) which lay the groundwork for managing end-to-end file metadata through the Alluxio S3 API.

For example the "[x-amz-tagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_Example_13){:target="_blank"}" header is now supported by the PutObject REST endpoint. You can specify user-defined tags to attach to the uploaded object through this request header as a query-parameterized string:

``` shell
curl -H "x-amz-tagging: key1=value1&key2=value2&flag" -XPUT "http://<host>:<port>/api/v1/s3/<bucket>/<object>" --data="..."
```

Object tags can be retrieved via:

```shell
curl -XGET "http://<host>:<port>/api/v1/s3/<bucket>/<object>?tagging"
```

See the [Alluxio S3 documentation]({{ '/en/api/S3-API.html' | relativize_url }}) for full details on supported tagging operations. Tags are currently limited to 10 user-defined tags per file in accordance with the [S3 specifications](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions){:target="_blank"}. 

This release aims to also further compatibility with S3 client applications through request header additions and improvements ([e913945](https://github.com/Alluxio/alluxio/commit/e91394574e3d6ef87d20d672e0eb2ea90594f721){:target="_blank"}).

### Stabilize and Scale Alluxio in Deep Training Workloads

Alluxio 2.8 improves the FUSE mount/unmount mechanism. FUSE can be mounted through the Alluxio CLI or via configuration properties ([e5f53a9](https://github.com/Alluxio/alluxio/commit/e5f53a970f1a9f4c5d9214d650490a79dfa4efae){:target="_blank"}). [FUSE unmount mechanism]({{ '/en/api/POSIX-API.html#configure-alluxio-unmount-options' | relativize_url }}) is improved to reduce the chance of leaving FUSE unmounted in the host machines ([5393b6](https://github.com/Alluxio/alluxio/commit/5393b6a60e3480a370b37872ef06871a4e7456bf){:target="_blank"}). [FUSE3]({{ '/en/api/POSIX-API.html#select-libfuse-version' | relativize_url}}) ([6f3fe6f](https://github.com/Alluxio/alluxio/commit/6f3fe6f4637261044b3938e620ac00e6e6e75708){:target="_blank"}) integration is newly supported, enabling future performance and scalability optimizations. Although FUSE2 is the default version, Alluxio will eventually phase out FUSE2 in favor of FUSE3.

Several critical issues were identified in production workloads and fixed to improve the stability of Alluxio when integrating with training workloads. Examples include a RocksDB core crash with high memory consumption ([918e73](https://github.com/Alluxio/alluxio/commit/918e739cba89b3bed54a24fde0890ba726b1164b){:target="_blank"}), FUSE segment fault error ([issue](https://github.com/Alluxio/alluxio/issues/15015){:target="_blank"}), and FUSE statfs potential OOM ([fabcf47](https://github.com/Alluxio/alluxio/commit/fabcf475478a13ac4456dd2ff2dd31be700ad4fa){:target="_blank"}).

More optimizations were added to support a large number of small files and highly concurrent access, including supporting worker registration with millions of blocks ([htf8e5e](https://github.com/Alluxio/alluxio/commit/f8e5e70d8116888e4e246be73ec13467f73142fa){:target="_blank"}) and improving the performance by 10x and reducing memory overhead when preloading a large number of small files ([bc104a9](https://github.com/Alluxio/alluxio/commit/bc104a99f02cff1850b64b45d50309ee07de4486){:target="_blank"}).

### Exception Handling for Data Movement

In Alluxio 2.8, we improved the exception handling of data movement, specifically the distributedCp and distributedMv operations. The improvements include removing 0-byte files if the task failed or was canceled ([2c83498](https://github.com/Alluxio/alluxio/commit/2c834984a33277d99016f36ef9f18a2bad4bbb8c){:target="_blank"}, [75210e0](https://github.com/Alluxio/alluxio/commit/75210e0f9477811cd778df5bfe798e1c33924049){:target="_blank"}) and preserving the destination file for overwrites until the write operation succeeds, which previously would have been deleted at the start of the job ([2fa2683](https://github.com/Alluxio/alluxio/commit/2fa2683f963fbc3cca53c4104e70753a58f7c738){:target="_blank"}).

### Asynchronous Job Service Execution

Distributed commands for load and copy can now be submitted in asynchronous mode ([cfbd06f](https://github.com/Alluxio/alluxio/commit/cfbd06ffb88e9a304fd599b95348bd0282fb004d){:target="_blank"}, [01af5b1](https://github.com/Alluxio/alluxio/commit/01af5b126e856cc69c351c0c4178230b980ca988){:target="_blank"}). The new mode can be turned on by specifying the –async flag in the [distributedCp]({{ '/en/operation/User-CLI.html#distributedcp' | relativize_url }}) and [distributedMv]({{ '/en/operation/User-CLI.html#distributedmv' | relativize_url}}) commands ([82505a5](https://github.com/Alluxio/alluxio/commit/82505a5f5b96ad48e1ad0dc0f08d989774f3d2e7){:target="_blank"}). 

This new feature simplifies the command submission and monitoring process for the end user. In previous versions, users needed to ensure their command console to remain active and not be interrupted while long-running commands were executing. After launching an asynchronous command, its progress can be reported by running the [getCmdStatus CLI command]({{ '/en/operation/User-CLI.html#getcmdstatus' | relativize_url }}). In the default sync mode, users will no longer see the progress information on files being loaded or copied as they complete. The output will now display the command JobControlId, successfully loaded or copied files, each separated on a new line, statistics of completed and failed files, and the failed file path information.

### System Stability and Scability

In 2.8, we improved the system scalability by fixing concurrency issues with RocksDB ([918e73](https://github.com/Alluxio/alluxio/commit/918e739cba89b3bed54a24fde0890ba726b1164b){:target="_blank"}) and resolving leaks ([2f2894](https://github.com/Alluxio/alluxio/commit/2f2894b22948bd77fad8e0a8b9e269c0a1fdc000){:target="_blank"}).

Multiple improvements were made to reduce the general memory usage of Alluxio master. In particular, we improved the performance of recursive deletion and reduced the memory consumption, both by roughly 20%. Based on our performance tests, we have also updated recommendations on how recursive deletion can be done more efficiently. You can find the recommendations in our [User-CLI doc]({{ '/en/operation/User-CLI.html#rm' | relativize_url }}).

### System Observability

New metrics were added in various parts of the system to provide better observability into the system state, including but not limited to, metadata sync operations ([f24c26](https://github.com/Alluxio/alluxio/commit/f24c268e910a4d1d73e36c39dae9f1dbd3c2ca3e){:target="_blank"}), data I/O exceptions ([250bb3](https://github.com/Alluxio/alluxio/commit/250bb35556dd0e347d372ca583ef70097d24e01e){:target="_blank"}), and distributed commands ([696cb8](https://github.com/Alluxio/alluxio/commit/696cb89bfc8b7dd41393e3003307947d2110e21f){:target="_blank"}).

Alluxio 2.8 also added the ability for standby masters to serve the Web UI and metrics ([a10823](https://github.com/Alluxio/alluxio/commit/a10823a9523e200f1665228cd4eb3ea7659a0d15){:target="_blank"}).

### StressBench Tool Usability

In 2.8 we improved the usability of the StressBench framework by making some parameters more user-friendly. We also added a [quick start pipeline]({{ '/en/operation/StressBench.html#batch-tasks' | relativize_url }}) to batch run multiple stress tests.

A new workflow has been added under the name MaxFile. The goal of this benchmark is to evaluate the maximum number of files an Alluxio master can handle before becoming unresponsive. The benchmark can run for many hours depending on the metastore type and heap space allocated to the Alluxio master. You can find more information on this [document]({{ '/en/operation/StressBench.html#max-file-stress-bench' | relativize_url }}).

## Improvements and Bugfixes Since 2.7.4

### CLI
* Add missing options in DistributedLoad command ([](https://github.com/Alluxio/alluxio/commit/){:target="_blank"})

### Docks, CSI, K8s
* Fix Alluxio tarball owner in docker ([46b20e997a](https://github.com/Alluxio/alluxio/commit/46b20e997a){:target="_blank"})
* Improve `entrypoint.sh` for Fuse ([f16c56f8da](https://github.com/Alluxio/alluxio/commit/f16c56f8da){:target="_blank"})
* Make java version configurable in dev image ([7fc1af3a99](https://github.com/Alluxio/alluxio/commit/7fc1af3a99){:target="_blank"})
* Unmount corrupted folder in case nodeserver restarted ([d1d0628157](https://github.com/Alluxio/alluxio/commit/d1d0628157){:target="_blank"})

### Filesystem
* Fix recursive delete not deleting children wtih valid permission ([04761a9f19](https://github.com/Alluxio/alluxio/commit/04761a9f19){:target="_blank"})
* Fix root object store UFS directory not being listed ([4ef4905056](https://github.com/Alluxio/alluxio/commit/4ef4905056){:target="_blank"})

### Journal
* Close `JournalServiceClient` asynchronously to account for async calls ([3506b4dfc7](https://github.com/Alluxio/alluxio/commit/3506b4dfc7){:target="_blank"})
* Make log compaction local in embedded journal ([662c9c9e12](https://github.com/Alluxio/alluxio/commit/662c9c9e12){:target="_blank"})
* Fix incorrect directory when mounting & formatting master journal volume ([2b83d7872f](https://github.com/Alluxio/alluxio/commit/2b83d7872f){:target="_blank"})

### Metrics
* Fix client side leak caused by Gauge ([1f54873a34](https://github.com/Alluxio/alluxio/commit/1f54873a34){:target="_blank"})
* Fix metric for UFS status cache can be negative ([0c43ea1108](https://github.com/Alluxio/alluxio/commit/0c43ea1108){:target="_blank"})
* Fix absent cache stats are always zero ([f3f308c463](https://github.com/Alluxio/alluxio/commit/f3f308c463){:target="_blank"})
* Fix metric not being documented in case of cancelled sync operation ([8e021d6a32](https://github.com/Alluxio/alluxio/commit/8e021d6a32){:target="_blank"})
* Support collecting worker metrics in `collectMetrics` command ([21560be53f](https://github.com/Alluxio/alluxio/commit/21560be53f){:target="_blank"})

### Web UI
* Update Web UI query string utility functions ([4101a3c362](https://github.com/Alluxio/alluxio/commit/4101a3c362){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.8.0 release. Especially, we would like to thank:

[Binyang2014](https://github.com/Binyang2014){:target="_blank"}, 
Junda Chen, 
Haoning Sun ([Haoning-Sun](https://github.com/Haoning-Sun){:target="_blank"}), 
[JunLuo](https://github.com/JunLuo){:target="_blank"}, 
Richard ([Sucran](https://github.com/Sucran){:target="_blank"}), 
XiChen ([xichen01](https://github.com/xichen01){:target="_blank"}), 
l-shen ([ljl1988com](https://github.com/ljl1988com){:target="_blank"}), 
Lei Qian ([qian0817](https://github.com/qian0817){:target="_blank"}), and
Xu Weize ([xwzbupt](https://github.com/xwzbupt){:target="_blank"})

We also want to especially thank [Fei Peng](https://github.com/lucaspeng12138){:target="_blank"} from the Tencent team, who provided invaluable insights with release testing.

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).