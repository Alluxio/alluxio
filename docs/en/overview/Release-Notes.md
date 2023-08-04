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

### Exception Handling for Data Movement

### Asynchronous Job Service Execution

### System Stability and Scability

### System Observability

### StressBench Tool Usability

## Improvements and Bugfixes Since 2.7.4

### CLI

### Docks, CSI, K8s

### Filesystem

### Journal

### Metrics

### Web UI
* ([](){:target="_blank"})([](){:target="_blank"})

## Acknowledgements

We want to thank the community for their valuable contributions to the Alluxio 2.8.0 release. Especially, we would like to thank:

Binyan2014, Junda Chen, Haoning Sun, JunLuo, Richard, XiChen, l-shen, qian0817, Weize Xu

([](){:target="_blank"})

We also want to especially thank [Fei Peng](https://github.com/lucaspeng12138){:target="_blank"} from the Tencent team, who provided invaluable insights with release testing.

Enjoy the new release and look forward to hearing your feedback on our [Community Slack Channel](https://alluxio.io/slack).