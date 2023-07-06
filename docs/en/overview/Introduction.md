---
layout: global
title: Introduction
group: Overview
priority: 1
---

* Table of Contents
{:toc}

## What is Alluxio

Alluxio is world's first open source [data orchestration technology](https://www.alluxio.io/blog/data-orchestration-the-missing-piece-in-the-data-world/)
for analytics and AI for the cloud. It bridges the gap between data driven applications and storage
systems, bringing data from the storage tier closer to the data driven applications and makes it
easily accessible enabling applications to connect to numerous storage systems through a common
interface. Alluxio’s memory-first tiered architecture enables data access at speeds orders of
magnitude faster than existing solutions.

In the data ecosystem, Alluxio lies between data driven applications, such as Apache Spark, Presto, Tensorflow, Apache HBase, Apache Hive, or Apache Flink, and various persistent storage systems, such
as Amazon S3, Google Cloud Storage, OpenStack Swift, HDFS, IBM Cleversafe, EMC ECS, Ceph,
NFS, Minio, and Alibaba OSS. Alluxio unifies the data stored in these different storage systems,
presenting unified client APIs and a global namespace to its upper layer data driven applications.

The Alluxio project originated from [the UC Berkeley AMPLab](https://amplab.cs.berkeley.edu/software/) (see [papers](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html)) as
the data access layer of the Berkeley Data Analytics Stack ([BDAS](https://amplab.cs.berkeley.edu/bdas/)).
It is open source under [Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE).
Alluxio is one of the fastest growing open source projects that has attracted more than [1000 contributors](https://github.com/alluxio/alluxio/graphs/contributors) from over 300 institutions including Alibaba, Alluxio, Baidu, CMU, Google, IBM, Intel, NJU, Red Hat, Tencent, UC Berkeley, and Yahoo.

Today, Alluxio is deployed in production by [hundreds of organizations](https://www.alluxio.io/powered-by-alluxio)
with the largest deployment exceeding 1,500 nodes.

<p align="center">
<img src="https://d39kqat1wpn1o5.cloudfront.net/app/uploads/2021/07/alluxio-overview-r071521.png" width="800" alt="Ecosystem"/>
</p>
<br />

## DORA Architecture

DORA, short for Decentralized Object Repository Architecture, is the foundation of the Alluxio system. As an open-source distributed caching storage system, DORA offers low latency, high throughput, and cost savings,
while aiming to provide a unified data layer that can support various data workloads, including AI and data analytics. [Read More](/en/overview/DORA-Architecture.html)

The diagram below shows the architecture design of DORA, which consists of four major components: the service registry, scheduler, client, and worker. 

![Dora Architecture]({{ '/img/dora_architecture.png' | relativize_url }})
<br />
<br />

## Alluxio Community Slack

Join our vibrant and fast-growing [Alluxio Community Slack Channel](alluxio.io/slack) to connect with users & developers of Alluxio. If you need help running Alluxio or encounter any blockers, send your technical questions to our `#troubleshooting` channel. If you are a developer looking to contribute to Alluxio, check out the `#dev` channel.