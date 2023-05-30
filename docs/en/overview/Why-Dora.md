---
layout: global
title: Moving from Alluxio 2.X to Dora
group: Overview
priority: 2
---

* Table of Contents
{:toc}

## Moving from Alluxio 2.x to the New Generation 

From the introduction of Alluxio 2.0 in March 2019 to the recent release of 2.9.3 last month,
we the dedicated maintainers in the Alluxio open-source community, along with our users, have been persistently enriching and optimizing the system design and implementation.
We've deepened our understanding in how to empower users to use Alluxio effectively to resolve their real-world challenges.
Through this journey, we've gradually recognized that the existing architecture started facing major challenges to keep up with the emerging workloads.

In particular, the architecture of Alluxio 2.x is akin to a general-purpose distributed file system.
For example, Alluxio relies on a distributed system based on RAFT to store the journal for HA.
It also serves the filesystem inode tree from a single master to implement strong consistency within the file system namespace.
In addition, the system has also been optimized for traditional sequential file reading and writing to maximize throughput for workloads like MapReduce or Spark.

However, over the past few years, we've witnessed many new trends that are challenging a lot of these design assumptions and tradeoffs we made before:
the inherent decoupling of storage and computation in Kubernetes environments,
the replacement of traditional HDFS by object stores and cloud storage,
and the dominance of column-oriented structured data storage in analytics workloads,
and the explosive growth of model training on top of hundreds of millions of files etc.

These developments present unique opportunities as well as challenges to Alluxio based on the existing architecture in 2.x.
Specifically, using an inode tree to store the entire namespace on a single master node results in constant throughput and resource pressure on the Alluxio master node when the namespace becomes large.
The conventional file system journaling often becomes a performance bottleneck during failovers.
Additionally, frequent access to columnar storage introduces a heavy load of seek operations which, under the existing optimizations,
often leads to unnecessary read amplification, thereby creating traffic pressure.

In light of these observations, we've decided to move to a simpler, more scalable, and more modular architecture,
which enables developers to iterate optimizations faster.

## Why Do We Need A New Distributed Caching Architecture

Today, users and engineers of data infrastructure face three major challenges:
agility in iteration, cost optimization, and ensuring data reliability.
To overcome these challenges, they are adopting Alluxio as a unified data access layer, which provides distributed cache and convenient data migration capabilities.

### Agility in Iteration

To achieve agility in data infrastructure, the data access layer must be flexible, scalable, high-performing, and transparent.
An agile data access layer streamlines the modification and extension of data models, infrastructure, and pipelines, facilitating quick responses to evolving business needs.
Moreover, it efficiently manages the surge in data volumes and the expansion of user bases.

For instance, the quantity of files utilized for AI training has surged from 100 million to 10 billion over the past two to three years.
This rise requires a new distributed caching system that offers high performance and transparency to data users and stakeholders.
Such a system would enable efficient processing and management of large-scale data.

### Cost Optimization

Cost optimization is a major challenge facing data infrastructure today.
With the proliferation of data, storage, and processing needs, managing the cost of maintaining and scaling data infrastructure has become increasingly difficult.

While cloud storage offers excellent scalability, the cost can be unpredictable and difficult to control.
In response to this challenge, there's a growing need for a new distributed caching system, engineered with an emphasis on cost optimization, in addition to latency and throughput.
The goal of the system design is leveraging cost-efficient technologies and optimizing resource usage, so that the overall cost of data infrastructure is minimized while maintaining high performance and transparency.

### Data Reliability

Ensuring data reliability is also a significant challenge in modern data infrastructure.
Data reliability means that data is available, consistent, and accurate.
Inaccurate data can lead to incorrect insights, which can result in poor decision-making and wasted resources.

For data reliability, it's crucial to design the data infrastructure with high availability, data quality checks, and version control.
Furthermore, implementing robust data governance policies and practices is vital to guarantee data protection, along with its ethical and legal use.

Collectively, designing the data access layer with these three considerations in mind is critical for organizations to remain competitive in rapidly evolving markets.
