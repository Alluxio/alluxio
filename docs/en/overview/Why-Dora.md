---
layout: global
title: Moving from Alluxio 2.x to Dora
group: Overview
priority: 2
---


## Challenges facing Alluxio 2.x 

The architecture of Alluxio 2.x is designed as a general-purpose distributed file system.
For example, Alluxio relies on a distributed system based on RAFT to store the journal for high availability.
It also serves the filesystem inode tree from a single master to implement strong consistency within the file system namespace.
The system has also been optimized for traditional sequential file reading and writing to maximize throughput for workloads like MapReduce or Spark.

In emerging use cases, the tradeoffs of these fundamental design choices are challenging the limits of the system.
Using an inode tree to store the entire namespace on a single master node results in constant throughput and resource pressure on the Alluxio master node when the namespace becomes massive in number of files.
The conventional file system journaling often becomes a performance bottleneck during when a failover event occurs to transition to a new master.
Frequent access to columnar storage introduces a heavy load of seek operations which leads to unnecessary read amplification.

## Why Do We Need A New Distributed Caching Architecture

Today, data infrastructure users and engineers face three major challenges:
maintaining agility as data scales, optimizing for cost, and ensuring data reliability.
To overcome these challenges, they are adopting Alluxio as a unified data access layer, which provides distributed cache and convenient data migration capabilities.

### Maintaining agility

To achieve agility in data infrastructure, the data access layer must be flexible, scalable, high-performing, and transparent.
An agile data access layer streamlines the modification and extension of data models, infrastructure, and pipelines,
facilitating quick responses to evolving business needs.
Moreover, it efficiently manages the surge in data volumes and the expansion of user bases.

For instance, the quantity of files utilized for AI training has surged from 100 million to 10 billion over the span of a couple years.
This rise requires a new distributed caching system that offers high performance and transparency to data users and stakeholders.
Such a system would enable efficient processing and management of large-scale data.

### Cost Optimization

With the proliferation of data, storage, and processing needs, managing the cost of maintaining and scaling data infrastructure has become increasingly difficult.

While cloud storage offers excellent scalability, the cost can be unpredictable and difficult to control.
In response to this challenge, there is a growing need for a new distributed caching system, engineered with an emphasis on cost optimization, in addition to latency and throughput.
The goal of the system design is leveraging cost-efficient technologies and optimizing resource usage so that the overall cost of data infrastructure is minimized.

### Data Reliability

Ensuring that data is available, consistent, and accurate is a fundamental assumption of any modern data infrastructure platform.
Inaccurate data can lead to incorrect insights, which can result in poor decision-making and wasted resources.

For data reliability, it is crucial to design the data infrastructure with high availability, data quality checks, and version control.
Furthermore, implementing robust data governance policies and practices is vital to guarantee data protection, along with its ethical and legal use.

Collectively, designing the data access layer with these three considerations in mind is critical for organizations to remain competitive in rapidly evolving markets.
