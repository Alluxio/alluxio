---
layout: global
title: Introduction
group: Dora
priority: 1
---

* Table of Contents
  {:toc}

Project Dora is the next-gen architecture for Alluxio's distributed cache, designed with scalability and
performance in mind.

## Architecture Overview

The major shift in the landscape of Alluxio's metadata and cache management in Project Dora is that there is no longer a single
master node in charge of all the file system metadata and cache information. Instead, the "workers", or simply "Dora
cache nodes," now handle both the metadata and the data of the files. Clients simply send requests to Dora cache
nodes, and each Dora cache node will serve both the metadata and the data of the requested files. Since you can have a
large number of Dora cache nodes in service, client traffic does not have to go to the single master node, but rather
is distributed among the Dora cache nodes, therefore, greatly reducing the burden of the master node.

### Load balancing between Dora nodes

Without a single master node dictating clients which workers to go to fetch the data, clients need an alternative
way to select its destination. Dora employs a simple yet effective algorithm called
[consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing) to deterministically compute a target node.
Consistent hashing ensures that given a list of all available Dora cache nodes, for a particular requested file,
any client will independently choose the same node to request the file from. If the hash algorithm used is
[uniform](https://en.wikipedia.org/wiki/Hash_function#Uniformity) over the nodes, then the requests will be uniformly
distributed among the nodes (modulo the distribution of the requests).

Consistent hashing allows the Dora cluster to scale linearly with the size of the dataset, as all the metadata and data
are partitioned onto multiple nodes, without any of them being the single point of failure.

### Fault tolerance and client-side UFS fallback

A Dora cache node can sometimes run into serious trouble and stop serving requests. To make sure the clients' requests
get served normally even if a node is faulty, there's a fallback mechanism supporting clients falling back from the
faulty node to another one, and eventually if all possible options are exhausted, falling back to the UFS.

For a given file, the target node chosen by the consistent hashing algorithm is the primary node for handling the
requests regarding this file. Consistent hashing allows a client to compute a secondary node following the
primary node's failure, and redirects the requests to it. Like the primary node, the secondary node computed
by different clients independently is exactly the same, ensuring that the fallback will happen to the same node.
This fallback process can happen a few more times (configurable by the user),
until the cost of retrying multiple nodes becomes unacceptable, when the client can fall back to the UFS directly.

### Metadata management

Dora cache nodes cache the metadata of the files they are in charge of. The metadata is fetched from the UFS directly
the first time when a file is requested by a client, and cached by the Dora node. It is then used to respond to
metadata requests afterwards.

Currently, the Dora architecture is geared towards immutable and read-only use cases only. This assumes the metadata
of the files in the UFS do not change over time, so that the cached metadata do not have to be invalidated. In the
future, we'd like to explore certain use cases where invalidation of metadata is needed but is relatively rare.
