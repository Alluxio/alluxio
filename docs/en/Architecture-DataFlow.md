---
layout: global
title: Architecture and Data Flow
group: Home
priority: 2
---

* Table of Contents
{:toc}

## Architecture

### Overview

Alluxio serves as a new data access layer in the big data and machine learning ecosystem,
residing between any persistent storage systems, such as Amazon S3, Microsoft Azure
Object Store, Apache HDFS, or OpenStack Swift, and computation frameworks such as
Apache Spark, Presto, or Hadoop MapReduce. Note that Alluxio itself is not a
persistent storage system. Using Alluxio as the data access layer provides multiple benefits:

- For user applications and computation frameworks, Alluxio provides fast storage,
facilitating data sharing and locality between jobs, regardless of the computation engine used.
As a result, Alluxio can serve data at memory speed when it is local or at the speed of the
computation cluster network when data is in Alluxio. Data is only read once from the
under storage system when accessed for the first time. Data access is significantly
accelerated when access to the under storage is slow. To achieve the best performance,
Alluxio is recommended to be deployed alongside a cluster’s computation framework.

- For under storage systems, Alluxio bridges the gap between big data applications
and traditional storage systems, expanding the set of workloads available to utilize
the data. Since Alluxio hides the integration of under storage systems from applications,
any under storage can support any of the applications and frameworks running on top of Alluxio.
When mounting multiple under storage systems simultaneously, Alluxio serves as a unifying
layer for any number of varied data sources.

Alluxio can be divided into three components: masters, workers, and clients.
A typical setup consists of a single leading master, multiple standby masters,
and multiple workers. The master and worker processes constitute the Alluxio servers,
which are the components a system administrator would maintain. The clients are used to
communicate with the Alluxio servers by applications such as Spark or MapReduce jobs,
Alluxio command-line, or the FUSE layer.

<p align="center">
<img src="{{ '/img/architecture-overview.png' | relativize_url }}" alt="Architecture overview"/>
</p>

### Master

<p align="center">
<img src="{{ '/img/architecture-master.png' | relativize_url }}" alt="Alluxio master"/>
</p>

The Alluxio master service can be deployed as one leading master and several standby
masters for fault tolerance. When the leading master goes down, a standby master
is elected to become the new leading master.

#### Leading Master

Only one master process can be the leading master in an Alluxio cluster.
The leading master is responsible for managing the global metadata of the system.
This includes file system metadata (e.g. the file system inode tree), block metadata
(e.g. block locations), and worker capacity metadata (free and used space).
Alluxio clients interact with the leading master to read or modify this metadata.
All workers periodically send heartbeat information to the leading master to maintain their
participation in the cluster. The leading master does not initiate communication
with other components; it only responds to requests via RPC services.
The leading master records all file system transactions to a distributed persistent storage
to allow for recovery of master state information; the set of records is referred to as the journal.

#### Standby Masters

Standby masters read journals written by the leading master to keep their own
copies of the master state up-to-date. They also write journal checkpoints for
faster recovery in the future. They do not process any requests from other
Alluxio components.

### Worker

<p align="center">
<img src="{{ '/img/architecture-worker.png' | relativize_url }}" alt="Alluxio worker"/>
</p>

Alluxio workers are responsible for managing user-configurable local resources
allocated to Alluxio (e.g. memory, SSDs, HDDs). Alluxio workers store data
as blocks and serve client requests that read or write data by reading or
creating new blocks within their local resources. Workers are only responsible
for managing blocks; the actual mapping from files to blocks is only stored by
the master.

Workers perform data operations on the under store. This brings two important benefits:
* The data read from the under store can be stored in the worker and be
immediately available to other clients.
* The client can be lightweight and does not depend on the under storage connectors.

Because RAM usually offers limited capacity, blocks in a worker can be evicted
when space is full. Workers employ eviction policies to decide which data to
keep in the Alluxio space. For more on this topic, check out the
documentation for [Tiered Storage]({{ '/en/advanced/Alluxio-Storage-Management.html' | relativize_url }}#multiple-tier-storage).

### Client

The Alluxio client provides users a gateway to interact with the Alluxio
servers. It initiates communication with the leading master to carry out
metadata operations and with workers to read and write data that is stored in
Alluxio. Alluxio supports a native filesystem API in Java and bindings in
multiple languages including REST, Go, and Python. Alluxio also supports APIs
that are compatible with the HDFS API and the Amazon S3 API.

Note that Alluxio clients never directly access the under storage systems. 
Data is transmitted through Alluxio workers.

## Data flow

This section describes the behavior of common read and write scenarios based on
a typical Alluxio configuration as described above: Alluxio is co-located with
the compute framework and and applications and the persistent storage system is
either a remote storage cluster or cloud-based storage.

### Read

Residing between the under storage and computation framework, Alluxio serves
as a caching layer for data reads. This subsection introduces different caching
scenarios and their implications on performance.

#### Local Cache Hit

A local cache hit occurs when the requested data resides on the local Alluxio worker.
For example, if an application requests data access through the Alluxio client,
the client asks the Alluxio master for the worker location of the data.
If the data is locally available, the Alluxio client uses a “short-circuit” read
to bypass the Alluxio worker and read the file directly via the local filesystem.
Short-circuit reads avoid data transfer over a TCP socket and provide the data access
directly. Short-circuit reads are the most performant way of reading data out of Alluxio.

By default, short-circuit reads use local filesystem operations which require
permissive permissions. This is sometimes impossible when the worker and client
are containerized due to incorrect resource accounting. In cases where the default
short-circuit is not feasible, Alluxio provides domain socket based short-circuit
in which the worker transfers data to the client through a
predesignated domain socket path. For more information on this topic, please
check out the instructions on
[running Alluxio on Docker]({{ '/en/deploy/Running-Alluxio-On-Docker.html' | relativize_url }}).

Also note that Alluxio can manage other storage media (e.g. SSD, HDD) in
addition to memory, so local data access speed may vary depending on the local
storage media. To learn more about this topic, please refer to the
[tiered storage document]({{ '/en/advanced/Alluxio-Storage-Management.html' | relativize_url }}#multiple-tier-storage).

<p align="center">
<img src="{{ '/img/dataflow-local-cache-hit.gif' | relativize_url }}" alt="Data Flow of Read from a Local Worker"/>
</p>

#### Remote Cache Hit

When requested data is stored in Alluxio, but not on a client's local worker,
the client will perform a remote read from a worker that does have the data. After the client finishes
reading the data, the client instructs the local worker, if present, to create a
copy locally so that future reads of the same data can be served locally.
Remote cache hits provide network-speed data reads. Alluxio prioritizes
reading from remote workers over reading from under storage because the network
speed between Alluxio workers is typically faster than the speed between Alluxio
workers and the under storage.

<p align="center">
<img src="{{ '/img/dataflow-remote-cache-hit.gif' | relativize_url }}" alt="Data Flow of Read from a Remote Worker"/>
</p>

#### Cache Miss

If the data is not available within the Alluxio space, a cache miss occurs and
the application will have to read the data from the under storage. The Alluxio
client delegates the read from UFS to a worker, preferably a local worker.
This worker reads and caches the data from the under storage.
Cache misses generally cause the largest delay because data must be fetched from the
under storage. A cache miss is expected when reading data for the first time.

When the client reads only a portion of a block or
reads the block non-sequentially, the client will instruct the worker to cache the
full block asynchronously. This is called async caching.
Async caching does not block the client, but may still impact
performance if the network bandwidth between Alluxio and the under storage
system is a bottleneck. You can tune the impact of async caching by setting
`alluxio.worker.network.netty.async.cache.manager.threads.max` on your workers.
The default value is `8`.

<p align="center">
<img src="{{ '/img/dataflow-cache-miss.gif' | relativize_url }}" alt="Cache Miss data flow"/>
</p>

#### Cache Skip

It is possible to turn off caching in Alluxio by setting the property
[`alluxio.user.file.readtype.default`]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.user.file.cache.partially.read.block)
in the client to `NO_CACHE`.

### Write

Users can configure how data should be written by choosing from different write
types. The write type can be set either through the Alluxio API or by
configuring the property
[`alluxio.user.file.writetype.default`]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.user.file.writetype.default)
in the client. This section describes the behaviors of different write types as
well as the performance implications to the applications.

#### Write to Alluxio only (`MUST_CACHE`)

With a write type of MUST_CACHE, the Alluxio client only writes to the local
Alluxio worker and no data will be written to the under storage. During the
write, if short-circuit write is available, Alluxio client directly writes
to the file on the local RAM disk, bypassing the Alluxio worker to avoid
network transfer. Since the data is not persisted to the under storage,
data can be lost if the machine crashes or data needs to be freed up for newer writes.
The `MUST_CACHE` setting is useful for writing temporary data when data loss can be tolerated.

<p align="center">
<img src="{{ '/img/dataflow-must-cache.gif' | relativize_url }}" alt="MUST_CACHE data flow"/>
</p>

#### Write through to UFS (`CACHE_THROUGH`)

With the write type of `CACHE_THROUGH`, data is written synchronously to an
Alluxio worker and the under storage system. The Alluxio client delegates the
write to the local worker and the worker simultaneously writes to both
local memory and the under storage. Since the under storage is typically
slower to write to than the local storage, the client write speed will
match the write speed of the under storage. The `CACHE_THROUGH` write type is
recommended when data persistence is required. A local copy is also written so
any future reads of the data can be served from local memory directly.

<p align="center">
<img src="{{ '/img/dataflow-cache-through.gif' | relativize_url }}" alt="CACHE_THROUGH data flow"/>
</p>

#### Write back to UFS (`ASYNC_THROUGH`)

Alluxio provides a write type of `ASYNC_THROUGH`. With `ASYNC_THROUGH`,
data is written synchronously to an Alluxio worker and asynchronously to the
under storage system. `ASYNC_THROUGH` can provide data write at memory speed
while still persisting the data.

<p align="center">
<img src="{{ '/img/dataflow-async-through.gif' | relativize_url }}" alt="ASYNC_THROUGH data flow"/>
</p>
