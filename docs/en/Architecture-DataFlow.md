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

Alluxio is not a persistent storage system. Instead, Alluxio serves as a new
data access layer in the ecosystem, residing between any persistent storage
systems (such as Amazon S3, Microsoft Azure Object Store, Apache HDFS or
OpenStack Swift) and computation frameworks (such as Apache Spark, Presto or
Hadoop MapReduce).  There are multiple benefits to have this layer in the stack:

- For user applications and computation frameworks, Alluxio can provide them
 fast storage, facilitating data sharing and locality between jobs, regardless
 of whether they are running on the same computation engine. As a result,
 Alluxio can serve the data at memory speed when data is local, or the
 computation cluster network speed when data is in Alluxio. Data is only read
 once from the under storage system on the first time it’s accessed. Therefore,
 the data access can be significantly accelerated when the access to the under
 storage is not fast. To achieve the best performance, Alluxio is recommended to
 be deployed alongside a cluster’s computation framework.

- For under storage systems, Alluxio bridges the gap between big data
  applications and traditional storage systems, and expands the set of workloads
  available to utilize the data. Since Alluxio hides the integration of under
  storage systems from applications, any under storage can back all the
  applications and frameworks running on top of Alluxio. Also, when mounting
  multiple under storage systems simultaneously, Alluxio can serve as a unifying
  layer for any number of varied data sources.

At a high level, Alluxio can be divided into three components: the master,
workers, and clients. Alluxio consists a single leader master, multiple standby masters,
and multiple workers. The masters and workers together make up the Alluxio servers, which are
the components a system admin would maintain and manage. The clients are used to
talk to Alluxio servers by the applications, such as Spark or MapReduce jobs,
Alluxio command-line, or the FUSE layer.

<p align="center">
<img src="{{site.baseurl}}{% link img/architecture-overview.png %}" alt="Architecture overview"/>
</p>

### Master

<p align="center">
<img src="{{site.baseurl}}{% link img/architecture-master.png %}" alt="Alluxio master"/>
</p>

Alluxio master service can be deployed as one leader master and several standby
masters for fault tolerance. When the leader master goes down, a standby master
is elected to become the new leader master.

#### Leader Master

There is only one leader master in an Alluxio cluster. The leader master is
responsible for managing the global metadata of the system. This includes file
system metadata (e.g. the namespace tree), block metadata (e.g block locations),
and worker capacity metadata (free and used space). Alluxio clients interact
with the leader master to read or modify this metadata. In addition, all workers
periodically send heartbeat information to the leader master to maintain their
participation in the cluster. The leader master does not initiate communication
with other components; it only responds to requests via RPC
services. Additionally, the leader master writes journals to a distributed
persistent storage to allow for recovery of master state information.

#### Standby Masters

Standby masters read journals written by the leader master to keep their own
copies of master state up-to-date. They also write journal checkpoints for
faster recovery in the future. They do not process any requests from other
Alluxio components.

### Worker

<p align="center">
<img src="{{site.baseurl}}{% link img/architecture-worker.png %}" alt="Alluxio worker"/>
</p>

Alluxio workers are responsible for managing user-configurable local resources
allocated to Alluxio (e.g. memory, SSDs, HDDs etc.). Alluxio workers store data
as blocks and serve client requests that read or write data by reading or
creating new blocks within their local resources. Workers are only responsible
for managing blocks; the actual mapping from files to blocks is only stored by
the master.

Also, Alluxio workers perform data operations on the under store (e.g. data
transfer to under stores). This brings two important
benefits: The data read from the under store can be stored in the worker and be
available immediately to other clients, and the client can be lightweight and
does not depend on the under storage connectors.

Because RAM usually offers limited capacity, blocks in a worker can be evicted
when space is full. Workers employ eviction policies to decide which data to
keep in the Alluxio space. For more on this topic, please check out the
documentation for [Tiered Storage]({{ site.baseurl }}{% link en/advanced/Alluxio-Storage-Management.md %}#multiple-tier-storage).

### Client

The Alluxio client provides users a gateway to interact with the Alluxio
servers. It initiates communication with the leader master to carry out
metadata operations and with workers to read and write data that is stored in
Alluxio. Alluxio supports a native filesystem API in Java, and bindings in
multiple languages including REST, Go and Python. In addition to that, Alluxio
also supports APIs that are compatible with HDFS API as well as Amazon S3 API.

Note that, Alluxio clients do not directly access the under stores. Instead
they read or write data through Alluxio workers.

## Data flow

This section describes the behavior of common read and write scenarios based on
a typical Alluxio configuration as described above: Alluxio is colocated with
the compute framework and and applications and the persistent storage system is
either a remote storage cluster or cloud-based storage.

### Read

Sitting between the under storage and computation framework, Alluxio can serve
as a caching layer for data reads. This subsection introduces different caching
scenarios and their implications on performance.

#### Local Cache Hit

This occurs when the requested data resides on the local Alluxio worker and the
computation gets a local cache hit. For example, if an application requests data
access through the Alluxio client, the client checks with the Alluxio master for
the worker location of the data. If the data is locally available, the Alluxio
client will use a “short-circuit” read to bypass the Alluxio worker and read the
file directly via the local filesystem. Short-circuit reads avoid data transfer
over a TCP socket, and provide the data access at memory speed. Short-circuit
reads are the most performant way of reading data out of Alluxio.

By default, short-circuit reads use local filesystem operations which require
permissive permissions. This is sometimes impossible when the worker and client
are dockerized due to incorrect resource accounting. In cases where the default
short circuit is not feasible, Alluxio provides domain socket based short
circuit in which the worker will transfer data to the client through a
predesignated domain socket path. For more information on this topic, please
check out the instructions on
[running Alluxio on Docker]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-On-Docker.md %}).

Also note that Alluxio can manage other storage media (e.g. SSD, HDD) in
addition to memory, so local data access speed may vary depending on the local
storage media. To learn more about this topic, please refer to the
[tiered storage document]({{ site.baseurl }}{% link en/advanced/Alluxio-Storage-Management.md %}#multiple-tier-storage).

<p align="center">
<img src="{{site.baseurl}}{% link img/dataflow-local-cache-hit.gif %}" alt="Data Flow of Read from a Local Worker"/>
</p>

#### Remote Cache Hit

When the Alluxio client finds the data is not available in a local Alluxio
worker but resides in another remote Alluxio worker, it will read from the
remote worker. After the client finish reading the data, the client will
instruct the local worker (if it presents) to also create a
copy locally so that future reads of the same data can be served locally from
memory. Remote cache hits provide network-speed data reads. Alluxio prioritizes
reading from remote workers over reading from under storage because the network
speed between Alluxio workers is typically faster than the speed between Alluxio
workers and the under storage.

<p align="center">
<img src="{{site.baseurl}}{% link img/dataflow-remote-cache-hit.gif %}" alt="Data Flow of Read from a Remote Worker"/>
</p>

#### Cache Misses

If the data is not available within the Alluxio space, a cache miss occurs and
the application will have to read the data from the under storage. The Alluxio
client will delegate the read from UFS to a worker (a local worker is
preferred).  This worker will read and cache the data from the under
storage. Cache misses generally cause the largest delay because fetching the
data from the under storage in general slower. A cache miss typically happens
when the data is read the first time.

Note that, when Alluxio client reads only a portion of the entire block or
non-sequentially (e.g., running SQL queries on files of ORC and Parquet
formats), the client will read data as normal and signal to the worker which
blocks it reads asynchronously.  In other words, the caching process is
completely transparent to the client and the worker will fetch these blocks from
the under storage asynchronously. Duplicate requests caused by concurrent
readers will be consolidated on the worker and result in caching the block
once. Partial caching is not on the critical path, but may still impact
performance if the network bandwidth between Alluxio and the under storage
system is a bottleneck.

<p align="center">
<img src="{{site.baseurl}}{% link img/dataflow-cache-miss.gif %}" alt="Cache Miss data flow"/>
</p>

#### Cache Skipped

It’s possible to turn off caching in Alluxio and have the client read directly
from the under storage by setting the property
[`alluxio.user.file.readtype.default`]({{ site.baseurl }}{% link en/reference/Properties-List.md %}#alluxio.user.file.cache.partially.read.block)
in the client to `NO_CACHE`.

### Write

Users can configure how data should be written by choosing from different write
types. The write type can be set either through the Alluxio API or by
configuring the property
[`alluxio.user.file.writetype.default`]({{ site.baseurl }}{% link en/reference/Properties-List.md %}#alluxio.user.file.writetype.default)
in the client. This section describes the behaviors of different write types as
well as the performance implications to the applications.

#### Write to Alluxio only (`MUST_CACHE`)

With a write type of MUST_CACHE, the Alluxio client only writes to the local
Alluxio worker, and no data will be written to the under storage. Before the
write, Alluxio client will create the metadata on the Alluxio master. During the
write, if short-circuit write is available, Alluxio client will directly write
to the file on the local RAM disk, bypassing the Alluxio worker to avoid the
slower network transfer. Short-circuit write is the most performant write (it
executes at memory speed). Since the data is not written persistently to the
under storage, data can be lost if the machine crashes or data needs to be freed
up for newer writes. As a result, the `MUST_CACHE` setting is useful for writing
temporary data when data loss can be tolerated.

<p align="center">
<img src="{{site.baseurl}}{% link img/dataflow-must-cache.gif %}" alt="MUST_CACHE data flow"/>
</p>

#### Write through to UFS (`CACHE_THROUGH`)

With the write type of `CACHE_THROUGH`, data is written synchronously to an
Alluxio worker and the under storage system. The Alluxio client delegates the
write to the local worker, and the worker will simultaneously write to both the
local memory as well as the under storage. Since the under storage is typically
much slower to write to than the local storage, the client write speed will
match the write speed of the under storage. The `CACHE_THROUGH` write type is
recommended when data persistence is required. A local copy is also written, so
any future reads of the data can be served from local memory directly.

<p align="center">
<img src="{{site.baseurl}}{% link img/dataflow-cache-through.gif %}" alt="CACHE_THROUGH data flow"/>
</p>

#### Write back to UFS (`ASYNC_THROUGH`)

Lastly, Alluxio provides a write type of `ASYNC_THROUGH`. With `ASYNC_THROUGH`,
data is written synchronously to an Alluxio worker and asynchronously to the
under storage system. `ASYNC_THROUGH` can provide data write at memory speed
while still persisting the data.

<p align="center">
<img src="{{site.baseurl}}{% link img/dataflow-async-through.gif %}" alt="ASYNC_THROUGH data flow"/>
</p>
