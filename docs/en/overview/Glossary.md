---
layout: global
title: Glossary
group: Overview
priority: 1
---

<!-- * Table of Contents
{:toc} -->

Glossary of Alluxio terminology

<!-- what words should be included in glossary? best if can point to more detailed doc-->
<!-- this is just an example, definitions taken from https://docs.alluxio.io/os/user/stable/en/overview/Architecture.html -->

* Table of Contents
{:toc}

### Master

The Alluxio Master serves all user requests and journals file system metadata changes. The Alluxio Job Master is the process which serves as a lightweight scheduler for file system operations which are then executed on Alluxio Job Workers.

The Alluxio Master can be deployed as one leading master and several standby masters for fault tolerance. When the leading master goes down, a standby master is elected to become the new leading master.

* Leading Master
: Only one master process can be the leading master in an Alluxio cluster. The leading master is responsible for managing the global metadata of the system. This includes file system metadata (e.g. the file system inode tree), block metadata (e.g. block locations), and worker capacity metadata (free and used space). The leading master will only ever query under storage for metadata. Application data will never be routed through the master. Alluxio clients interact with the leading master to read or modify this metadata. All workers periodically send heartbeat information to the leading master to maintain their participation in the cluster. The leading master does not initiate communication with other components; it only responds to requests via RPC services. The leading master records all file system transactions to a distributed persistent storage location to allow for recovery of master state information; the set of records is referred to as the journal.

* Standby Master
: Standby masters are launched on different servers to provide fault-tolerance when running Alluxio in HA mode. Standby masters read journals written by the leading master to keep their own copies of the master state up-to-date. They also write journal checkpoints for faster recovery in the future. However, they do not process any requests from other Alluxio components. After leading master fail-over, the standby masters will re-elect the new leading master after.

* Secondary Master (for UFS Journal)
: When running a single Alluxio master without HA mode with UFS journal, one can start a secondary master on the same server as the leading master to write journal checkpoints. Note that, the secondary master is not designed to provide high availability but offload the work from the leading master for fast recovery. Different from standby masters, a secondary master can never upgrade to a leading master.

* Job Master
: The Alluxio Job Master is a standalone process which is responsible for scheduling a number of more heavyweight file system operations asynchronously within Alluxio. By separating the need for the leading Alluxio Master to execute these operations within the same process the Alluxio Master utilizes less resources and is able to serve more clients within a shorter period. Additionally, it provides an extensible framework for more complicated operations to be added in the future.
: The Alluxio Job Master accepts requests to perform the above operations and schedules the operations to be executed on Alluxio Job Workers which act as clients to the Alluxio file system. The job workers are discussed more in the following section.

<!-- ##### Leading Master

Only one master process can be the leading master in an Alluxio cluster. The leading master is responsible for managing the global metadata of the system. This includes file system metadata (e.g. the file system inode tree), block metadata (e.g. block locations), and worker capacity metadata (free and used space). The leading master will only ever query under storage for metadata. Application data will never be routed through the master. Alluxio clients interact with the leading master to read or modify this metadata. All workers periodically send heartbeat information to the leading master to maintain their participation in the cluster. The leading master does not initiate communication with other components; it only responds to requests via RPC services. The leading master records all file system transactions to a distributed persistent storage location to allow for recovery of master state information; the set of records is referred to as the journal. -->

<!-- ##### Standby Master

Standby masters are launched on different servers to provide fault-tolerance when running Alluxio in HA mode. Standby masters read journals written by the leading master to keep their own copies of the master state up-to-date. They also write journal checkpoints for faster recovery in the future. However, they do not process any requests from other Alluxio components. After leading master fail-over, the standby masters will re-elect the new leading master after. -->

<!-- ##### Secondary Master (for UFS journal)

When running a single Alluxio master without HA mode with UFS journal, one can start a secondary master on the same server as the leading master to write journal checkpoints. Note that, the secondary master is not designed to provide high availability but offload the work from the leading master for fast recovery. Different from standby masters, a secondary master can never upgrade to a leading master. -->


<!-- ##### Job Master

The Alluxio Job Master is a standalone process which is responsible for scheduling a number of more heavyweight file system operations asynchronously within Alluxio. By separating the need for the leading Alluxio Master to execute these operations within the same process the Alluxio Master utilizes less resources and is able to serve more clients within a shorter period. Additionally, it provides an extensible framework for more complicated operations to be added in the future.

The Alluxio Job Master accepts requests to perform the above operations and schedules the operations to be executed on Alluxio Job Workers which act as clients to the Alluxio file system. The job workers are discussed more in the following section. -->
<br />

### Worker

* Alluxio Worker
: Alluxio workers are responsible for managing user-configurable local resources allocated to Alluxio (e.g. memory, SSDs, HDDs). Alluxio workers store data as blocks and serve client requests that read or write data by reading or creating new blocks within their local resources. Workers are only responsible for managing blocks; the actual mapping from files to blocks is only stored by the master.

* Alluxio Job Worker
: Alluxio Job Workers are clients of the Alluxio file system. They are responsible for running tasks given to them by the Alluxio Job Master. Job Workers receive instructions to run load, persist, replicate, move, or copy operations on any given file system locations.
: Alluxio job workers don’t necessarily have to be co-located with normal workers, but it is recommended to have both on the same physical node.

<!-- ##### Alluxio Worker

Alluxio workers are responsible for managing user-configurable local resources allocated to Alluxio (e.g. memory, SSDs, HDDs). Alluxio workers store data as blocks and serve client requests that read or write data by reading or creating new blocks within their local resources. Workers are only responsible for managing blocks; the actual mapping from files to blocks is only stored by the master.

##### Alluxio Job Worker

Alluxio Job Workers are clients of the Alluxio file system. They are responsible for running tasks given to them by the Alluxio Job Master. Job Workers receive instructions to run load, persist, replicate, move, or copy operations on any given file system locations.

Alluxio job workers don’t necessarily have to be co-located with normal workers, but it is recommended to have both on the same physical node. -->
<br />

### Client

The Alluxio client provides users a gateway to interact with the Alluxio servers. It initiates communication with the leading master to carry out metadata operations and with workers to read and write data that is stored in Alluxio. Alluxio supports a native file system API in Java and bindings in multiple languages including REST, Go, and Python. Alluxio also supports APIs that are compatible with the HDFS API and the Amazon S3 API.
