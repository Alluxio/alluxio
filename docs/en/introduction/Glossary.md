---
layout: global
title: Glossary
group: Introduction
priority: 1
---

Master
: The Alluxio Master serves all user requests and journals file system metadata changes. The Alluxio Job Master is the process which serves as a lightweight scheduler for file system operations which are then executed on Alluxio Job Workers.

: The Alluxio Master can be deployed as one leading master and several standby masters for fault tolerance. When the leading master goes down, a standby master is elected to become the new leading master.

Worker
<!-- : Alluxio workers are responsible for managing user-configurable local resources allocated to Alluxio (e.g. memory, SSDs, HDDs). Alluxio workers store data as blocks and serve client requests that read or write data by reading or creating new blocks within their local resources. Workers are only responsible for managing blocks; the actual mapping from files to blocks is only stored by the master. -->
: Worker, the most important component of DORA architecture, stores both metadata and data that are sharded by key, usually the path of the file.

Client
<!-- : The Alluxio client provides users a gateway to interact with the Alluxio servers. It initiates communication with the leading master to carry out metadata operations and with workers to read and write data that is stored in Alluxio. Alluxio supports a native file system API in Java and bindings in multiple languages including REST, Go, and Python. Alluxio also supports APIs that are compatible with the HDFS API and the Amazon S3 API. -->
: The client runs inside the applications and utilizes the same consistent hash algorithm to determine the appropriate worker for the corresponding file.

Service Registry
: The service registry is responsible for service discovery and maintains a list of workers.

Scheduler
: The scheduler handles all asynchronous jobs, such as preloading data to workers.