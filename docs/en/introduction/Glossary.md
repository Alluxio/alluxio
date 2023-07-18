---
layout: global
title: Glossary
---

Alluxio
: Alluxio is a distributed data orchestration system that provides a unified interface to access data from various storage systems, offering improved performance and data locality. [Learn more about Alluxio]({{ '/en/introduction/Introduction.html' | relativize_url }})

Client
: The client is a component or entity tht interacts with the Alluxio system to access and manipulate data stored in Alluxio. The client is typically an application or framework that utilizes Alluxio as a distributed storage layer for efficient data access and processing. [See Client in Dora Architecture]({{ '/en/introduction/Introduction.html#dora-architecture' | relativize_url }})

Consistent Hashing
: Consistent hashing is a technique used in distributed systems to efficiently distribute data across multiple nodes in a way that minimizes data movement and disruption when nodes are added or removed from the system. [See Consistent Hashing in Dora Cache]({{ '/en/core-services/Data-Caching.html#load-balancing-between-dora-nodes' | relativize_url }})

Data Caching
: Alluxio provides an in-memory data caching mechanism to accelerate data access. Frequently accessed data is cached in memory, reducing the need for repetitive disk I/O and improving overall application performance. [Learn more]({{ '/en/core-services/Data-Caching.html' | relativize_url }})

Data Lake
: A data lake is a centralized repository that stores vast amounts of raw, unstructured, and structured data in its native format, enabling flexible and scalable data storage and analytics across various data sources and formats.

Fault Tolerance
: Alluxio incorporates fault tolerance mechanisms to ensure data availability and reliability. It replicates data across different notes in the cluster, providing resilience against node failures. [See Fault Tolerance in Dora Cache]({{ '/en/core-services/Data-Caching.html#fault-tolerance-and-client-side-ufs-fallback' | relativize_url }})

FUSE
: Filesystem in Userspace (FUSE) is a software interface for Unix and Unix-like operating systems that lets non-privileged users create their own file systems without editing kernel code. [Learn about Alluxio FUSE SDK]({{ '/en/fuse-sdk/Overview-Fuse-Sdk.html' | relativize_url }})

FUSE Kernel Cache
: Recommended for metadata cache, kernel cache is a cache mechanism employed by the operating system kernel to improve the performance of file system operations. The kernel cache stores recently accessed file system metadata in memory, reducing the need to access the underlying storage devices. [Learn more]({{ '/en/fuse-sdk/Local-Cache.html' | relativize_url }})

FUSE Userspace Cache
: Recommended for data cache, userspace cache is a cache mechanism that operates at the application or userspace level. It involves storing frequently accessed data closer to the application, typically in memory, to improve performance and reduce the need for expensive disk or network operations. Userspace cache provides a more fine-grain control on the cache (e.g. cache medium, maximum cache size, eviction policy) and the cache will not affect other applications in containerized environments unexpectedly. [Learn more]({{ '/en/fuse-sdk/Local-Cache.html' | relativize_url }})

Job
: A job is a computation or processing task performed by an application or framework that interacts with Alluxio as a data storage layer.

Job Service
: Job service helps in coordinating and monitoring the execution of jobs that involve data access or processing using Alluxio as the storage layer.

Master
: The Alluxio Master serves all user requests and journals file system metadata changes. The Alluxio Job Master is the process which serves as a lightweight scheduler for file system operations which are then executed on Alluxio Job Workers.

: The Alluxio Master can be deployed as one leading master and several standby masters for fault tolerance. When the leading master goes down, a standby master is elected to become the new leading master.

Metadata Management
: Alluxio manages metadata associated with data stored in various storage systems. It tracks metadata changes, provides metadata caching, and ensures consistency and coherence across different storage systems. [See more on Metadata Cache & Invalidation]({{ '/en/core-services/Metadata-Caching.html' | relativize_url }})

Paging Worker Storage
: Alluxio supports finer-grained page-level caching storage on Alluxio workers, as an alternative option to the existing block-based tiered caching storage. This paging storage supports general workloads including reading and writing, with customizable cache eviction policies. [Learn more]({{ '/en/core-services/Data-Caching.html#paging-worker-storage' | relativize_url}})

PrestoDB
: Presto is an open source distributed SQL query engine designed for running interactive analytic queries across large datasets. It allows you to query data from multiple sources, including relational databases, NoSQL databases, and file systems, using a SQL-like language. [Run Presto with Alluxio]({{ '/en/compute/Presto.html' | relativize_url }})

Scheduler
: The scheduler helps optimize resource utilization, task scheduling, and data locality to ensure efficient execution of tasks across a distributed computing environment. It handles all asynchronous jobs, such as preloading data to workers. [See Scheduler in Dora Architecture]({{ '/en/introduction/Introduction.html#dora-architecture' | relativize_url }})

Service Registry
: The service registry acts as a central repository that allows clients and other services to communicate with different componenets or instances of services. It is responsible for service discovery and maintains a list of workers. [See Service Registry in Dora Architecture]({{ '/en/introduction/Introduction.html#dora-architecture' | relativize_url }})

Transparent Data Access
: Alluxio allows applications to access data stored in different storage systems without modifying the application code. It provides a transparent data access layer, enabling seamless integration with existing applications and frameworks.

Trino
: Trino is a distributed SQL query engine designed for fast, interactive SQL queries across large-scale data sets, supporting a wide range of data sources and providing excellent performance and scalability. [Run Trino with Alluxio]({{ '/en/compute/Trino.html' | relativize_url }})

Unified Namespace
: Alluxio presents a unified namespace that spans multiple storage systems, creating a logical view of the data. It allows applications to interact with data consistently, regardless of where the data is physically stored.

Worker
: Worker is a component or node that activiely participates in the Alluxio system to store and manage data. Workers play a crucial role in data caching, data movement, and serving data to clients or compute frameworks [See Worker in Dora Architecture]({{ '/en/introduction/Introduction.html#dora-architecture' | relativize_url }})
