---
layout: global
title: Tachyon-Release-Notes
---

### [](#wiki-tachyon-021-release-april-26-2013)Tachyon 0.2.1 Release (April 26, 2013)

Tachyon 0.2.1 is a maintenance release that contains several bug fixes
and usability improvements.

-   In addition to Hadoop 1, Tachyon can also build with Hadoop 2.
-   Allow changing raw table's metadata.
-   Better command line support.
-   Improved test coverage: increased the number of tests from 87 to
    123.
-   Bug fixes.

* * * * *

### [](#wiki-tachyon-020-release-april-10-2013)Tachyon 0.2.0 Release (April 10, 2013)

The new version brings better API, new features, performance
improvements, enhenced UI, and stability to Tachyon. See the
documentation on the Github wiki to get started:
[https://github.com/amplab/tachyon/wiki](https://github.com/amplab/tachyon/wiki)
. Major changes are documented below:

**Java-like file API**

-   Tachyon has an API similar to JAVA file with InputStream,
    OutputStream API.
-   Efficient support for memory mapped IO.

**Native support for raw tables**

-   Table data with hundreds or more columns is common in data
    warehouses. Tachyon provides native support for multi-columned data.
-   The user can choose only to put often queried columns in memory.

**Compatibility**

-   Hadoop MapReduce and Spark can run with Tachyon without any
    modification.

**Pluggable underlayer file system**

-   Tachyon checkpoints in memory data into the underlayer file system.
    Tachyon has a generic interface to make plugging in an underlayer
    file system easy.
-   Tachyon now supports HDFS, and single node local file system.

**Enhanced UI**

-   The new web UI is more user friendly.
-   User can browse the file system easily.
-   With debug mode, admins can also view detailed information of each
    file, including locations, checkpoint path, etc.

**Command line interaction**

-   Users can use ./bin/tachyon tfs to interact with the file system.
-   It is simple to copy data in and out of the file system.

**Simpler Deployment**

-   We have significantly simplified the deployment process.
-   For example, [Running Tachyon
    Locally](Running-Tachyon-Locally.html) contains a
    guide to launch Tachyon 0.2.0 locally in \~ 5 mins.

* * * * *

### [](#wiki-tachyon-010-release-dec-22-2012)Tachyon 0.1.0 Release (Dec 22, 2012)

Tachyon v0.1.0 enables high-throughput memory sharing between different
jobs/queries and cloud computing frameworks, such as Spark and
MapReduce. This is done by leveraging something that resembles the Spark
RDD abstraction. Tachyon will cache datasets in memory, and enable
different jobs/queries and frameworks to access cached datasets at
memory speed. Thus, Tachyon avoids going to disk to load datasets that
is frequently read. It has following main use cases:

-   Different jobs/queries (Spark/Shark/StreamingSpark/Hadoop) accessing
    the same datasets will read it directly from memory, except the
    first time it is loaded from disk.
-   If a job that read a dataset crashes, the restarted job does not
    need to read the dataset from disk, but can read it at memory speed
    from Tachyon.


