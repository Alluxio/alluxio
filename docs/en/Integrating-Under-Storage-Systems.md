---
layout: global
title: Integrating Under Storage Systems
group: Resources
---

Alluxio exposes a
[UnderFileSystem](https://github.com/amplab/tachyon/blob/master/common/src/main/java/tachyon/underfs/UnderFileSystem.java)
interface to enable any under storage to integrate with Alluxio. The current available under storage
integrations can be found as submodules of the `underfs` module. To create a new module, the
following components should be implemented:

* An implementation of the UnderFileSystem interface, allowing Alluxio to communicate with the
under storage.

* A corresponding implementation of the
[UnderFileSystemFactory](https://github.com/amplab/tachyon/blob/master/common/src/main/java/tachyon/underfs/UnderFileSystemFactory.java)
interface which allows Alluxio to match path URIs to the UnderFileSystem implementation.

* A META-INF/services file to register the new integration

The [HDFS Submodule](https://github.com/amplab/tachyon/tree/master/underfs/hdfs) and
[S3 Submodule](https://github.com/amplab/tachyon/tree/master/underfs/s3) are two good examples of
how to enable a storage system to serve as Alluxio's underlying storage.
