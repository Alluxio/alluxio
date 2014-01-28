---
layout: global
title: Tachyon-Master-Fault-Tolerance
---

Tachyon uses ZooKeeper to achieve master fault tolerance. It is also
required in order to use shared storage (such as HDFS) for writing logs
and images.

ZooKeeper must be set up independently (see [ZooKeeper Getting
Started](http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html))
and then in conf/tachyon-env.sh, these java options should be used:

  property name               example          represents
  --------------------------- ---------------- -------------------------------------------------------
  tachyon.usezookeeper        true             Whether or not Master processes should use ZooKeeper.
  tachyon.zookeeper.address   localhost:2181   The hostname and port ZooKeeper is running on.



