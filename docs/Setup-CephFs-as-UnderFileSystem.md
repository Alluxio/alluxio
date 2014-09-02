---
layout: global
title: Setup CephFS as UnderFileSystem
---

Tachyon can use [CephFs](http://www.ceph.com) as its UnderFileSystem.

# Prerequisites

You need to install Ceph on your cluster. [ceph-deploy]
(http://http://ceph.com/docs/master/rados/deployment/) installs Ceph packages,
creates cluster, and adds OSDs.


# Install

Install Tachyon with CephFS: `mvn clean install -Dtest.profile=cephfs -Dhadoop.version=2.3.0 -DskipTests`.

# Test

Test Tachyon with CephFS under filesystem: `mvn test -Dtest.profile=cephfs -Dhadoop.version=2.3.0`. Note `resources/ceph/core-site.xml` provides a basic CephFS configuration that uses local host as MDS and `data` as pool name.

# Format the filesystem

    $ cd /root/tachyon/
    $ ./bin/tachyon format

