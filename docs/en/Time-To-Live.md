---
layout: global
title: Time to Live
nickname: Time to Live
group: Features
priority: 8
---

* Table of Contents
{:toc}

Alluxio supports a `Time to Live (TTL)` setting on each file and directory in the namespace. This
feature can be used to effectively manage the Alluxio cache, especially in environments with strict
guarantees on the data access patterns. For example, if analytics is only done on the last week of
ingested data, TTL can be used to explicitly flush old data to free the cache for new
files.

## Overview

Alluxio has TTL attributes associated with each file or directory. These attributes are journaled
and persist across cluster restarts. The active master node is responsible for holding the metadata
in memory when Alluxio is serving. Internally, the master runs a background thread which
periodically checks if files have reached their TTL.

Note that the background thread runs on a configurable period, by default 1 hour. This means a TTL
will not be enforced until the next check interval, and the enforcement of a TTL can be up to 1
TTL interval late. The interval length is set by the `alluxio.master.ttl.checker.interval`
property.

For example, to set the interval to 10 minutes, add the following to `alluxio-site.properties`:

```
alluxio.master.ttl.checker.interval=10m
```

Refer to the [configuration page](Configuration-Settings.html) for more details on setting Alluxio
configurations.

While the master node enforces TTLs, it is up to the clients to set the appropriate TTLs.

## APIs

There are three ways to set the TTL of a path.

1. Through the Alluxio shell command line.
1. Through the Alluxio Java File System API.
1. Passively on each load metadata or create file.

The TTL API is as follows:

```
SetTTL(path, duration, action)
`path`          the path in the Alluxio namespace
`duration`      the number of milliseconds before the TTL action goes into effect, this overrides
                any previous value
`action`        the action to take when duration has elapsed. `FREE` will cause the file to be
                evicted from Alluxio storage, regardless of the pin status. `DELETE` will cause the
                file to be deleted from the Alluxio namespace and under store.
                NOTE: `DELETE` is the default for certain commands and will cause the file to be
                permanently removed.
```

### Command Line Usage

See the detailed [command line documentation](Command-Line-Interface.html#setttl).

### Java File System API

Use the Alluxio FileSystem object to set the file attribute with the appropriate options.

```
FileSystem alluxioFs = FileSystem.Factory.get();

AlluxioURI path = new AlluxioURI("alluxio://hostname:port/file/path");
long ttlMs = 86400000L; // 1 day
TtlAction ttlAction = TtlAction.FREE; // Free the file when TTL is hit

SetAttributeOptions options = SetAttributeOptions.defaults().setTtl(ttlMs).setTtlAction(ttlAction);
alluxioFs.setAttribute(path);
```

See the [Javadocs](http://www.alluxio.org/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/index.html) for
more details.
