---
layout: global
title: GlusterFS
nickname: GlusterFS
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [GlusterFS](http://www.gluster.org/) as the under
storage system.

## Initial Setup

The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

## Configuring Alluxio

Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Assuming the GlusterFS bricks are co-located with Alluxio nodes, the GlusterFS volume is mounted at
`/mnt/gluster`, the following configuration parameters need to be added to
`conf/alluxio-site.properties`:

```
alluxio.master.mount.table.root.ufs=/mnt/gluster
```

## Running Alluxio Locally with GlusterFS

Start up Alluxio locally to see that everything works.

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local SudoMount
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Visit your GlusterFS volume to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

```
/mnt/gluster/default_tests_files/BASIC_CACHE_ASYNC_THROUGH
```

To stop Alluxio, you can run:

```console
$ ./bin/alluxio-stop.sh local
```

## Contributed by the Alluxio Community

GlusterFS UFS integration is contributed and maintained by the Alluxio community.
The source code is located [here](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/glusterfs).
Feel free submit pull requests to improve the integration and update 
the documentation [here](https://github.com/Alluxio/alluxio/edit/master/docs/en/ufs/GlusterFS.md) 
if any information is missing or out of date.
