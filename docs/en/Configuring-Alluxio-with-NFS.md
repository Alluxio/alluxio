---
layout: global
title: Configuring Alluxio with NFS
nickname: Alluxio with NFS
group: Under Store
priority: 5
---
* Table of Contents
{:toc}

This guide describes the instructions to configure [NFS](http://nfs.sourceforge.net) as Alluxio's under
storage system.

## Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-From-Source.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

## Configuring Alluxio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Assuming the NFS clients are co-located with Alluxio nodes, all the NFS shares are mounted at
directory `/mnt/nfs`, the following environment variable assignment needs to be added to
`conf/alluxio-site.properties`:

```
alluxio.master.hostname=localhost
alluxio.underfs.address=/mnt/nfs
```

## Running Alluxio with NFS

Simply run the following command to start Alluxio filesystem.

```bash
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

To verify that Alluxio is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder.

Next, you can run a simple example program:

```bash
$ ./bin/alluxio runTests
```

After this succeeds, you can visit your NFS volume to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

```
/mnt/nfs/default_tests_files/Basic_CACHE_THROUGH
```

You can stop Alluxio any time by running:

```bash
$ ./bin/alluxio-stop.sh local
```
