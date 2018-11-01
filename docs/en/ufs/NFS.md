---
layout: global
title: NFS
nickname: NFS
group: Under Stores
priority: 10
---
* Table of Contents
{:toc}

This guide describes the instructions to configure [NFS](http://nfs.sourceforge.net) as Alluxio's under
storage system.

## Initial Setup

The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

## Configuring Alluxio

Configure Alluxio to use under storage systems by modifying
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

Run the following command to start Alluxio filesystem.

```bash
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

To verify that Alluxio is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder.

Run a simple example program:

```bash
$ ./bin/alluxio runTests
```

Visit your NFS volume to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

```
/mnt/nfs/default_tests_files/Basic_CACHE_THROUGH
```

Stop Alluxio by running:

```bash
$ ./bin/alluxio-stop.sh local
```
