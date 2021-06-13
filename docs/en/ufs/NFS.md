---
layout: global
title: NFS
nickname: NFS
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}

This guide describes the instructions to configure [NFS](http://nfs.sourceforge.net) as Alluxio's under
storage system.

You'll need to have a configured and running installation of NFS for the rest of this guide.
If you need to get your own NFS installation up and running, we recommend taking a look at the
[NFS-HOW TO](http://nfs.sourceforge.net/nfs-howto/)

## Requirements

The prerequisite for this part is that you have a version of
[Java 8](https://adoptopenjdk.net/releases.html?variant=openjdk8&jvmVariant=hotspot)
installed.

Turn on remote login service so that `ssh localhost` can succeed. To avoid the need to
repeatedly input the password, you can add the public SSH key for the host into
`~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html) for more
details.

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

The following configuration assumes that all NFS clients are co-located with Alluxio nodes.
We also assume that all of the NFS shares are located at the same location of `/mnt/nfs`.
Given those assumptions, the following lines should be exist within the `conf/alluxio-site.properties` file.

```
alluxio.master.hostname=localhost
alluxio.master.mount.table.root.ufs=/mnt/nfs
```

## Running Alluxio with NFS

Run the following command to start Alluxio filesystem.

```console
$ ./bin/alluxio-mount.sh SudoMount
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

To verify that Alluxio is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder.

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Visit your NFS volume at `/mnt/nfs` to verify the files and directories created by Alluxio exist.
For this test, you should see files named:

```
/mnt/nfs/default_tests_files/BASIC_CACHE_THROUGH
```

Stop Alluxio by running:

```console
$ ./bin/alluxio-stop.sh local
```
