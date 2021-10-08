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

## Creating NFS mount point

Before Alluxio master and workers can access the NFS server, mount points to the NFS server need to be created.
Typically, all the machines will have the NFS shares located at the same path, such as `/mnt/nfs`.
NFS client cache can interfere with the correct operation of Alluxio, specifically if Alluxio master creates a file on the NFS but the NFS client on the Alluxio worker continue to use the cached file listing, it will not see the newly created file. 
Thus we highly recommend setting the attribute cache timeout to 0. 
Please mount your nfs share like this. 

```console
$ sudo mount -o actimeo=0 nfshost:/nfsdir /mnt/nfs
```

## Configuring Alluxio

Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Assume we have mounted NFS share at `/mnt/nfs` on all Alluxio masters and workers, the following lines should be exist within the `conf/alluxio-site.properties` file.

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
