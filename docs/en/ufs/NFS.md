---
layout: global
title: NFS
---


This guide describes the instructions to configure [NFS](http://nfs.sourceforge.net){:target="_blank"} as Alluxio's under
storage system.

Network File System (NFS) is a distributed file system protocol that allows a client computer to access files over a network as if they were located on its local storage. NFS enables file sharing and remote file access between systems in a networked environment.

You'll need to have a configured and running installation of NFS for the rest of this guide.
If you need to get your own NFS installation up and running, we recommend taking a look at the
[NFS-HOW TO](http://nfs.sourceforge.net/nfs-howto/){:target="_blank"}.

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

Make sure that you have a version of
[Java 8](https://adoptopenjdk.net/releases.html?variant=openjdk8&jvmVariant=hotspot){:target="_blank"} installed.

Turn on remote login service so that `ssh localhost` can succeed. To avoid the need to
repeatedly input the password, you can add the public SSH key for the host into
`~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html){:target="_blank"} for more details.

## Creating NFS mount point

Before Alluxio master and workers can access the NFS server, mount points to the NFS server need to be created.
Typically, all the machines will have the NFS shares located at the same path, such as `/mnt/nfs`.
NFS client cache can interfere with the correct operation of Alluxio, specifically if Alluxio master creates a file on the NFS but the NFS client on the Alluxio worker continue to use the cached file listing, it will not see the newly created file. 
Thus we highly recommend setting the attribute cache timeout to 0. 
Please mount your nfs share like this. 

```shell
$ sudo mount -o actimeo=0 nfshost:/nfsdir /mnt/nfs
```

## Basic Setup

To use NFS as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Assume we have mounted NFS share at `/mnt/nfs` on all Alluxio masters and workers, modify `conf/alluxio-site.properties` to include:

```properties
alluxio.master.hostname=localhost
alluxio.dora.client.ufs.root=/mnt/nfs
```

## Running Alluxio with NFS

Once you have configured Alluxio to NFS, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

Visit your NFS volume at `/mnt/nfs` to verify the files and directories created by Alluxio exist.
For this test, you should see files named:

```
/mnt/nfs/default_tests_files/BASIC_CACHE_THROUGH
```
