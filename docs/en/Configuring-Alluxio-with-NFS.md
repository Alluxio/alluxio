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
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

## Configuring Alluxio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Assuming the NFS clients are co-located with Alluxio nodes, all the NFS shares are mounted at
directory `/mnt/nfs`, the following environment variable assignment needs to be added to
`conf/alluxio-site.properties`:

{% include Configuring-Alluxio-with-NFS/underfs-address.md %}

## Running Alluxio with NFS

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Configuring-Alluxio-with-NFS/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Configuring-Alluxio-with-NFS/runTests.md %}

After this succeeds, you can visit your NFS volume to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-NFS/nfs-file.md %}

To stop Alluxio, you can run:

{% include Configuring-Alluxio-with-NFS/stop-alluxio.md %}
