---
layout: global
title: Configuring Alluxio with NFS
nickname: Alluxio with NFS
group: Under Store
priority: 5
---

This guide describes how to configure Alluxio with [NFS](http://nfs.sourceforge.net) as the under storage system.

# Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

{% include Configuring-Alluxio-with-NFS/copy-alluxio-env.md %}

# Configuring Alluxio

Assuming the NFS clients are co-located with Alluxio nodes, all the NFS shares are mounted at directory
`/alluxio_vol`, the following environment variable assignment needs to be added to 
`conf/alluxio-env.sh`:

{% include Configuring-Alluxio-with-NFS/underfs-address.md %}

# Running Alluxio with NFS

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
