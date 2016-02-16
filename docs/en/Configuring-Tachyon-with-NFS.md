---
layout: global
title: Configuring Tachyon with NFS
nickname: Tachyon with NFS
group: Under Store
priority: 5
---

This guide describes how to configure Tachyon with [NFS](http://nfs.sourceforge.net) as the under storage system.

# Initial Setup

First, the Tachyon binaries must be on your machine. You can either
[compile Tachyon](Building-Tachyon-Master-Branch.html), or
[download the binaries locally](Running-Tachyon-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

{% include Configuring-Tachyon-with-NFS/copy-tachyon-env.md %}

# Configuring Tachyon

Assuming the NFS clients are co-located with Tachyon nodes, all the NFS shares are mounted at directory
`/tachyon_vol`, the following environment variable assignment needs to be added to 
`conf/tachyon-env.sh`:

{% include Configuring-Tachyon-with-NFS/underfs-address.md %}

# Running Tachyon with NFS

After everything is configured, you can start up Tachyon locally to see that everything works.

{% include Configuring-Tachyon-with-NFS/start-tachyon.md %}

This should start a Tachyon master and a Tachyon worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Configuring-Tachyon-with-NFS/runTests.md %}

After this succeeds, you can visit your NFS volume to verify the files and directories created
by Tachyon exist. For this test, you should see files named like:

{% include Configuring-Tachyon-with-NFS/nfs-file.md %}

To stop Tachyon, you can run:

{% include Configuring-Tachyon-with-NFS/stop-tachyon.md %}
