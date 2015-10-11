---
layout: global
title: Configuring Tachyon with GlusterFS
nickname: Tachyon with GlusterFS
group: Under Stores
priority: 4
---

This guide describes how to configure Tachyon with [Swift](http://www.gluster.org/) as the under storage system.

# Initial Setup

First, the Tachyon binaries must be on your machine. You can either [compile Tachyon](Building-Tachyon-Master-Branch.html), or [download the binaries locally](Running-Tachyon-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

    $ cp conf/tachyon-env.sh.template conf/tachyon-env.sh

# Configuring Tachyon
Assume the GlusterFS bricks are co-located with Tachyon nodes, the GlusterFS volume name is `gvol`, and the mount point is `/tachyon_vol`.

Modify `conf/tachyon-env.sh` file must be made, adding the following environment variables:

    export TACHYON_UNDERFS_ADDRESS=glusterfs://gvol
    export TACHYON_UNDERFS_GLUSTERFS_VOLUMES=gvol
    export TACHYON_UNDERFS_GLUSTERFS_MOUNTS=/tachyon_vol

# Running Tachyon Locally with GlusterFS

After everything is configured, you can start up Tachyon locally to see that everything works.

    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh local

This should start a Tachyon master and a Tachyon worker. You can see the master UI at [http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

    $ ./bin/tachyon runTest Basic CACHE_THROUGH

After this succeeds, you can visit your GlusterFS volume to verify the files and directories created by Tachyon. For this test, you should see a file named:

    /tachyon_vol/default_tests_files/BasicFile_CACHE_THROUGH

To stop Tachyon, you can run:

    $ ./bin/tachyon-stop.sh