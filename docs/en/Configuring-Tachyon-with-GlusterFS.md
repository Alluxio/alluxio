---
layout: global
title: Configuring Tachyon with GlusterFS
nickname: Tachyon with GlusterFS
group: Under Store
priority: 2
---

This guide describes how to configure Tachyon with [GlusterFS](http://www.gluster.org/) as the under
storage system.

# Initial Setup

First, the Tachyon binaries must be on your machine. You can either
[compile Tachyon](Building-Tachyon-Master-Branch.html), or
[download the binaries locally](Running-Tachyon-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

```bash
$ cp conf/tachyon-env.sh.template conf/tachyon-env.sh
```

# Configuring Tachyon

Assuming the GlusterFS bricks are co-located with Tachyon nodes, the GlusterFS volume is mounted at
`/tachyon_vol`, the following environment variable assignment needs to be added to 
`conf/tachyon-env.sh`:

```bash
export TACHYON_UNDERFS_ADDRESS=/tachyon_vol
```

# Running Tachyon Locally with GlusterFS

After everything is configured, you can start up Tachyon locally to see that everything works.

```bash
$ ./bin/tachyon format
$ ./bin/tachyon-start.sh local
```

This should start a Tachyon master and a Tachyon worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ ./bin/tachyon runTests
```

After this succeeds, you can visit your GlusterFS volume to verify the files and directories created
by Tachyon exist. For this test, you should see files named like:

    /tachyon_vol/default_tests_files/BasicFile_STORE_SYNC_PERSIST

To stop Tachyon, you can run:

```bash
$ ./bin/tachyon-stop.sh all
```
