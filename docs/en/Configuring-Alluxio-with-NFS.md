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

Then, if you haven't already done so, create your configuration file with `bootstrapConf` command.
For example, if you are running Alluxio on your local machine, `<ALLUXIO_MASTER_HOSTNAME>` should be
set to `localhost` in the following command:

```bash
$ ./bin/alluxio bootstrapConf <ALLUXIO_MASTER_HOSTNAME>
```

Alternatively, you can also create the configuration file from the template and set the contents
manually.

{% include Configuring-Alluxio-with-NFS/copy-alluxio-env.md %}

## Configuring Alluxio

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
