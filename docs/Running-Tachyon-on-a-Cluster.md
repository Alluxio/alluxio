---
layout: global
title: Running Tachyon on a Cluster
---

## Standalone cluster

First download the `Tachyon` tar file, and extract it.

    $ wget http://tachyon-project.org/downloads/tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
    $ tar xvfz tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz

In the `tachyon/conf` directory, copy `tachyon-env.sh.template` to `tachyon-env.sh`. Make sure
`JAVA_HOME` points to a valid Java 6/7 installation. Add the IP addresses of all the slaves to the
`tachyon/conf/slaves` file. Finally, sync all the information to worker nodes.

Now, you can start Tachyon:

    $ cd tachyon
    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh # use the right parameters here. e.g. all Mount

To verify that Tachyon is running, you can visit
[http://tachyon.master.hostname:19999](http://tachyon.master.hostname:19999), check the log in the
folder tachyon/logs, or run a sample program:

    $ ./bin/tachyon runTests

**Note**: If you are using EC2, make sure the security group settings on the master node allow
 incoming connections on the tachyon web UI port.

## Using the bootstrap-conf argument to the bin/tachyon script

The tachyon script also contains logic to create a basic config for a
cluster.  If you run:

    $ cd tachyon
    $ ./bin/tachyon bootstrap-conf <tachyon_master_hostname>

and there is no existing `tachyon/conf/tachyon-env.sh` file, then the
script will create one with the appropriate settings for a cluster
with a master node running at `<tachyon_master_hostname>`.

This script needs to be run on each node you wish to configure.

The script will configure your workers to use 2/3 of the total memory
on each worker.  This amount can be changed by editing the created
`tachyon/conf/tachyon-env.sh` file on the worker.

## EC2 cluster with Spark

If you use Spark to launch an EC2 cluster, `Tachyon` will be installed and configured by default.
