---
layout: global
title: Running Tachyon on a Cluster
---

The easiest way to get up and running with a tachyon cluster is to use the
[Spark EC2 Tool](https://spark.incubator.apache.org/docs/latest/ec2-scripts.html). If you are
setting up a cluster manually, most of the steps should be the same. This section assumes you are
using Spark 0.9.0.

To get started, start an EC2 cluster using `spark-ec2`. Next download Tachyon onto the master node
in the cluster.

    $ wget http://tachyon-project.org/downloads/tachyon-0.4.0-bin.tar.gz
    $ tar xvfz tachyon-0.4.0-bin.tar.gz

In the `tachyon-0.4.0/conf` directory, copy `tachyon-env.sh.template` to `tachyon-env.sh`. Make sure
`JAVA_HOME` points to a valid Java 7 installation. `spark-ec2` should have created a `tachyon/conf`
directory in the home directory. Copy the `TACHYON_MASTER_ADDRESS`, `TACHYON_UNDERFS_ADDRESS`, and
`TACHYON_WORKER_MEMORY_SIZE` environment variable settings from the `~/tachyon/conf/tachyon-env.sh`
file to the `tachyon-env.sh` file in your downloaded `tachyon-0.4.0/conf` directory.

Copy the `slaves` file from `~/tachyon/conf/` to the downloaded `tachyon-0.4.0/conf/` directory.
Make sure to add the master address as a slave in the file. Copy the entire `tachyon-0.4.0`
directory to the other nodes in the cluster, making sure it's in the same location as the directory
on the master node.

Now, you can start Tachyon:

    $ cd tachyon
    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh all Mount

To verify that Tachyon is running, you can visit
[http://tachyon.master.hostname:19999](http://tachyon.master.hostname:19999), check the log in the
folder tachyon/logs, or run a sample program:

    $ ./bin/tachyon runTests

**Note**: If you are using EC2, make sure the security group settings on the master node allow
 incoming connections on the tachyon web UI port.
