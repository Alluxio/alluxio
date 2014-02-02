---
layout: global
title: Running Tachyon on a Cluster
---

To run a Tachyon on a cluster, download Tachyon into a machine:

    $ wget http://tachyon-project.org/downloads/tachyon-0.4.0-bin.tar.gz
    $ tar xvfz tachyon-0.4.0-bin.tar.gz

Edit `tachyon-env.sh` file. Setup `TACHYON_UNDERFS_ADDRESS=hdfs://HDFS_HOSTNAME:HDFS_PORT` or
`TACHYON_UNDERFS_ADDRESS=s3://s3address`, and
corresponding tachyon.master.hostname

Edit `slaves` file, add slaves' hostnames into it. Sync the configuration to all nodes.

Now, you can start Tachyon:

    $ cd tachyon
    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh all Mount

To verify that Tachyon is running, you can visit
[http://tachyon.master.hostname:19999](http://tachyon.master.hostname:19999), or see the log in the
folder tachyon/logs, or run a sample program:

    $ ./bin/tachyon runTests