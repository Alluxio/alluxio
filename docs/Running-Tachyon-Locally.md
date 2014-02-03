---
layout: global
title: Running Tachyon Locally
---

This guide describes:

-   Run Tachyon Standalone
-   Set HDFS under Tachyon
-   Set Amazon S3 under Tachyon

# Run Tachyon Standalone

The prerequisite for this part is that you have [Java 7](java-setup.html).

Download the binary distribution of Tachyon 0.4.0:

    $ wget http://tachyon-project.org/downloads/tachyon-0.4.0-bin.tar.gz
    $ tar xvfz tachyon-0.4.0-bin.tar.gz
    $ cd tachyon-0.4.0

Before executing Tachyon run scripts, requisite environment variables must be specified in `conf
/tachyon-env.sh`, which should be created from the included template file:

    $ cp conf/tachyon-env.sh.template conf/tachyon-env.sh

To run standalone mode, make sure that `TACHYON_UNDERFS_ADDRESS` in `conf/tachyon-env.sh` is set to
a tmp directory in the local filesystem (e.g., `export TACHYON_UNDERFS_ADDRESS=/tmp`).

Then, you can format Tachyon FileSystem and start it. *Note: since Tachyon needs to setup RAMfs,
starting a local system requires users to input their root password for Linux based users.*

    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh local

To verify that Tachyon is running, you can visit
**[http://localhost:19999](http://localhost:19999)**,
or see the log in the folder `tachyon/logs`. You can also run a sample program:

    $ ./bin/tachyon runTest Basic CACHE_THROUGH

For the first sample program, you should be able to see following results:

    /Basic_File_CACHE_THROUGH has been removed
    2014-02-02 09:32:02,760 INFO   (TachyonFS.java:connect) - Trying to connect master @ localhost/127.0.0.1:19998
    2014-02-02 09:32:02,791 INFO   (MasterClient.java:getUserId) - User registered at the master localhost/127.0.0.1:19998 got UserId 10
    2014-02-02 09:32:02,792 INFO   (TachyonFS.java:connect) - Trying to get local worker host : hy-ubuntu
    2014-02-02 09:32:02,800 INFO   (TachyonFS.java:connect) - Connecting local worker @ hy-ubuntu/127.0.1.1:29998
    2014-02-02 09:32:02,819 INFO   (CommonUtils.java:printTimeTakenMs) - createFile with fileId 18 took 60 ms.
    2014-02-02 09:32:03,194 INFO   (TachyonFS.java:createAndGetUserTempFolder) - Folder /mnt/ramdisk/tachyonworker/users/10 was created!
    2014-02-02 09:32:03,198 INFO   (BlockOutStream.java:<init>) - /mnt/ramdisk/tachyonworker/users/10/19327352832 was created!
    Passed the test!

To run a more comprehensive sanity check:

    $ ./bin/tachyon runTests

# Set HDFS as Tachyon's under filesystem

The additional prerequisite for this part is [Hadoop HDFS](http://www.michael-noll.com/tutorials
/running-hadoop-on-ubuntu-linux-multi-node-cluster/) (version 1.0.4. You can use other Hadoop
version by change pom.xml in Tachyon and recompile it.)

Edit `tachyon-env.sh` file. Setup `TACHYON_UNDERFS_ADDRESS=hdfs://HDFS_HOSTNAME:HDFS_IP`. You may
also need to setup `JAVA_HOME` in the same file.

# Set Amazon S3 as Tachyon's under filesystem

Edit `tachyon-env.sh` file. Setup `TACHYON_UNDERFS_ADDRESS=s3://s3address`. You may also need to
setup `JAVA_HOME` in the same file.