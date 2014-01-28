---
layout: global
title: Running-Tachyon-Locally
---

This guide describes:

-   Run Tachyon Standalone
-   Set HDFS under Tachyon

[](#run-tachyon-standalone)Run Tachyon Standalone
-------------------------------------------------

The prerequisite for this part is that you have [Java
7](https://github.com/amplab/tachyon/wiki/Java-setup/).

Download the binary distribution of Tachyon 0.3.0:

    $ wget http://tachyon-project.org/downloads/tachyon-0.3.0-bin.tar.gz
    $ tar xvfz tachyon-0.3.0-bin.tar.gz
    $ cd tachyon-0.3.0

Before executing Tachyon run scripts, requisite environment variables
must be specified in `conf/tachyon-env.sh`, which should be created from
the included template file:

    $ cp conf/tachyon-env.sh.template conf/tachyon-env.sh

To run standalone mode, make sure that `TACHYON_UNDERFS_ADDRESS` in
`conf/tachyon-env.sh` is set to a tmp directory in the local filesystem
(e.g., `export TACHYON_UNDERFS_ADDRESS=/tmp`).

Then, you can format Tachyon FileSystem and start it. *Note: since
Tachyon needs to setup RAMfs, starting a local system requires users to
input their root password for Linux based users.*

    $ ./bin/format.sh
    $ ./bin/start.sh local

To verify that Tachyon is running, you can visit
[http://localhost:19999](http://localhost:19999), or see the log in the
folder `tachyon/logs`. You can also run a sample program:

    $ ./bin/run-test.sh Basic TRY_CACHE

For the first sample program, you should be able to see following
results:

    2013-10-22 17:14:57,805 INFO   (TachyonFS.java:connect) - Trying to connect master @ localhost/127.0.0.1:19998
    2013-10-22 17:14:57,841 INFO   (MasterClient.java:getUserId) - User registered at the master localhost/127.0.0.1:19998 got UserId 1
    2013-10-22 17:14:57,841 INFO   (TachyonFS.java:connect) - Trying to get local worker host : haoyuan-T420s
    2013-10-22 17:14:57,849 INFO   (TachyonFS.java:connect) - Connecting local worker @ haoyuan-T420s/127.0.1.1:29998
    2013-10-22 17:14:57,885 INFO   (CommonUtils.java:printTimeTakenMs) - createFile with fileId 2 took 81 ms.
    2013-10-22 17:14:57,885 INFO   (BasicOperations.java:writeFile) - Writing data...
    2013-10-22 17:14:57,886 INFO   (CommonUtils.java:printByteBuffer) - 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 
    2013-10-22 17:14:57,918 INFO   (TachyonFS.java:createAndGetUserTempFolder) - Folder /mnt/ramdisk/users/1 was created!
    2013-10-22 17:14:57,922 INFO   (BlockOutStream.java:<init>) - /mnt/ramdisk/users/1/2147483648 was created!
    2013-10-22 17:14:57,958 INFO   (BasicOperations.java:readFile) - Reading data...
    2013-10-22 17:14:57,999 INFO   (CommonUtils.java:printByteBuffer) - 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 

To run a more comprehensive sanity check:

    $ ./bin/tachyon runTests

[](#set-hdfs-under-tachyon)Set HDFS under Tachyon
-------------------------------------------------

The additional prerequisite for this part is [Hadoop
HDFS](http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-multi-node-cluster/)
(version 1.0.4. You can use other Hadoop version by change pom.xml in
Tachyon and recompile it.)

Edit `tachyon-env.sh` file. Setup
`TACHYON_UNDERFS_ADDRESS=hdfs://HDFS_HOSTNAME:HDFS_IP`. You may also
need to setup `JAVA_HOME` in the same file.

