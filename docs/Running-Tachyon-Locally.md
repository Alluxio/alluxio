---
layout: global
title: Running Tachyon Locally
nickname: Tachyon on Local Machine
group: User Guide
priority: 1
---

# Run Tachyon Standalone on a Single Machine.

The prerequisite for this part is that you have [Java](Java-Setup.html) (JDK 6 or above).

Download the binary distribution of Tachyon {{site.TACHYON_RELEASED_VERSION}}:

    $ wget https://github.com/amplab/tachyon/releases/download/v{{site.TACHYON_RELEASED_VERSION}}/tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
    $ tar xvfz tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
    $ cd tachyon-{{site.TACHYON_RELEASED_VERSION}}

Before executing Tachyon run scripts, requisite environment variables must be specified in `conf/tachyon-env.sh`, which can be copied from the included template file:

    $ cp conf/tachyon-env.sh.template conf/tachyon-env.sh

To run in standalone mode, make sure that:

* `TACHYON_UNDERFS_ADDRESS` in `conf/tachyon-env.sh` is set to a tmp directory in the local filesystem (e.g., `export TACHYON_UNDERFS_ADDRESS=/tmp`).

* Remote login service is turned on so that `ssh localhost` can succeed.

Then, you can format Tachyon FileSystem and start it. *Note: since Tachyon needs to setup [RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt), starting a local system requires users to input their root password for Linux based users.*

    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh local

To verify that Tachyon is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder. You can also run a sample program:

    $ ./bin/tachyon runTest Basic CACHE_THROUGH

For the first sample program, you should be able to see something similar to the following:

    /default_tests_files/BasicFile_CACHE_THROUGH has been removed
    2015-10-06 23:10:43,037 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with BlockMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,050 INFO   (ClientBase.java:connect) - Client registered with BlockMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,070 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with BlockMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,070 INFO   (ClientBase.java:connect) - Client registered with BlockMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,076 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with FileSystemMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,076 INFO   (ClientBase.java:connect) - Client registered with FileSystemMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,085 INFO   (BasicOperations.java:createFile) - createFile with fileId 67108863 took 11 ms.
    2015-10-06 23:10:43,109 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with FileSystemMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,109 INFO   (ClientBase.java:connect) - Client registered with FileSystemMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,175 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.1.30:29998
    2015-10-06 23:10:43,197 INFO   (FileUtils.java:createStorageDirPath) - Folder /Volumes/ramdisk/tachyonworker/5140688161268825905 was created!
    2015-10-06 23:10:43,200 INFO   (LocalBlockOutStream.java:<init>) - LocalBlockOutStream created new file block, block path: /Volumes/ramdisk/tachyonworker/5140688161268825905/50331648
    2015-10-06 23:10:43,201 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.1.30:29998
    2015-10-06 23:10:43,240 INFO   (BasicOperations.java:writeFile) - writeFile to file /default_tests_files/BasicFile_CACHE_THROUGH took 155 ms.
    2015-10-06 23:10:43,443 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with BlockMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,444 INFO   (ClientBase.java:connect) - Client registered with BlockMaster master @ localhost/127.0.0.1:19998
    2015-10-06 23:10:43,467 INFO   (BasicOperations.java:readFile) - readFile file /default_tests_files/BasicFile_CACHE_THROUGH took 227 ms.
    Passed the test!

To run a more comprehensive sanity check:

    $ ./bin/tachyon runTests

You can stop Tachyon any time by running:

    $ ./bin/tachyon-stop.sh
