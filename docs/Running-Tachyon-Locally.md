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

Before executing Tachyon run scripts, requisite environment variables must be specified in `conf
/tachyon-env.sh`, which should be created from the included template file:

    $ cp conf/tachyon-env.sh.template conf/tachyon-env.sh

To run standalone mode, make sure that:
* `TACHYON_UNDERFS_ADDRESS` in `conf/tachyon-env.sh` is set to a tmp directory in the local filesystem (e.g., ``export TACHYON_UNDERFS_ADDRESS=/tmp``).
* Remote login service is turned on so ``ssh localhost`` will succeed.

Then, you can format Tachyon FileSystem and start it. *Note: since Tachyon needs to setup RAMfs,
starting a local system requires users to input their root password for Linux based users.*

    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh local

To verify that Tachyon is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder. You can
also run a sample program:

    $ ./bin/tachyon runTest Basic CACHE_THROUGH

For the first sample program, you should be able to see something similar to the following:

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

You can stop Tachyon any time by running:

    $ ./bin/tachyon-stop.sh
