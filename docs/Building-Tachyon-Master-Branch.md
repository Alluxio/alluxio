---
layout: global
title: Building Tachyon Master Branch
---

This guide describes how to compile Tachyon from the beginning.

The prerequisite for this guide is that you have [Java 6 (or above)](Java-Setup.html),
[Maven](Maven.html), and [Thrift 0.9 (Optional)](Thrift.html) installed.

Checkout Tachyon master branch from Github, and package:

    $ git clone git://github.com/amplab/tachyon.git
    $ cd tachyon
    $ mvn package

If you want to build a particular version of Tachyon, for example 0.4.0, please do `git checkout
v0.4.0` after `cd tachyon`.

The Maven build system fetches its dependencies, compiles, runs system's unit tests, and package the
system. If this is the first time you are building the project, it can take a while to download all
the dependencies. Subsequent builds, however, will be much faster.

Once it is built, you can start the Tachyon:

    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh local

Note: since Tachyon needs to setup RAMfs, start-local.sh will require users to input their root
password for Linux based users (Mac users do not need it). Mac users will need to create a folder
/mnt/ramdisk and set its permissions.

    $ sudo mkdir -p /mnt/ramdisk
    $ sudo chmod a+w /mnt/ramdisk

To verify that Tachyon is running, you can visit [http://localhost:19999](http://localhost:19999),
or see the log in the folder tachyon/logs. You can also run a simple program:

    $ ./bin/tachyon runTest Basic TRY_CACHE

You should be able to see results similar to the following:

    2013-04-10 20:35:06,190 INFO   (TachyonClient.java:connect) - Trying to connect master @ localhost/127.0.0.1:19998
    2013-04-10 20:35:06,223 INFO   (MasterClient.java:getUserId) - User registered at the master localhost/127.0.0.1:19998 got UserId 1
    2013-04-10 20:35:06,224 INFO   (TachyonClient.java:connect) - Trying to get local worker host : haoyuan-ThinkPad-T420s
    2013-04-10 20:35:06,235 INFO   (TachyonClient.java:connect) - Connecting local worker @ haoyuan-ThinkPad-T420s/127.0.1.1:29998
    2013-04-10 20:35:06,291 INFO   (CommonUtils.java:printTimeTakenMs) - createFile with fileId 2 took 101 ms.
    2013-04-10 20:35:06,291 INFO   (BasicOperations.java:writeFile) - Writing data...
    2013-04-10 20:35:06,292 INFO   (CommonUtils.java:printByteBuffer) - 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
    2013-04-10 20:35:06,307 INFO   (TachyonClient.java:createAndGetUserTempFolder) - Folder /mnt/ramdisk/users/1 was created!
    2013-04-10 20:35:06,309 INFO   (OutStream.java:<init>) - File /mnt/ramdisk/users/1/2 was created!
    2013-04-10 20:35:06,340 INFO   (BasicOperations.java:readFile) - Reading data...
    2013-04-10 20:35:06,360 INFO   (CommonUtils.java:printByteBuffer) - 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19

You can also stop the system by using:

    $ ./bin/tachyon-stop.sh