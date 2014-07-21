---
layout: global
title: Building Tachyon Master Branch
---

This guide describes how to compile Tachyon from the beginning.

The prerequisite for this guide is that you have [Java 6 (or above)](Java-Setup.html),
[Maven](Maven.html), and [Thrift 0.9](Thrift.html) (Optional) installed.

Checkout the Tachyon master branch from Github and package:

    $ git clone git://github.com/amplab/tachyon.git
    $ cd tachyon
    $ mvn install

If you want to build a particular version of Tachyon, for example {{site.TACHYON_RELEASED_VERSION}},
please do `git checkout v{{site.TACHYON_RELEASED_VERSION}}` after `cd tachyon`.

The Maven build system fetches its dependencies, compiles, runs system's unit tests, and package the
system. If this is the first time you are building the project, it can take a while to download all
the dependencies. Subsequent builds, however, will be much faster.

Once it is built, you can start Tachyon:

    $ cp conf/tachyon-env.sh.template conf/tachyon-env.sh
    $ ./bin/tachyon format
    $ ./bin/tachyon-start.sh local

To verify that Tachyon is running, you can visit [http://localhost:19999](http://localhost:19999) or
check the log in the folder tachyon/logs. You can also run a simple program:

    $ ./bin/tachyon runTest Basic CACHE_THROUGH

You should be able to see results similar to the following:

    /Basic_File_CACHE_THROUGH has been removed
    2014-02-02 09:32:02,760 INFO   (TachyonFS.java:connect) - Trying to connect master @ localhost/127.0.0.1:19998
    2014-02-02 09:32:02,791 INFO   (MasterClient.java:getUserId) - User registered at the master localhost/127.0.0.1:19998 got UserId 10
    2014-02-02 09:32:02,792 INFO   (TachyonFS.java:connect) - Trying to get local worker host : hy-ubuntu
    2014-02-02 09:32:02,800 INFO   (TachyonFS.java:connect) - Connecting local worker @ hy-ubuntu/127.0.1.1:29998
    2014-02-02 09:32:02,819 INFO   (CommonUtils.java:printTimeTakenMs) - createFile with fileId 18 took 60 ms.
    2014-02-02 09:32:03,194 INFO   (TachyonFS.java:createAndGetUserTempFolder) - Folder /mnt/ramdisk/tachyonworker/users/10 was created!
    2014-02-02 09:32:03,198 INFO   (BlockOutStream.java:<init>) - /mnt/ramdisk/tachyonworker/users/10/19327352832 was created!
    Passed the test!

You can also stop the system by using:

    $ ./bin/tachyon-stop.sh

# Unit tests

To run all unit tests:

    $ mvn test

To run all the unit tests with under filesystem other than local filesystem:

    $ mvn test [ -Dhadoop.version=x.x.x ] -Dintegration [ -Dufs=tachyon.LocalMiniDFSCluster ]
