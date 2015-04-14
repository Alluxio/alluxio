---
layout: global
title: Building Tachyon Master Branch
---

This guide describes how to compile Tachyon from the beginning.

The prerequisite for this guide is that you have [Java 6 (or above)](Java-Setup.html),
[Maven](Maven.html), and [Thrift 0.9.1](Thrift.html) (Optional) installed.

Checkout the Tachyon master branch from Github and package:

    $ git clone git://github.com/amplab/tachyon.git
    $ cd tachyon
    $ mvn install

If you getting java.lang.OutOfMemoryError: Java heap space, please execute 

    $ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

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

    $ mvn test [ -Dhadoop.version=x.x.x ] [ -Dtest.profile=hdfs ]

Current supported profiles:

    local #default, uses local disk
    hdfs # uses hadoop's minicluster
    glusterfs # uses glusterfs

To have the logs output to STDOUT, append the following to the `mvn` command

    -Dtest.output.redirect=false -Dtachyon.root.logger=DEBUG,CONSOLE

# Distro Support

To build master against one of the different distros of hadoop, you only need to change the `hadoop.version`.

## Apache

All main builds are from Apache so all Apache releases can be used directly

    -Dhadoop.version=2.2.0
    -Dhadoop.version=2.3.0
    -Dhadoop.version=2.4.0

## Cloudera

To build against Cloudera's releases, just use a version like `$apacheRelease-cdh$cdhRelease`

    -Dhadoop.version=2.3.0-cdh5.1.0
    -Dhadoop.version=2.0.0-cdh4.7.0

## MapR

To build against a MapR release

    -Dhadoop.version=2.3.0-mapr-4.0.0-FCS

## Pivotal

To build against a Pivotal release, just use a version like `$apacheRelease-gphd-$pivotalRelease`

    -Dhadoop.version=2.0.5-alpha-gphd-2.1.1.0
    -Dhadoop.version=2.2.0-gphd-3.0.1.0

## Hortonworks

To build against a Hortonworks release, just use a version like `$apacheRelease.$hortonRelease`

    -Dhadoop.version=2.1.0.2.0.5.0-67
    -Dhadoop.version=2.2.0.2.1.0.0-92
    -Dhadoop.version=2.4.0.2.1.3.0-563

# System Settings

Some times you will need to play with a few system settings in order to have the unit tests pass locally.  A common setting that may need to be set is ulimit.

## Mac

In order to increase the number of files and procs allowed, run the following

```bash
sudo launchctl limit maxfiles 16384 16384
sudo launchctl limit maxproc 2048 2048

sudo ulimit -n 16384
sudo ulimit -u 2048
```

It is also recommended to exclude your local clone of Tachyon from Spotlight indexing as otherwise your Mac may hang constantly trying to re-index the file system during the unit tests.  To do this go to `System Preferences > Spotlight > Privacy` and click the `+` button, browse to the folder containing your local clone of Tachyon and click `Choose` to add it to the exclusions list.
