---
layout: global
title: Building Tachyon Master Branch
nickname: Building Master Branch
group: Development
---

### toc
* [Building Master](#)
* [Unit Tests](#unit-tests)
* [Distro Support](#distro-support)
    * [Apache](#apache)
    * [Cloudera](#cloudera)
    * [MapR](#mapr)
    * [Pivotal](#pivotal)
    * [Hortonworks](#hortonworks)
* [System Settings](#system-settings)
    * [Mac](#mac)

This guide describes how to compile Tachyon from the beginning.

The prerequisite for this guide is that you have [Java 6 (or above)](Java-Setup.html),
[Maven](Maven.html), and [Thrift 0.9.2](Thrift.html) (Optional) installed.

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

    /default_tests_files/BasicFile_CACHE_THROUGH has been removed
    2015-09-06 13:27:17,358 INFO   (MasterClient.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with master @ localhost/127.0.0.1:19998
    2015-09-06 13:27:17,375 INFO   (MasterClient.java:connect) - User registered with the master @ localhost/127.0.0.1:19998; got UserId 9
    2015-09-06 13:27:17,380 INFO   (BasicOperations.java:createFile) - createFile with fileId 5 took 25 ms.
    2015-09-06 13:27:17,397 INFO   (WorkerClient.java:connect) - Trying to get local worker host : 10.239.44.23
    2015-09-06 13:27:17,405 INFO   (WorkerClient.java:connect) - Connecting local worker @ /10.239.44.23:29998
    2015-09-06 13:27:17,438 INFO   (BlockOutStream.java:get) - Writing with local stream. tachyonFile: /default_tests_files/BasicFile_CACHE_THROUGH, blockIndex: 0, opType: CACHE_THROUGH
    2015-09-06 13:27:17,451 INFO   (FileUtils.java:createStorageDirPath) - Folder /mnt/ramdisk/tachyonworker/9 was created!
    2015-09-06 13:27:17,454 INFO   (LocalBlockOutStream.java:<init>) - /mnt/ramdisk/tachyonworker/9/5368709120 was created! tachyonFile: /default_tests_files/BasicFile_CACHE_THROUGH, blockIndex: 0, blockId: 5368709120, blockCapacityByte: 536870912
    2015-09-06 13:27:17,468 INFO   (BasicOperations.java:writeFile) - writeFile to file /default_tests_files/BasicFile_CACHE_THROUGH took 87 ms.
    2015-09-06 13:27:17,470 INFO   (BlockInStream.java:get) - Reading with local stream.
    2015-09-06 13:27:17,564 INFO   (BasicOperations.java:readFile) - readFile file /default_tests_files/BasicFile_CACHE_THROUGH took 96 ms.
    Passed the test!

You can also stop the system by using:

    $ ./bin/tachyon-stop.sh

# Unit Tests

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
