---
layout: global
title: Building Tachyon Master Branch
nickname: Building Master Branch
group: Dev Resources
---

* Table of Contents
{:toc}

This guide describes how to compile Tachyon from the beginning.

The prerequisite for this guide is that you have [Java 7 (or above)](Java-Setup.html),
[Maven](Maven.html), and [Thrift 0.9.2](Thrift.html) (Optional) installed.

Checkout the Tachyon master branch from Github and package:

```bash
$ git clone git://github.com/amplab/tachyon.git
$ cd tachyon
$ mvn install
```

If you are seeing `java.lang.OutOfMemoryError: Java heap space`, please execute:

```bash
$ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```

If you want to build a particular version of Tachyon, for example {{site.TACHYON_RELEASED_VERSION}},
please do `git checkout v{{site.TACHYON_RELEASED_VERSION}}` after `cd tachyon`.

The Maven build system fetches its dependencies, compiles source code, runs unit tests, and packages
the system. If this is the first time you are building the project, it can take a while to download
all the dependencies. Subsequent builds, however, will be much faster.

Once Tachyon is built, you can start it with:

```bash
$ cp conf/tachyon-env.sh.template conf/tachyon-env.sh
$ ./bin/tachyon format
$ ./bin/tachyon-start.sh local
```

To verify that Tachyon is running, you can visit [http://localhost:19999](http://localhost:19999) or
check the log in the `tachyon/logs` directory. You can also run a simple program:

```bash
$ ./bin/tachyon runTests
```

You should be able to see results similar to the following:

```bash
/default_tests_files/BasicFile_STORE_SYNC_PERSIST has been removed
2015-10-20 23:02:54,403 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,422 INFO   (ClientBase.java:connect) - Client registered with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,460 INFO   (BasicOperations.java:createFile) - createFile with fileId 1476395007 took 65 ms.
2015-10-20 23:02:54,557 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with BlockMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,558 INFO   (ClientBase.java:connect) - Client registered with BlockMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,590 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.31.242:29998
2015-10-20 23:02:54,654 INFO   (FileUtils.java:createStorageDirPath) - Folder /Volumes/ramdisk/tachyonworker/6601007274872912185 was created!
2015-10-20 23:02:54,657 INFO   (LocalBlockOutStream.java:<init>) - LocalBlockOutStream created new file block, block path: /Volumes/ramdisk/tachyonworker/6601007274872912185/1459617792
2015-10-20 23:02:54,658 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.31.242:29998
2015-10-20 23:02:54,754 INFO   (BasicOperations.java:writeFile) - writeFile to file /default_tests_files/BasicFile_STORE_SYNC_PERSIST took 294 ms.
2015-10-20 23:02:54,803 INFO   (BasicOperations.java:readFile) - readFile file /default_tests_files/BasicFile_STORE_SYNC_PERSIST took 47 ms.
Passed the test!
```

You can also stop the system by using:

```bash
$ ./bin/tachyon-stop.sh all
```

# Unit Tests

To run all unit tests:

```bash
$ mvn test
```

To run all the unit tests with under storage other than local filesystem:

```bash
$ mvn test [ -Dhadoop.version=x.x.x ] [ -P<under-storage-profile> ]
```

Currently supported values for `<under-storage-profile>` are:

```bash
Not Specified # [Default] Tests against local file system
hdfs1Test     # Tests against HDFS 1.x minicluster
hdfs2Test     # Tests against HDFS 2.x minicluster
glusterfsTest # Tests against GlusterFS
s3Test        # Tests against Amazon S3 (requires a real s3 bucket)
```

To have the logs output to STDOUT, append the following to the `mvn` command

    -Dtest.output.redirect=false -Dtachyon.root.logger=DEBUG,CONSOLE

# Distro Support

To build Tachyon against one of the different distros of hadoop, you only need to change the
`hadoop.version`.

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

Sometimes you will need to play with a few system settings in order to have the unit tests pass
locally.  A common setting that may need to be set is ulimit.

## Mac

In order to increase the number of files and processes allowed, run the following

```bash
$ sudo launchctl limit maxfiles 16384 16384
$ sudo launchctl limit maxproc 2048 2048
```

It is also recommended to exclude your local clone of Tachyon from Spotlight indexing. Otherwise,
your Mac may hang constantly trying to re-index the file system during the unit tests.  To do this,
go to `System Preferences > Spotlight > Privacy`, click the `+` button, browse to the directory
containing your local clone of Tachyon, and click `Choose` to add it to the exclusions list.
