---
layout: global
title: Building Alluxio Master Branch
nickname: Building Master Branch
group: Resources
---

* Table of Contents
{:toc}

This guide describes how to compile Alluxio from the beginning.

The prerequisite for this guide is that you have [Java 8 or later](Java-Setup.html), [Maven 3.3.9 or later](Maven.html), and [Thrift 0.9.3](Thrift.html) (Optional) installed.

Checkout the Alluxio master branch from Github and build the source code:

{% include Building-Alluxio-Master-Branch/checkout.md %}

If you are seeing `java.lang.OutOfMemoryError: Java heap space`, please execute:

{% include Building-Alluxio-Master-Branch/OutOfMemoryError.md %}

If you want to build a particular version of Alluxio, for example {{site.ALLUXIO_RELEASED_VERSION}}, please do `git checkout v{{site.ALLUXIO_RELEASED_VERSION}}` after `cd alluxio`.

The Maven build system fetches its dependencies, compiles source code, runs unit tests, and packages the system. If this is the first time you are building the project, it can take a while to download all the dependencies. Subsequent builds, however, will be much faster.

Once Alluxio is built, you can start it with:

{% include Common-Commands/start-alluxio.md %}

To verify that Alluxio is running, you can visit [http://localhost:19999](http://localhost:19999) or check the log in the `alluxio/logs` directory. You can also run a simple program:

{% include Common-Commands/runTests.md %}

You should be able to see results similar to the following:

{% include Building-Alluxio-Master-Branch/test-result.md %}

You can also stop the system by using:

{% include Common-Commands/stop-alluxio.md %}

## Unit Tests

To run all unit tests:

{% include Building-Alluxio-Master-Branch/unit-tests.md %}

To run all the unit tests with under storage other than local filesystem:

{% include Building-Alluxio-Master-Branch/under-storage.md %}

Currently supported values for `<under-storage-profile>` are:

{% include Building-Alluxio-Master-Branch/supported-values.md %}

To have the logs output to STDOUT, append the following to the `mvn` command

{% include Building-Alluxio-Master-Branch/STDOUT.md %}

## Compute Framework Support

Since Alluxio 1.7, the Alluxio client jar built and located at
`{{site.ALLUXIO_CLIENT_JAR_PATH}}` will work with different compute frameworks
(e.g., Spark, Flink, Presto and etc). **You do not need run Maven build with different compute
profiles.**

## Hadoop Distribution Support

To build Alluxio against one of the different distributions of hadoop, you can run the following
 command by specifying `<HADOOP_PROFILE>` and the corresponding `hadoop.version`.:

```bash
$ mvn install -P<HADOOP_PROFILE> -Dhadoop.version=<HADOOP_VERSION> -DskipTests
```
where `<HADOOP_VERSION>` can be set for different distributions.
Available Hadoop profiles include `hadoop-1`, `hadoop-2`, `hadoop-3` to cover the major Hadoop 
versions 1.x, 2.x and 3.x.

### Apache

All main builds are from Apache so all Apache releases can be used directly

```properties
-Phadoop-1 -Dhadoop.version=1.0.4
-Phadoop-1 -Dhadoop.version=1.2.0
-Phadoop-2 -Dhadoop.version=2.2.0
-Phadoop-2 -Dhadoop.version=2.3.0
-Phadoop-2 -Dhadoop.version=2.4.1
-Phadoop-2 -Dhadoop.version=2.5.2
-Phadoop-2 -Dhadoop.version=2.6.5
-Phadoop-2 -Dhadoop.version=2.7.3
-Phadoop-2 -Dhadoop.version=2.8.0
-Phadoop-2 -Dhadoop.version=2.9.0
-Phadoop-3 -Dhadoop.version=3.0.0
```

### Cloudera

To build against Cloudera's releases, just use a version like `$apacheRelease-cdh$cdhRelease`

```properties
-Phadoop-2 -Dhadoop.version=2.3.0-cdh5.1.0
-Phadoop-2 -Dhadoop.version=2.0.0-cdh4.7.0
```

### MapR

To build against a MapR release

```properties
-Phadoop-2 -Dhadoop.version=2.7.0-mapr-1607
-Phadoop-2 -Dhadoop.version=2.7.0-mapr-1602
-Phadoop-2 -Dhadoop.version=2.7.0-mapr-1506
-Phadoop-2 -Dhadoop.version=2.3.0-mapr-4.0.0-FCS
```

### Hortonworks

To build against a Hortonworks release, just use a version like `$apacheRelease.$hortonRelease`

```properties
-Phadoop-2 -Dhadoop.version=2.1.0.2.0.5.0-67
-Phadoop-2 -Dhadoop.version=2.2.0.2.1.0.0-92
-Phadoop-2 -Dhadoop.version=2.4.0.2.1.3.0-563
```

## System Settings

Sometimes you will need to play with a few system settings in order to have the unit tests pass locally.  A common setting that may need to be set is ulimit.

### Mac

In order to increase the number of files and processes allowed, run the following

{% include Building-Alluxio-Master-Branch/increase-number.md %}

It is also recommended to exclude your local clone of Alluxio from Spotlight indexing. Otherwise, your Mac may hang constantly trying to re-index the file system during the unit tests.  To do this, go to `System Preferences > Spotlight > Privacy`, click the `+` button, browse to the directory containing your local clone of Alluxio, and click `Choose` to add it to the exclusions list.
