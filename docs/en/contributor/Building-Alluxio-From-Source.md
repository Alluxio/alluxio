---
layout: global
title: Building Alluxio From Source
nickname: Building Alluxio From Source
group: Contributor Resources
priority: 0
---

This guide describes how to clone the Alluxio repository, compile the source code, and run tests in your environment.

* Table of Contents
{:toc}

## Required Software

- [Java 8 installed on your system](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [Maven 3.3.9 or later](http://maven.apache.org/download.cgi)
- [Git](https://git-scm.org/downloads)

## Checkout Source Code

Checkout the Alluxio master branch from Github:

```console
$ git clone git://github.com/alluxio/alluxio.git
$ cd alluxio
```

By default, cloning the repository will check out the master branch. If you are looking to build a
particular version of the code you may check out the version using a git tag.

For example to checkout the source for version v{{site.ALLUXIO_RELEASED_VERSION}}, run:

```console
$ git checkout v{{site.ALLUXIO_RELEASED_VERSION}}
```

To view a list of all possible versions you can run

```console
$ git tag
```

## Build

Build the source code using Maven:

```java
mvn clean install -DskipTests
```

To speed up the compilation, you can run the following instruction to skip different checks:

```console
$ mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip \
  -Dcheckstyle.skip -Dlicense.skip
```

If you are seeing `java.lang.OutOfMemoryError: Java heap space`, please set the following
variable to increase the memory heap size for maven:

```console
$ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```

The Maven build system fetches its dependencies, compiles source code, runs unit tests, and packages
the system. If this is the first time you are building the project, it can take a while to download
all the dependencies. Subsequent builds, however, will be much faster.

## Test

Once Alluxio is built, you can validate and start it with:

```console
$ # Alluxio uses ./underFSStorage for under file system storage by default
$ mkdir ./underFSStorage 
$ ./bin/alluxio validateEnv local
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local SudoMount
```

To verify that Alluxio is running, you can visit [http://localhost:19999](http://localhost:19999) or
check the log in the `alluxio/logs` directory. The `worker.log` and `master.log` files will
typically be the most useful. It may take a few seconds for the web server to start. You can also
run a simple program to test that data can be read and written to Alluxio's UFS:

```console
$ ./bin/alluxio runTests
```

You should be able to see the result `Passed the test!`

You can stop the local Alluxio system by using:

```console
$ ./bin/alluxio-stop.sh local
```

## Build Options

### Compute Framework Support

Since Alluxio 1.7, **you do not need to run Maven build with different compute profiles.**
The Alluxio client jar built and located at
`{{site.ALLUXIO_CLIENT_JAR_PATH}}` will work with different compute frameworks
(e.g., Spark, Flink, Presto and etc) by default.

### Hadoop Distribution Support

To build Alluxio against one of the different distributions of hadoop, you can run the following
command by specifying `<HADOOP_PROFILE>` and the corresponding `hadoop.version`.:

```console
$ mvn install -P<HADOOP_PROFILE> -Dhadoop.version=<HADOOP_VERSION> -DskipTests
```
where `<HADOOP_VERSION>` can be set for different distributions.
Available Hadoop profiles include `hadoop-1`, `hadoop-2`, `hadoop-3` to cover the major Hadoop
versions 1.x, 2.x and 3.x.

#### Apache

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

#### Cloudera

To build against Cloudera's releases, just use a version like `$apacheRelease-cdh$cdhRelease`

```properties
-Phadoop-2 -Dhadoop.version=2.3.0-cdh5.1.0
-Phadoop-2 -Dhadoop.version=2.0.0-cdh4.7.0
```

#### MapR

To build against a MapR release

```properties
-Phadoop-2 -Dhadoop.version=2.7.0-mapr-1607
-Phadoop-2 -Dhadoop.version=2.7.0-mapr-1602
-Phadoop-2 -Dhadoop.version=2.7.0-mapr-1506
-Phadoop-2 -Dhadoop.version=2.3.0-mapr-4.0.0-FCS
```

#### Hortonworks

To build against a Hortonworks release, just use a version like `$apacheRelease.$hortonRelease`

```properties
-Phadoop-2 -Dhadoop.version=2.1.0.2.0.5.0-67
-Phadoop-2 -Dhadoop.version=2.2.0.2.1.0.0-92
-Phadoop-2 -Dhadoop.version=2.4.0.2.1.3.0-563
```
