---
layout: global
title: Building Alluxio From Source
nickname: Building Alluxio From Source
group: Contributor Resources
priority: 0
---

* Table of Contents
{:toc}

## Build Alluxio

This guide describes how to compile Alluxio from the beginning.

The prerequisite for this guide is that you have [Java 8 or later](Java-Setup.html), [Maven 3.3.9 or later](Maven.html) installed.

### Checkout Source Code


Checkout the Alluxio master branch from Github and build the source code:

```bash
$ git clone git://github.com/alluxio/alluxio.git
$ cd alluxio
```

Optionally, you can build a particular version of Alluxio, for example {{site.ALLUXIO_RELEASED_VERSION}}.
Otherwise, this will build the master branch of the source code.

```bash
$ git checkout v{{site.ALLUXIO_RELEASED_VERSION}}
```

### Build

Build the source code using Maven:

```java
$ mvn clean install -DskipTests
```

To speed up the compilation, you can run the following instruction to skip different checks:

```bash
$ mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip
```

If you are seeing `java.lang.OutOfMemoryError: Java heap space`, please set the following
variable to increase the memory heap size for maven:

```bash
$ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```

The Maven build system fetches its dependencies, compiles source code, runs unit tests, and packages the system. If this is the first time you are building the project, it can take a while to download all the dependencies. Subsequent builds, however, will be much faster.

### Test

Once Alluxio is built, you can start it with:

```bash
$ echo "alluxio.master.hostname=localhost" > conf/alluxio-site.properties
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

To verify that Alluxio is running, you can visit [http://localhost:19999](http://localhost:19999) or check the log in the `alluxio/logs` directory. You can also run a simple program:

```bash
$ ./bin/alluxio runTests
```

You should be able to see the result `Passed the test!`

You can also stop the system by using:

```bash
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

```bash
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
