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

Alternatively, we have publishhed a docker image [alluxio/alluxio-maven](https://hub.docker.com/r/alluxio/alluxio-maven) with Java, Maven and Git installed to build Alluxio source code.
For example, to build Alluxio with JDK8, checkout this image at tag `0.0.5-jdk8`:

```console
$ docker pull alluxio/alluxio-maven:0.0.5-jdk8
```

Create a container `alluxio-build` based on this image and get into this container to proceed:

```console
$ docker run -itd \
  --network=host \
  -v ${HOME}/.m2:/root/.m2 \
  --name alluxio-build \
  alluxio/alluxio-maven:0.0.5-jdk8 bash
$ docker exec -it alluxio-build bash
```

## Checkout Source Code

Checkout the Alluxio master branch from Github:

```console
$ git clone git://github.com/alluxio/alluxio.git
$ cd alluxio
```

By default, cloning the repository will check out the master branch. If you are looking to build a
particular version of the code you may check out the version using a git tag.

```console
$ git tag
$ git checkout <TAG_NAME>
```

## Build

Build the source code using Maven:

```console
$ mvn clean install -DskipTests
```

To speed up the compilation, you can run the following instruction to skip different checks:

```console
$ mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip \
  -Dcheckstyle.skip -Dlicense.skip -Dskip.protoc
```

> Note: The flag `-Dskip.protoc` skips generating source files related to gRPC proto.
You can skip this step if you have already built them.

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

Since Alluxio 1.7, Alluxio client jar built and located at
`{{site.ALLUXIO_CLIENT_JAR_PATH}}` will work with different compute frameworks
(e.g., Spark, Flink, Presto and etc) by default.

### Support of Different HDFS under storage

By default, Alluxio is built with the HDFS under storage of Hadoop 3.3.
Run the following command by specifying `<UFS_HADOOP_PROFILE>` and the corresponding `ufs.hadoop
.version` to build ufs with different versions.

```console
$ mvn install -P<UFS_HADOOP_PROFILE> -Dufs.hadoop.version=<HADOOP_VERSION> -DskipTests
```

Here `<UFS_HADOOP_VERSION>` can be set for different distributions.
Available Hadoop profiles include `ufs-hadoop-1`, `ufs-hadoop-2`, `ufs-hadoop-3` to cover the major
Hadoop versions 1.x, 2.x and 3.x.

For example
```
$ mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true \
  -Dcheckstyle.skip=true -Dfindbugs.skip=true \
  -Pufs-hadoop-1 -Dufs.hadoop.version=1.2.0
```

```
$ mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true \
  -Dcheckstyle.skip=true -Dfindbugs.skip=true \
  -Pufs-hadoop-2 -Dufs.hadoop.version=2.6.0
```

#### Apache

All main builds are from Apache so all Apache releases can be used directly

```properties
-Pufs-hadoop-1 -Dufs.hadoop.version=1.0.4
-Pufs-hadoop-1 -Dufs.hadoop.version=1.2.0
-Pufs-hadoop-2 -Dufs.hadoop.version=2.2.0
-Pufs-hadoop-2 -Dufs.hadoop.version=2.3.0
-Pufs-hadoop-2 -Dufs.hadoop.version=2.4.1
-Pufs-hadoop-2 -Dufs.hadoop.version=2.5.2
-Pufs-hadoop-2 -Dufs.hadoop.version=2.6.5
-Pufs-hadoop-2 -Dufs.hadoop.version=2.7.3
-Pufs-hadoop-2 -Dufs.hadoop.version=2.8.0
-Pufs-hadoop-2 -Dufs.hadoop.version=2.9.0
-Pufs-hadoop-3 -Dufs.hadoop.version=3.0.0
```

#### Cloudera

To build against Cloudera's releases, just use a version like `$apacheRelease-cdh$cdhRelease`

```properties
-Pufs-hadoop-2 -Dufs.hadoop.version=2.3.0-cdh5.1.0
-Pufs-hadoop-2 -Dufs.hadoop.version=2.0.0-cdh4.7.0
```

#### Hortonworks

To build against a Hortonworks release, just use a version like `$apacheRelease.$hortonworksRelease`

```properties
-Pufs-hadoop-2 -Dufs.hadoop.version=2.1.0.2.0.5.0-67
-Pufs-hadoop-2 -Dufs.hadoop.version=2.2.0.2.1.0.0-92
-Pufs-hadoop-2 -Dufs.hadoop.version=2.4.0.2.1.3.0-563
```
