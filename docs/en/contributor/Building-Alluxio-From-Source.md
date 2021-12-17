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

Alternatively, we have published a docker image [alluxio/alluxio-maven](https://hub.docker.com/r/alluxio/alluxio-maven) with Java, Maven, and Git pre-installed to help build Alluxio source code.

## Checkout Source Code

Checkout the Alluxio master branch from Github:

```shell
$ git clone https://github.com/Alluxio/alluxio.git
$ cd alluxio
$ export ALLUXIO_HOME=$(pwd)
```

By default, cloning the repository will check out the master branch. If you are looking to build a
particular version of the code you may check out the version using a git tag.

```shell
$ git tag
$ git checkout <TAG_NAME>
```

## (Optional) Checkout Building Environment Using Docker

This section guides you to setup pre-configured compilation environment based on our published docker image.
You can skip this section and build Alluxio source code if JDK and Maven are already installed locally.

Start a container named `alluxio-build` based on this image and get into this container to proceed:

```shell
$ docker run -itd \
  --network=host \
  -v ${ALLUXIO_HOME}:/alluxio  \
  -v ${HOME}/.m2:/root/.m2 \
  --name alluxio-build \
  alluxio/alluxio-maven bash

$ docker exec -it -w /alluxio alluxio-build bash
```

Note that,
- Container path `/alluxio` is mapped to host path `${ALLUXIO_HOME}`, so the binary built will still be accessible outside the container afterwards.
- Container path `/root/.m2` is mapped to host path `${HOME}/.m2` to leverage your local copy of the maven cache. This is optional.

When done using the container, destroy it by running

```shell
$ docker rm -f alluxio-build
```

## Build

Build the source code using Maven:

```shell
$ mvn clean install -DskipTests
```

To speed up the compilation, you can run the following instruction to skip different checks:

```shell
$ mvn -T 2C clean install \
    -DskipTests \
    -Dmaven.javadoc.skip=true \
    -Dfindbugs.skip=true \
    -Dcheckstyle.skip=true \
    -Dlicense.skip=true
```

The Maven build system fetches its dependencies, compiles source code, runs unit tests, and packages
the system. If this is the first time you are building the project, it can take a while to download
all the dependencies. Subsequent builds, however, will be much faster.

## Test

Once Alluxio is built, you can validate and start it with:

```shell
# Alluxio uses ./underFSStorage for under file system storage by default
$ mkdir ./underFSStorage
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local SudoMount
```

To verify that Alluxio is running, you can visit [`http://localhost:19999`](http://localhost:19999) or
check the log in the `alluxio/logs` directory. The `worker.log` and `master.log` files will
typically be the most useful. It may take a few seconds for the web server to start. You can also
run a simple program to test that data can be read and written to Alluxio's UFS:

```shell
$ ./bin/alluxio runTests
```

You should be able to see the result `Passed the test!`

You can stop the local Alluxio system by using:

```shell
$ ./bin/alluxio-stop.sh local
```

## Build Options

### Compute Framework Support

Since Alluxio 1.7, Alluxio client jar built and located at
`{{site.ALLUXIO_CLIENT_JAR_PATH}}` will work with different compute frameworks
(e.g., Spark, Flink, Presto and etc) by default.

### Support Apple M1

Protoc has not yet released the version supporting M1 chip, the compatible mode is required for compiling on M1 Mac

```console
$ mvn clean compile -DskipTests -Dos.detected.classifier=osx-x86_64
```

### Build different HDFS under storage

By default, Alluxio is built with the HDFS under storage of Hadoop 3.3.
Run the following command by specifying `<UFS_HADOOP_PROFILE>` and the corresponding `ufs.hadoop.version` to build ufs with different versions.

```shell
$ mvn install -pl underfs/hdfs/ \
   -P<UFS_HADOOP_PROFILE> -Dufs.hadoop.version=<HADOOP_VERSION> -DskipTests
```

Here `<UFS_HADOOP_VERSION>` can be set for different distributions.
Available Hadoop profiles include `ufs-hadoop-1`, `ufs-hadoop-2`, `ufs-hadoop-3` to cover the major
Hadoop versions 1.x, 2.x and 3.x.

Hadoop versions >= 3.0.0 are best for compatibility with newer releases of Alluxio.

For example,
```shell
$ mvn clean install -pl underfs/hdfs/ \
  -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true \
  -Dcheckstyle.skip=true -Dfindbugs.skip=true \
  -Pufs-hadoop-3 -Dufs.hadoop.version=3.3.1
```

To enable active sync be sure to build using the `hdfsActiveSync` property.
Please visit [Active Sync for HDFS]({{ '/en/core-services/Unified-Namespace.html#active-sync-for-hdfs' | relativize_url }})
for more information on using active sync.

If you find a jar named `alluxio-underfs-hdfs-<UFS_HADOOP_VERSION>-{{site.ALLUXIO_VERSION_STRING}}.jar` under `${ALLUXIO_HOME}/lib`, it indicates successful compilation.

Checkout the flags for different HDFS distributions.

{% accordion HdfsDistributions %}
  {% collapsible Apache %}
All main builds are from Apache so all Apache releases can be used directly

```shell
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
-Pufs-hadoop-2 -Dufs.hadoop.version=2.10.0
-Pufs-hadoop-3 -Dufs.hadoop.version=3.0.0
-Pufs-hadoop-3 -Dufs.hadoop.version=3.3.1
```

  {% endcollapsible %}
  {% collapsible Cloudera %}
To build against Cloudera's releases, just use a version like `$apacheRelease-cdh$cdhRelease`

```shell
-Pufs-hadoop-2 -Dufs.hadoop.version=2.3.0-cdh5.1.0
-Pufs-hadoop-2 -Dufs.hadoop.version=2.0.0-cdh4.7.0
```
  {% endcollapsible %}
  {% collapsible Hortonworks %}
To build against a Hortonworks release, just use a version like `$apacheRelease.$hortonworksRelease`

```shell
-Pufs-hadoop-2 -Dufs.hadoop.version=2.1.0.2.0.5.0-67
-Pufs-hadoop-2 -Dufs.hadoop.version=2.2.0.2.1.0.0-92
-Pufs-hadoop-2 -Dufs.hadoop.version=2.4.0.2.1.3.0-563
```

  {% endcollapsible %}
{% endaccordion %}

## TroubleShooting

### The exception of java.lang.OutOfMemoryError: Java heap space

If you are seeing `java.lang.OutOfMemoryError: Java heap space`, please set the following
variable to increase the memory heap size for maven:

```shell
$ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```

### An error occurred while running protolock

If you see following error message by maven:
"`An error occurred while running protolock: Cannot run program "/alluxio/core/transport/target/protolock-bin/protolock" (in directory "/alluxio/core/transport/target/classes"): error=2, No such file or directory`"

please make sure the maven flag "`-Dskip.protoc`" is NOT included when building the source code.

### NullPointerException occurred while execute org.codehaus.mojo:buildnumber-maven-plugin:1.4:create

If you see following error message by maven like below:
"`Failed to execute goal org.codehaus.mojo:buildnumber-maven-plugin:1.4:create-metadata (default) on project alluxio-core-common: Execution default of goal org.codehaus.mojo:buildnumber-maven-plugin:1.4:create-metadata failed.: NullPointerException`"

Because the build number is based on the revision number retrieved from SCM, it will check build number from git hash code. If check failed, SCM will throw NPE. To avoid the exception,  please set the alluxio version with  maven parameter "`-Dmaven.buildNumber.revisionOnScmFailure`".

For example, if the alluxio version is 2.7.3 then set  parameter "`-Dmaven.buildNumber.revisionOnScmFailure=2.7.3`". 

See https://www.mojohaus.org/buildnumber-maven-plugin/create-mojo.html#revisionOnScmFailure for more infomation.

