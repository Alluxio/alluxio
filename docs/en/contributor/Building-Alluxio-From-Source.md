---
layout: global
title: Building Alluxio From Source
---

This guide describes how to clone the Alluxio repository, compile the source code, and run tests in your environment.


## Required Software

- [Java 11](https://www.oracle.com/java/technologies/downloads/#java11)
- [Maven 3.8.6 or later](http://maven.apache.org/download.cgi)
- [Git](https://git-scm.org/downloads)

Alternatively, we have published a docker image [alluxio/alluxio-maven](https://hub.docker.com/r/alluxio/alluxio-maven) with Java, Maven, and Git pre-installed to help build Alluxio source code.

## Checkout Source Code

Checkout the Alluxio main branch from Github:

```shell
$ git clone https://github.com/Alluxio/alluxio.git
$ cd alluxio
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
$ mvn clean install \
    -DskipTests \
    -Dmaven.javadoc.skip=true \
    -Dfindbugs.skip=true \
    -Dcheckstyle.skip=true \
    -Dlicense.skip=true
```

The Maven build system fetches its dependencies, compiles source code, runs unit tests, and packages the system.
If this is the first time you are building the project, it can take a while to download all the dependencies.
Subsequent builds, however, will be much faster.

## Test

Once Alluxio is built, you can start it with:

```shell
$ ./bin/alluxio-start.sh local SudoMount
```

To verify that Alluxio is running, you can visit [`http://localhost:19999`](http://localhost:19999) or
check the log in the `alluxio/logs` directory.
The `worker.log` and `master.log` files will typically be the most useful.
It may take a few seconds for the web server to start.
You can run a test command to verify that data can be read and written to Alluxio:

```shell
$ ./bin/alluxio runTests
```

You should be able to see the result `Passed the test!`

Stop the local Alluxio system by using:

```shell
$ ./bin/alluxio-stop.sh local
```

## Troubleshooting

### The exception of java.lang.OutOfMemoryError: Java heap space

If you are seeing `java.lang.OutOfMemoryError: Java heap space`, please set the following
variable to increase the memory heap size for maven:

```shell
$ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```

### NullPointerException occurred while execute org.codehaus.mojo:buildnumber-maven-plugin:1.4:create

If you see following error message by maven like below:
"`Failed to execute goal org.codehaus.mojo:buildnumber-maven-plugin:1.4:create-metadata (default) on project alluxio-core-common: Execution default of goal org.codehaus.mojo:buildnumber-maven-plugin:1.4:create-metadata failed.: NullPointerException`"

Because the build number is based on the revision number retrieved from SCM, it will check build number from git hash code.
If check failed, SCM will throw a NPE.
To avoid the exception, please set the Alluxio version with parameter "`-Dmaven.buildNumber.revisionOnScmFailure`".
For example, if the alluxio version is 2.7.3 then set "`-Dmaven.buildNumber.revisionOnScmFailure=2.7.3`". 

See [revisionOnScmFailure](https://www.mojohaus.org/buildnumber-maven-plugin/create-mojo.html#revisionOnScmFailure) for more information.

