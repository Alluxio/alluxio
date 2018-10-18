---
layout: global
title: Contributor Tools
nickname: Contributor Tools
group: Contributor Resources
priority: 3
---

* Table of Contents
{:toc}

## IDE

We recommend using either Eclipse or IntelliJ IDEA to contribute to Alluxio. You can generate an
Eclipse configuration file by running:

{% include Contributing-to-Alluxio/eclipse-configuration.md %}

Then import the folder into Eclipse.

You may also have to add the classpath variable M2_REPO by running:

{% include Contributing-to-Alluxio/M2_REPO.md %}

If you are using IntelliJ IDEA, you may need to change the Maven profile to 'developer' in order
to avoid import errors. You can do this by going to

View > Tool Windows > Maven Projects

## Maven Targets and Plugins

Before pushing changes or submitting pull requests we recommend running various maven targets on
your local machine to make sure your changes do not break existing behavior.

For these maven commands we'll assume that your command terminal is located in the root directory
of your locally cloned copy of the Alluxio repository.

```bash
$ cd ${ALLUXIO_HOME}
```

### Checkstyle

To make sure your code follows our style conventions you may run. Note that this is run any time
you run targets such as `compile`, `install`, or `test`.

{% include Contributing-To-Alluxio/checkstyle.md %}

### FindBugs

Before submitting the pull-request, run the latest code against the
[`findbugs`](http://findbugs.sourceforge.net/) Maven plugin to verify no new warnings are
introduced.

{% include Contributing-to-Alluxio/findbugs.md %}

### Compilation

To simply compile the code you can run the following command:

```bash
$ mvn clean compile -DskipTests
```

This will not execute any unit tests but will execute the `checkstyle`, `findbugs`, and other
plugins.

To speed up compilation you may use the command:

```bash
$ mvn -T 2C compile -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip
```

This command will skip many of our checks that are in place to help keep our code neat. We
recommend running all checks before committing.

You may replace the `compile` target in the above command with any other valid target to skip checks
as well. The targets `install`, `verify`, and `compile` will be most useful.


### Creating a Local Install

If you want to test your changes with a compiled version of the repository, you may generate the
jars with the Maven `install` target.

```bash
mvn install -DskipTests
```

After the install target executes, you may configure and start a local cluster
with the following commands:

> If you haven't configured or set up a local cluster yet, run the following commands

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
$ echo "alluxio.master.hostname=localhost" >> conf/alluxio-site.properties
$ ./bin/alluxio format
```

Once you've run those configuration steps you can start a local Alluxio instance with

```bash
./bin/alluxio-start.sh local SudoMount
```

### Unit Tests

- Run all unit and integration tests

```bash
$ cd ${ALLUXIO_HOME}
$ mvn test
```

This will use the local filesystem as the under storage.

- Run a single unit test:

```bash
$ mvn -Dtest=AlluxioFSTest#createFileTest -DfailIfNoTests=false test
```

- To run unit tests for a specific module, execute the `maven test` command targeting
the desired submodule directory. For example, to run tests for HDFS UFS module you would run

```bash
$ mvn test -pl underfs/hdfs
```

Run unit tests for HDFS UFS module with a different Hadoop version:

```bash
# build and run test on HDFS under storage module for Hadoop 2.7.0
$ mvn test -pl underfs/hdfs -Phadoop-2 -Dhadoop.version=2.7.0
# build and run test on HDFS under storage module for Hadoop 3.0.0
$ mvn test -pl underfs/hdfs -Phadoop-3 -Dhadoop.version=3.0.0
```

The above unit tests will create a simulated HDFS service with the specific version.
To run more comprehensive tests on HDFS under storage using a real and running HDFS deployment:

```bash
$ mvn test -pl underfs/hdfs -PufsContractTest -DtestHdfsBaseDir=hdfs://ip:port/alluxio_test
```

- To have the logs output to STDOUT, append the following arguments to the `mvn` command

```
-Dtest.output.redirect=false -Dalluxio.root.logger=DEBUG,CONSOLE
```

- To quickly test the working of some APIs in an interactive manner, you may
leverage the Scala shell, as discussed in this
[blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding).

- The fuse tests are ignored if the libfuse library is missing. To run those tests, please install the correct libraries
mentioned in [this page](Mounting-Alluxio-FS-with-FUSE.html#requirements).

## Modifying a Thrift RPC definition

Alluxio uses [Thrift](https://thrift.apache.org/) 0.9.3 for RPC communication between clients and servers. The `.thrift`
files defined in `core/common/src/thrift/` are used to auto-generate Java code for calling the
RPCs on clients and implementing the RPCs on servers. To change a Thrift definition, you
must first [install the Thrift compiler](https://thrift.apache.org/docs/install/).
If you have brew, you can do this by running

```bash
$ brew install thrift@0.9
$ brew link --force thrift@0.9
```

Then to regenerate the Java code, run

```bash
$ bin/alluxio thriftGen
```

## Modifying a Protocol Buffer Message

Alluxio uses [Protocol Buffers](https://developers.google.com/protocol-buffers/) 2.5.0 to read and write journal messages. The `.proto` files
defined in `core/protobuf/src/proto/` are used to auto-generate Java definitions for
the protocol buffer messages. To change one of these messages, first read about
[updating a message type](https://developers.google.com/protocol-buffers/docs/proto#updating)
to make sure your change will not break backwards compatibility. Next,
[install protoc](https://github.com/google/protobuf#protocol-buffers---googles-data-interchange-format).
If you have brew, you can do this by running

```bash
$ brew install protobuf@2.5
$ brew link --force protobuf@2.5
```

Then to regenerate the Java code, run

```bash
$ bin/alluxio protoGen
```

## Usage of `./bin/alluxio`

Most commands in `bin/alluxio` are for developers. The following table explains the description and
the syntax of each command.

<table class="table table-striped">
    <tr><th>Command</th><th>Args</th><th>Description</th></tr>
    {% for dscp in site.data.table.Developer-Tips %}
        <tr>
            <td>{{dscp.command}}</td>
            <td>{{dscp.args}}</td>
            <td>{{site.data.table.en.Developer-Tips[dscp.command]}}</td>
        </tr>
    {% endfor %}
</table>

In addition, these commands have different prerequisites. The prerequisite for the `format`,
`formatWorker`, `journalCrashTest`, `readJournal`, `version`, `validateConf` and `validateEnv` commands is that you
have already built Alluxio (see [Build Alluxio Master Branch](Building-Alluxio-From-Source.html)
about how to build Alluxio manually). Further, the prerequisite for the `fs`, `loadufs`, `logLevel`, `runTest`
and `runTests` commands is that you have a running Alluxio system.
