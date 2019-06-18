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

### Eclipse

We recommend using either Eclipse or IntelliJ IDEA to contribute to Alluxio. You can generate an
Eclipse configuration file by running:

{% include Contributing-to-Alluxio/eclipse-configuration.md %}

Then import the folder into Eclipse.
You may also have to add the classpath variable `M2_REPO` by running:

{% include Contributing-to-Alluxio/M2_REPO.md %}

### IntelliJ IDEA

If you are using IntelliJ IDEA, you may need to change the Maven profile to 'developer' in order
to avoid import errors. You can do this by going to

> `View > Tool Windows > Maven Projects`

## Maven Targets and Plugins

Before pushing changes or submitting pull requests we recommend running various maven targets on
your local machine to make sure your changes do not break existing behavior.

For these maven commands we'll assume that your command terminal is located in the root directory
of your locally cloned copy of the Alluxio repository.

```bash
cd ${ALLUXIO_HOME}
```

### Checkstyle

To make sure your code follows our style conventions you may run. Note that this is run any time
you run targets such as `compile`, `install`, or `test`.

{% include Contributing-to-Alluxio/checkstyle.md %}

### FindBugs

Before submitting the pull-request, run the latest code against the
[`findbugs`](http://findbugs.sourceforge.net/) Maven plugin to verify no new warnings are
introduced.

{% include Contributing-to-Alluxio/findbugs.md %}

### Compilation

To simply compile the code you can run the following command:

```bash
mvn clean compile -DskipTests
```

This will not execute any unit tests but _will_ execute maven plugins such as `checkstyle` and
`findbugs`.

To speed up compilation you may use the following command:

```bash
mvn -T 2C compile -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip -pl '!webui'
```

This command will skip many of our checks that are in place to help keep our code neat.
We recommend running all checks before committing.

- `-T 2C` runs maven with [up to 2 threads per CPU core](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3)
- `-DskipTests` skips running unit and integration tests
- `-Dmaven.javadoc.skip` skips javadoc generation
- `-Dfindbugs.skip` skips findbugs execution
- `-Dcheckstyle.skip` skips code-style checking
- `-Dlicense.skip` skips checking files for license headers
- `-pl '!webui'` skips building the Alluxio UI module.
If this module isn't compiled then the UI cannot be accessed locally.

You may replace the `compile` target in the above command with any other valid maven target to skip
checks as well.
The targets `compile`, `verify`, and `install` are typically the most useful.

### Creating a Local Install

If you want to test your changes with a compiled version of the repository, you may generate the
jars with the Maven `install` target.
The first time Maven executes it will likely need to download many dependencies.
Please be patient as the first build may take a while.

```bash
mvn -T 2C install -DskipTests
```

After the install target executes, you may configure and start a local cluster
with the following commands:

> If you haven't configured or set up a local cluster yet, run the following commands to configure
a local installation.

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
echo "alluxio.master.hostname=localhost" >> conf/alluxio-site.properties
./bin/alluxio format
```

Once you've run those commands steps you can start a local Alluxio instance with

```bash
./bin/alluxio-start.sh local SudoMount
```

### Unit Tests

- Run all unit and integration tests

```bash
cd ${ALLUXIO_HOME}
mvn test
```

This will use the local filesystem as the under storage.

- Run a single unit test:

```bash
mvn -Dtest=AlluxioFSTest#createFileTest -DfailIfNoTests=false test
```

- To run unit tests for a specific module, execute the `maven test` command targeting
the desired submodule directory. For example, to run tests for HDFS UFS module you would run

```bash
mvn test -pl underfs/hdfs
```

Run unit tests for HDFS UFS module with a different Hadoop version:

```bash
# build and run test on HDFS under storage module for Hadoop 2.7.0
mvn test -pl underfs/hdfs -Phadoop-2 -Dhadoop.version=2.7.0
# build and run test on HDFS under storage module for Hadoop 3.0.0
mvn test -pl underfs/hdfs -Phadoop-3 -Dhadoop.version=3.0.0
```

The above unit tests will create a simulated HDFS service with the specific version.
To run more comprehensive tests on HDFS under storage using a real and running HDFS deployment:

```bash
mvn test -pl underfs/hdfs -PufsContractTest -DtestHdfsBaseDir=hdfs://ip:port/alluxio_test
```

- To have the logs output to STDOUT, append the following arguments to the `mvn` command

```
-Dtest.output.redirect=false -Dalluxio.root.logger=DEBUG,CONSOLE
```

- To quickly test the working of some APIs in an interactive manner, you may
leverage the Scala shell, as discussed in this
[blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding).

- The fuse tests are ignored if the `libfuse` library is missing.
To run those tests, please install the correct libraries mentioned in
[the Alluxio FUSE documentation]({{ '/en/api/POSIX-API.html' | relativize_url }}#requirements).

## Modifying a gRPC definition

Alluxio uses [gRPC](https://grpc.io/) 1.17.1 for RPC communication between clients and servers. The `.proto`
files defined in `core/transport/src/grpc/` are used to auto-generate Java code for calling the
RPCs on clients and implementing the RPCs on servers. To regenerate Java code after changing 
a gRPC definition, you must rebuild `alluxio-core-transport` module with `'generate'` maven profile.

```bash
mvn clean install -Pgenerate
```

## Modifying a Protocol Buffer Message

Alluxio uses [Protocol Buffers](https://developers.google.com/protocol-buffers/) 2.5.0 to read and write journal messages. The `.proto` files
defined in `core/transport/src/proto/` are used to auto-generate Java definitions for
the protocol buffer messages. To change one of these messages, first read about
[updating a message type](https://developers.google.com/protocol-buffers/docs/proto#updating)
to make sure your change will not break backwards compatibility. To regenerate Java code after changing 
a definition, you must rebuild `alluxio-core-transport` module with `'generate'` maven profile.

```bash
mvn clean install -Pgenerate
```

## Usage of `./bin/alluxio`

Most commands in `bin/alluxio` are for developers. The following table explains the description and
the syntax of each command.

<table class="table table-striped">
    <tr><th>Command</th><th>Args</th><th>Description</th></tr>
    {% for dscp in site.data.table.developer-tips %}
        <tr>
            <td>{{dscp.command}}</td>
            <td>{{dscp.args}}</td>
            <td>{{site.data.table.en.developer-tips[dscp.command]}}</td>
        </tr>
    {% endfor %}
</table>

In addition, these commands have different prerequisites. The prerequisite for the `format`,
`formatWorker`, `journalCrashTest`, `readJournal`, `runClass`, `version`, `validateConf` and `validateEnv` commands is
that you have already built Alluxio (see
[Build Alluxio Master Branch]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}) about how to
build Alluxio manually). Further, the prerequisite for the `fs`, `logLevel`, `runTest` and `runTests` commands is that
the Alluxio service is up and running.

