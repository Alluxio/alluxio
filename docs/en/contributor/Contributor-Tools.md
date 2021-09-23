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

We recommend using either Eclipse or IntelliJ IDEA to contribute to Alluxio.
Instructions for setting up both IDEs can be found below.

### Eclipse

You can generate an Eclipse configuration file by running:

```console
$ mvn clean -Pdeveloper install -DskipTests
$ mvn clean -Pdeveloper -DskipTests eclipse:eclipse -DdownloadJavadocs=true -DdownloadSources=true
```

Then import the folder into Eclipse.
You may also have to add the classpath variable `M2_REPO` by running:

```console
$ mvn -Declipse.workspace="your Eclipse Workspace" eclipse:configure-workspace
```

> Note: Alluxio 2.2 moved generated gRPC proto source files into `alluxio/core/transport/target/generated-sources/protobuf/`.
You will need to mark the directory as a source folder for Eclipse to resolve the source files.

### IntelliJ IDEA

To use IntelliJ IDEA to contribute to Alluxio, simply open IntelliJ and select "Import existing project".
Then select the "Maven" project type from the IntelliJ dialog.
IntelliJ's default configuration works without any modifications. 

After successfully importing your local Alluxio repo into IntelliJ, you may need to add the Maven profile 'developer'
in order to avoid import errors.

You can do this by going to

> `View > Tool Windows > Maven Projects`

And then check the box next to "developer" in the window pane. 

> Note: Alluxio 2.2 moved generated gRPC proto source files into `alluxio/core/transport/target/generated-sources/protobuf/`.
You will need to mark the directory as "Generated Sources Root" for IntelliJ to resolve the source files.

#### Run Alluxio processes within IntelliJ IDEA

##### Start a single master Alluxio cluster
1. Run `dev/intellij/install-runconfig.sh`
2. Restart IntelliJ IDEA
3. Edit `conf/alluxio-site.properties`
```console
  alluxio.home={alluxio.home}
  alluxio.master.hostname=localhost
  alluxio.master.journal.type=UFS
```
4. Edit `conf/log4j.properties` to print log in console
```console
  log4j.rootLogger=INFO, ${alluxio.logger.type}, ${alluxio.remote.logger.type}, stdout
  log4j.threshold=ALL
  log4j.appender.stdout=org.apache.log4j.ConsoleAppender
  log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
  log4j.appender.stdout.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n
```
5. Format the Alluxio master by running `bin/alluxio formatMaster`
6. In Intellij, start Alluxio master process by selecting `Run > Run > AlluxioMaster`
7. Prepare the RamFS and format the Alluxio Worker with `bin/alluxio-mount.sh SudoMount && bin/alluxio formatWorker`
8. In Intellij, start Alluxio worker process by selecting `Run > Run > AlluxioWorker`
9. Verify the Alluxio cluster is up as [Running Alluxio Locally]({{ '/en/deploy/Running-Alluxio-Locally.html#verify-alluxio-is-running' | relativize_url }})

##### Start a High Availability (HA) Alluxio cluster
1. Execute the following command
```console
mkdir -p /tmp/alluxio-tmp/alluxio-0/journal
mkdir -p /tmp/alluxio-tmp/alluxio-1/journal
mkdir -p /tmp/alluxio-tmp/alluxio-2/journal
```
> Note: If the journal folder exist, and you want to apply a new HA cluster, you should clear files in the journal folder first.  
2. Run `dev/intellij/install-runconfig.sh`
3. Restart IntelliJ IDEA
4. Edit `conf/alluxio-site.properties`
```console
  alluxio.home={alluxio.home}
  alluxio.master.hostname=localhost
  alluxio.master.embedded.journal.addresses=localhost:19200,localhost:19201,localhost:19202
  alluxio.master.rpc.addresses=localhost:19998,localhost:19988,localhost:19978
```
5. In Intellij, start Alluxio master 0 process by selecting `Run > Run > AlluxioMaster-0`
6. In Intellij, start Alluxio master 1 process by selecting `Run > Run > AlluxioMaster-1`
7. In Intellij, start Alluxio master 2 process by selecting `Run > Run > AlluxioMaster-2`
8. Prepare the RamFS and format the Alluxio Worker with `bin/alluxio-mount.sh SudoMount && bin/alluxio formatWorker`
9. In Intellij, start Alluxio worker process by selecting `Run > Run > AlluxioWorker`
10. Verify the HA Alluxio cluster is up, Run`bin/alluxio fsadmin journal quorum info -domain MASTER`, and you will get message like this:
```console
Journal domain	: MASTER
Quorum size	: 3
Quorum leader	: localhost:19201

STATE       | PRIORITY | SERVER ADDRESS
AVAILABLE   | 0        | localhost:19200
AVAILABLE   | 0        | localhost:19201
AVAILABLE   | 0        | localhost:19202
```
In addition, you can also start a High Availability (HA) JobMaster process on this basis.

11. Edit `conf/alluxio-site.properties` and add lines
```console
    alluxio.job.master.rpc.addresses=localhost:20001,localhost:20011,localhost:20021
    alluxio.job.master.embedded.journal.addresses=localhost:20003,localhost:20013,localhost:20023
```
12. In Intellij, start Alluxio master 0 process by selecting `Run > Run > AlluxioJobMaster-0`
13. In Intellij, start Alluxio master 1 process by selecting `Run > Run > AlluxioJobMaster-1`
14. In Intellij, start Alluxio master 2 process by selecting `Run > Run > AlluxioJobMaster-2`
15. Verify the HA JobMaster process is up, Run`bin/alluxio job leader`, and you will get message like this:
```console
    localhost
```

## Maven Targets and Plugins

Before pushing changes or submitting pull requests, we recommend running various maven targets on
your local machine to make sure your changes do not break existing behavior.

For these maven commands we'll assume that your command terminal is located in the root directory
of your local copy of the Alluxio repository.

```console
$ cd ${ALLUXIO_HOME}
```

### Checkstyle

To make sure your code follows our style conventions you may run. Note that this is run any time
you run targets such as `compile`, `install`, or `test`.

```console
$ mvn checkstyle:checkstyle
```

### FindBugs

Before submitting the pull-request, run the latest code against the
[`findbugs`](http://findbugs.sourceforge.net/) Maven plugin to verify no new warnings are
introduced.

```console
$ mvn compile findbugs:findbugs
```

### Compilation

To simply compile the code you can run the following command:

```console
$ mvn clean compile -DskipTests
```

This will not execute any unit tests but _will_ execute maven plugins such as `checkstyle` and
`findbugs`.

To speed up compilation you may use the following command:

```console
$ mvn -T 2C compile -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip \
  -Dlicense.skip -pl '!webui'
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

```console
$ mvn -T 2C install -DskipTests
```

After the install target executes, you can follow the instructions at 
[Running Alluxio Locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }})
to start a local cluster.

### Unit Tests

#### Run all unit and integration tests

```console
$ cd ${ALLUXIO_HOME}
$ mvn test
```

This will use the local filesystem as the under storage.

#### Run a single unit test

```console
$ mvn -Dtest=<AlluxioTestClass>#<testMethod> -DfailIfNoTests=false test
```

#### Run unit tests for a specific module
You can execute the `maven test` command targeting
the desired submodule directory. For example, to run tests for HDFS UFS module you would run

```console
$ mvn test -pl underfs/hdfs
```

#### Run unit tests for HDFS UFS module with a different Hadoop version

```console
# build and run test on HDFS under storage module for Hadoop 2.7.0
$ mvn test -pl underfs/hdfs -Phadoop-2 -Dhadoop.version=2.7.0
# build and run test on HDFS under storage module for Hadoop 3.0.0
$ mvn test -pl underfs/hdfs -Phadoop-3 -Dhadoop.version=3.0.0
```

The above unit tests will create a simulated HDFS service with a specific version.
To run more comprehensive tests on HDFS under storage using a real and running HDFS deployment:

```console
$ mvn test -pl underfs/hdfs -PufsContractTest -DtestHdfsBaseDir=hdfs://ip:port/alluxio_test
```

#### Redirect logs to STDOUT
To have the logs output to STDOUT, append the following arguments to the `mvn` command

```
-Dtest.output.redirect=false -Dalluxio.root.logger=DEBUG,CONSOLE
```

#### Test FUSE

The FUSE tests are ignored if the `libfuse` library is missing.
To run those tests, please install the libraries referenced in
[the Alluxio FUSE documentation]({{ '/en/api/POSIX-API.html' | relativize_url }}#requirements).

## Modifying a gRPC definition

Alluxio uses [gRPC](https://grpc.io/) 1.37.0 for RPC communication between clients and servers. The `.proto`
files defined in `core/transport/src/grpc/` are used to auto-generate Java code for calling the
RPCs on clients and implementing the RPCs on servers. To regenerate Java code after changing 
a gRPC definition, you must rebuild `alluxio-core-transport` module with `'generate'` maven profile.

```console
$ mvn clean install -Pgenerate
```

## Modifying a Protocol Buffer Message

Alluxio uses [Protocol Buffers](https://developers.google.com/protocol-buffers/) 3.12 to read and write journal entries.
The `.proto` files defined in `core/transport/src/proto/` are used to auto-generate Java definitions for the protocol
buffer messages.

To change one of these messages, first read about
[updating a message type](https://developers.google.com/protocol-buffers/docs/proto#updating)
to make sure your change will not break backwards compatibility.
To regenerate Java code after changing  a definition, you must rebuild `alluxio-core-transport` module with
the `'generate'` maven profile.

```console
$ mvn clean install -Pgenerate
```

## Usage of `./bin/alluxio`
 
Please refer to [Alluxio commands]({{ '/en/operation/User-CLI.html' | relativize_url }})
for all available commands.

Some commands have different prerequisites.

All commands except `bootstrapConf`, `killAll`, `copyDir` and `clearCache`
will require that you have already built Alluxio 
(see [Build Alluxio Master Branch]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}) 
about how to build Alluxio manually).

Some commands require the Alluxio cluster to be running, and others do not.
Please check [all Alluxio commands]({{ '/en/operation/User-CLI.html' | relativize_url }})
where each command specifies if it requires the Alluxio cluster to be running.
