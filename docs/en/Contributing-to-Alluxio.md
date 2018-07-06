---
layout: global
title: Contributing to Alluxio Guidelines
nickname: Contributing to Alluxio Guidelines
group: Resources
priority: 2
---

* Table of Contents
{:toc}

> If you are the first time contributor to the Alluxio open source project, we strongly encourage
> you to follow the step-by-step example in the [Contributing to Alluxio Tutorial](Contributing-Getting-Started.html)
> and finish a new contributor task before making more advanced changes to Alluxio.

Thank you for your interest in Alluxio! We greatly appreciate any new features or fixes.

## Submitting Code

-   We encourage you to break your work into small, single-purpose patches if possible. It is much
    harder to merge in a large change with a lot of disjoint features.
-   We track issues and features in our [JIRA](https://alluxio.atlassian.net/). If you have not
    registered an account, please do so!
-   Open a ticket in [JIRA](https://alluxio.atlassian.net/) detailing the proposed change and what
    purpose it serves.
-   Submit the patch as a GitHub pull request. For a tutorial, see the GitHub guides on
    [forking a repo](https://help.github.com/articles/fork-a-repo) and
    [sending a pull request](https://help.github.com/articles/using-pull-requests).
-   In your pull request title, make sure to reference the JIRA ticket. This will connect the
    ticket to the proposed code changes. for example:

~~~~~
[ALLUXIO-100] Implement an awesome new feature
~~~~~

-   In the description field of the pull request, please include a link to the JIRA ticket.

Note that for some minor changes it is not required to create corresponding JIRA tickets before
submitting the pull requests. For instance:

-   For pull requests that only address typos or formatting issues in source code, you
    can prefix the titles of your pull requests with "[SMALLFIX]", for example:

~~~~~
[SMALLFIX] Fix formatting in Foo.java
~~~~~

-   For pull requests that improve the documentation of Alluxio project website (e.g., modify the
    markdown files in `docs` directory), you can prefix the titles of your pull requests with "[DOCFIX]".
    For example, to edit this web page which is generated from `docs/Contributing-to-Alluxio.md`, the title
    can be:

~~~~~
[DOCFIX] Improve documentation of how to contribute to Alluxio
~~~~~

## Unit Tests

- Run all unit tests

```bash
$ cd ${ALLUXIO_HOME}
$ mvn test
```

This will use the local filesystem as the under filesystem.

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
# build and run test on HDFS under filesystem module for Hadoop 2.7.0
$ mvn test -pl underfs/hdfs -Phadoop-2 -Dhadoop.version=2.7.0
# build and run test on HDFS under filesystem module for Hadoop 3.0.0
$ mvn test -pl underfs/hdfs -Phadoop-3 -Dhadoop.version=3.0.0
```

The above unit tests will create a simulated HDFS service with the specific version.
To run more comprehensive tests on HDFS under filesystem using a real and running HDFS deployment:

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

### System Settings

Sometimes you will need to play with a few system settings in order to have the unit tests pass locally.  A common setting that may need to be set is `ulimit`.

In order to increase the number of files and processes allowed on MacOS, run the following

```bash
$ sudo launchctl limit maxfiles 32768 32768
$ sudo launchctl limit maxproc 32768 32768
```

It is also recommended to exclude your local clone of Alluxio from Spotlight indexing. Otherwise, your Mac may hang constantly trying to re-index the file system during the unit tests.  To do this, go to `System Preferences > Spotlight > Privacy`, click the `+` button, browse to the directory containing your local clone of Alluxio, and click `Choose` to add it to the exclusions list.


## Coding Style

-   Please follow the style of the existing codebase. Specifically, we use
    [Google Java style](https://google.github.io/styleguide/javaguide.html),
    with the following changes or deviations:
    -  Maximum line length of **100** characters.
    -  Third-party imports are grouped together to make IDE formatting much simpler.
    -  Class member variable names should be prefixed with `m`, for example `private WorkerClient
       mWorkerClient;`
    -  Static variable names should be prefixed with `s`, for example `public static String
    sUnderFSAddress;`
-   Bash scripts follow [Google Shell style](https://google.github.io/styleguide/shell.xml), and
    must be compatible with Bash 3.x
-   You can download our [Eclipse formatter](../resources/alluxio-code-formatter-eclipse.xml)
    -  For Eclipse to organize your imports correctly, configure "Organize Imports" to look like
       [this](../resources/eclipse_imports.png)
    -  If you use IntelliJ IDEA:
       - You can either use our formatter with the help from
         [Eclipse Code Formatter](https://github.com/krasa/EclipseCodeFormatter#instructions)
         or use [Eclipse Code Formatter Plugin](http://plugins.jetbrains.com/plugin/6546) in
         IntelliJ IDEA.
       - To automatically format the **import**, configure in
         Preferences->Code Style->Java->Imports->Import Layout according to
         [this order](../resources/intellij_imports.png)
       - To automatically reorder methods alphabetically, try the
         [Rearranger Plugin](http://plugins.jetbrains.com/plugin/173), open Preferences, search for
         rearranger, remove the unnecessary comments, then right click, choose "Rearrange", codes
         will be formatted to what you want

-  To verify that the coding standards match, you should run
   [checkstyle](http://checkstyle.sourceforge.net) before sending a pull-request to verify no new
   warnings are introduced:

```bash
$ mvn checkstyle:checkstyle
```

## Logging Conventions

Alluxio is using [SLF4J](https://www.slf4j.org/) for logging with typical usage pattern of:

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public MyClass {

  private static final Logger LOG = LoggerFactory.getLogger(MyClass.class);

    public void someMethod() {
      LOG.info("Hello world");
    }
}
```

Here is the convention in Alluxio source code on different log levels:

* Error level logging (i.e., `LOG.error`) indicates system level problems which cannot be recovered from.
  It should always be accompanied by a stack trace.
```java
LOG.error("Failed to do something due to an exception", e);
```
* Warn level logging (i.e., `LOG.warn`) indicates a logical mismatch between user intended behavior
and Alluxio behavior. Warn level logs are accompanied by an exception message. The associated stack trace may be found in debug level logs.
```java
LOG.warm("Failed to do something due to {}", e.getMessage());
```
* Info level logging (i.e., `LOG.info`) records important system state changes. Exception messages and stack traces are never associated with info level logs.
Note that, this level of logging should not be used on critical path of operations that may
happen frequently to prevent negative performance impact.
```java
LOG.info("Master started.");
```
* Debug level logging (i.e., `LOG.debug`) includes detailed information for various aspects of
the Alluxio system. Control flow logging (Alluxio system enter and exit calls) is done in debug
level logs. Debug level logging of exceptions typically has the detailed information including
stack trace. Please avoid the slow strings construction on debug-level logging on critical path.
```java
LOG.debug("Failed to connec to {} due to exception", host + ":" + port, e); // wrong
LOG.debug("Failed to connec to {} due to exception", mAddress, e); // OK
if (LOG.isDebugEnabled()) {
    LOG.debug("Failed to connec to address {} due to exception", host + ":" + port, e); // OK
}
```
* Trace level logging (i.e., `LOG.trace`) is not used in Alluxio.

## FindBugs

Before submitting the pull-request, run the latest code against
[FindBugs](http://findbugs.sourceforge.net/) to verify no new warnings are introduced.

{% include Contributing-to-Alluxio/findbugs.md %}

## IDE

You can generate an Eclipse configuration file by running:

{% include Contributing-to-Alluxio/eclipse-configuration.md %}

Then import the folder into Eclipse.

You may also have to add the classpath variable M2_REPO by running:

{% include Contributing-to-Alluxio/M2_REPO.md %}

If you are using IntelliJ IDEA, you may need to change the Maven profile to 'developer' in order
to avoid import errors. You can do this by going to

    View > Tool Windows > Maven Projects

## Change a Thrift RPC definition

Alluxio uses [Thrift](https://thrift.apache.org/) 0.9.3 for RPC communication between clients and servers. The `.thrift`
files defined in `core/common/src/thrift/` are used to auto-generate Java code for calling the
RPCs on clients and implementing the RPCs on servers. To change a Thrift definition, you
must first [install the Thrift compiler](https://thrift.apache.org/docs/install/).
If you have brew, you can do this by running

```bash
$ brew install thrift
```

Then to regenerate the Java code, run

```bash
$ bin/alluxio thriftGen
```

## Change a Protocol Buffer Message

Alluxio uses [Protocol Buffers](https://developers.google.com/protocol-buffers/) 2.5.0 to read and write journal messages. The `.proto` files
defined in `core/protobuf/src/proto/` are used to auto-generate Java definitions for
the protocol buffer messages. To change one of these messages, first read about
[updating a message type](https://developers.google.com/protocol-buffers/docs/proto#updating)
to make sure your change will not break backwards compatibility. Next,
[install protoc](https://github.com/google/protobuf#protocol-buffers---googles-data-interchange-format).
If you have brew, you can do this by running

```bash
$ brew install protobuf
```

Then to regenerate the Java code, run

```bash
$ bin/alluxio protoGen
```

## Full list of the commands in bin/alluxio

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
have already built Alluxio (see [Build Alluxio Master Branch](Building-Alluxio-Master-Branch.html)
about how to build Alluxio manually). Further, the prerequisite for the `fs`, `loadufs`, `logLevel`, `runTest`
and `runTests` commands is that you have a running Alluxio system.
