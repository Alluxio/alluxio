---
layout: global
title: Startup Tasks for New Contributors
---

Thank you for contributing to Tachyon! We greatly appreciate any additions or bug fixes. Here are
few things that everyone should do before developing on Tachyon.

1.  [Running Tachyon Locally](Running-Tachyon-Locally.html)

2.  [Running Tachyon on a Cluster](Running-Tachyon-on-a-Cluster.html)
    (Optional)

3.  Read
    [Configuration-Settings](Configuration-Settings.html)
    (Optional) and
    [Command-Line Interface](Command-Line-Interface.html)
    (Optional)

4.  Read and understand [an example](https://github.com/amplab/tachyon/blob/master/core/src/main/java/tachyon/examples/BasicOperations.java).

5.  [Building Tachyon Master Branch](Building-Tachyon-Master-Branch.html).

6.  Fork the repository, add unit tests or javadoc for one or two files in the following list,
and then submit a pull request. You are also welcome to address issues in our
[JIRA](https://spark-project.atlassian.net/browse/TACHYON). For a tutorial, see the GitHub guides on
[forking a repo](https://help.github.com/articles/fork-a-repo) and
[sending a pull request](https://help.github.com/articles/using-pull-requests).

* * * * *

    core/src/main/java/tachyon/Users.java

    core/src/main/java/tachyon/master/MasterWorkerInfo.java

    core/src/main/java/tachyon/worker/Worker.java

    core/src/main/java/tachyon/worker/WorkerClient.java

    core/src/main/java/tachyon/worker/DataServerMessage.java

### Testing

-   Run all unit tests with ``mvn test`` (will use the local filesystem as the under filesystem) and
    -``mvn Dtest.profile=hdfs test`` (will use HDFS as the under filesystem)

-   In GlusterFS environment, also run GlusterFS unit tests:
    ``mvn -Dhadoop.version=2.3.0 -Dtest.profile=glusterfs -Dtachyon.underfs.glusterfs.mounts=/vol
    -Dtachyon.underfs.glusterfs.volumes=testvol test``
    (use GlusterFS as under filesystem, where /vol is a valid GlusterFS mount point) and
    ``mvn -Dhadoop.version=2.3.0 -Dtest.profile=glusterfs test`` (use localfs as under filesystem)

-   Run a single unit test: ``mvn -Dtest=TestCircle#mytest test`` ; e.g.
    ``mvn -Dtest=TachyonFSTest#createFileTest test`` ;

-   To quickly test the working of some APIs in an interactive manner, you may leverage
the Scala shell, as discussed in this [blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding).

-   Run tests with a different Hadoop version: ``mvn -Dhadoop.version=2.2.0 clean test``

### Coding Style

-   Please follow the style of the existing codebase. Specifically, we use
    [Sun's conventions](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html),
    with the following changes:
    -  Indent **2** spaces per level, not **4**.
    -  Maximum line length of **100** characters.
    -  Imported packages should be in **alphabetical order**.
    -  `i ++` instead of `i++`
    -  `i + j` instead of `i+j`
    -  Class and member modifiers, when present, appear in the order recommended by the Java
    Language Specification: **public protected private abstract static final transient volatile
    synchronized native strictfp**, then as **alphabetical order**.
-   You can download our [Eclipse formatter](resources/tachyon-code-formatter-eclipse.xml).
    -  If you use Intellij, you can use our formatter with the help from [EclipseCodeFormatter](https://github.com/krasa/EclipseCodeFormatter#instructions)

### IDE

You can generate an Eclipse configuration file by running:

    mvn install -Dintegration -DskipTests eclipse:eclipse

Then import the folder into Eclipse.

### Submitting Code

-   We encourage you to break your work into small, single-purpose patches if possible. It’s much
    harder to merge in a large change with a lot of disjoint features.

-   Make sure that any methods you add maintain the alphabetical ordering of method names in each file.

-   Submit the patch as a GitHub pull request. For a tutorial, see the GitHub guides on
    [forking a repo](https://help.github.com/articles/fork-a-repo) and
    [sending a pull request](https://help.github.com/articles/using-pull-requests).

-   Make sure that your code passes all unit tests: ``mvn test`` and ``mvn -Dintegration test``

### Readings

-   [Tachyon: Memory Throughput I/O for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2013_ladis_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Eric Baldeschwieler, Scott Shenker, Ion Stoica, *LADIS 2013*, November 2013.
