---
layout: global
title: Startup Tasks for New Contributors
---

A list of tasks that everyone should do before contributing to Tachyon.

1.  [Running Tachyon Locally](Running-Tachyon-Locally.html)

2.  [Running Tachyon on a Cluster](Running-Tachyon-on-a-Cluster.html)
    (Optional)

3.  Read
    [Configuration-Settings](Configuration-Settings.html)
    (Optional) and play
    [Command-Line Interface](Command-Line-Interface.html)
    (Optional)

4.  Read and understand [an example](https://github.com/amplab/tachyon/blob/master/main/src/main/java/tachyon/examples/BasicOperations.java).

5.  [Building Tachyon Master Branch](Building-Tachyon-Master-Branch.html).

6.  Fork the repository, write/add unit tests/java doc for one or two files in the following list,
and then submit a pull request. You are also welcome to address issues in our
[JIRA](https://spark-project.atlassian.net/browse/TACHYON)

* * * * *

    src/main/java/tachyon/Users.java

    src/main/java/tachyon/master/MasterWorkerInfo.java

    src/main/java/tachyon/worker/Worker.java

    src/main/java/tachyon/worker/WorkerClient.java

    src/main/java/tachyon/worker/DataServerMessage.java

After the pull request is reviewed and merged, you become a Tachyon contributor!

### Testing

-   Run all unit tests: ``mvn test`` (use localfs as under filesystem) and ``mvn -Dintegration test`` (use HDFS as under filesystem)

-   In Glusterfs environment, also run Glusterfs unit tests: ``mvn -Dglusterfs -Dtachyon.underfs.glusterfs.mounts=/vol -Dtachyon.underfs.glusterfs.volumes=testvol test`` (use Glusterfs as under filesystem, where /vol is a valid Glusterfs mount point) and ``mvn -Dglusterfs test`` (use localfs as under filesystem)

-   Run a single unit test: ``mvn -Dtest=TestCircle#mytest test`` ; e.g.  ``mvn -Dtest=TachyonFSTest#createFileTest test`` ;

-   Quickly test the working of some APIs in an interactive manner, you may leverage
the Scala shell, as discussed in this [blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding).

### Coding Style

-   Follow the style of the existing codebase. Specifically, we use
    [Sun's conventions](http://www.oracle.com/technetwork/java/codeconv-138413.html),
    with the following changes:
    -  Indent **2** spaces per level, not **4**.
    -  Maximum line length of **100** characters.
    -  Imported packages should be order as **alphabetical order**.
    -  `i ++` instead of `i++`
    -  `i + j` instead of `i+j`
    -  Class and member modifiers, when present, appear in the order recommended by the Java
    Language Specification: **public protected private abstract static final transient volatile
    synchronized native strictfp**, then as **alphabetical order**.
-   You can download our [Eclipse formatter](resources/tachyon-code-formatter-eclipse.xml).

### IDE

You can generate Eclipse configure file by run:

    mvn install -Dintegration -DskipTests eclipse:eclipse

Then import the folder into Eclipse.

### Submitting Code

-   Break your work into small, single-purpose patches if possible. It’s much harder to merge in
    a large change with a lot of disjoint features.

-   Make sure that any methods you add maintain the alphabetical ordering of method names in each file.

-   Submit the patch as a GitHub pull request. For a tutorial, see the GitHub guides on
    [forking a repo](https://help.github.com/articles/fork-a-repo) and
    [sending a pull request](https://help.github.com/articles/using-pull-requests).

-   Make sure that your code passes all unit tests: ``mvn test`` and ``mvn -Dintegration test``

### Readings

-   [Tachyon: Memory Throughput I/O for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2013_ladis_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Eric Baldeschwieler, Scott Shenker, Ion Stoica, *LADIS 2013*, November 2013.
