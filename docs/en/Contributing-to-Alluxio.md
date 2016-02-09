---
layout: global
title: Contributing to Alluxio
nickname: Contributor Guide
group: Resources
---

* Table of Contents
{:toc}

Thank you for your interest in Alluxio! We greatly appreciate any new features or fixes.

### Alluxio Getting Started Tasks

There are a few things that new contributors can do to familiarize themselves with Alluxio:

1.  [Run Alluxio Locally](Running-Alluxio-Locally.html)

2.  [Run Alluxio on a Cluster](Running-Alluxio-on-a-Cluster.html)

3.  Read [Configuration-Settings](Configuration-Settings.html) and [Command-Line Interface](Command-Line-Interface.html)

4.  Read a
    [Code Example](https://github.com/amplab/alluxio/blob/master/examples/src/main/java/alluxio/examples/BasicOperations.java)

5.  [Build Alluxio Master Branch](Building-Alluxio-Master-Branch.html)

6.  Fork the repository, add unit tests or javadoc for one or two files, and submit a pull request. You are also welcome to address
issues in our [JIRA](https://alluxio.atlassian.net/browse/TACHYON).
Here is a list of
[tasks](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20NewContributor%20AND%20status%20%3D%20OPEN)
for New Contributors. Please limit 2 tasks per New Contributor.
Afterwards, try some Beginner/Intermediate tasks, or ask in the
[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users).
For a tutorial, see the GitHub guides on
[forking a repo](https://help.github.com/articles/fork-a-repo) and
[sending a pull request](https://help.github.com/articles/using-pull-requests).

### Submitting Code

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
[TACHYON-100] Implement an awesome new feature
~~~~~

-   In the description field of the pull request, please include a link to the JIRA ticket.


Note that for some minor changes it is not required to create corresponding JIRA tickets before
submiting the pull requests. For instance:

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

#### Testing

-   Run all unit tests with ``mvn test`` (will use the local filesystem as the under filesystem and
HDFS 1.0.4 as the under filesystem in the HDFS module). ``mvn -Dhadoop.version=2.4.0 test`` will
use HDFS 2.4.0 as the under filesystem for the HDFS module tests.

-   To run tests against specific under filesystems, execute the maven command from the desired
submodule directory. For example, to run tests for HDFS you would run ``mvn test`` from ``alluxio/underfs/hdfs``.

-   For a GlusterFS environment, GlusterFS unit tests can be run from ``alluxio/underfs/glusterfs`` with:
`mvn -PglusterfsTest -Dhadoop.version=2.3.0 -Dalluxio.underfs.glusterfs.mounts=/vol
-Dalluxio.underfs.glusterfs.volumes=testvol test` (use GlusterFS as under filesystem,
where `/vol` is a valid GlusterFS mount point)

-   Run a single unit test: `mvn -Dtest=AlluxioFSTest#createFileTest -DfailIfNoTests=false test`

-   To quickly test the working of some APIs in an interactive manner, you may
leverage the Scala shell, as discussed in this
[blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding).

-   Run tests with a different Hadoop version: ``mvn -Dhadoop.version=2.2.0 clean test``

-   Run tests with Hadoop FileSystem contract tests (uses hadoop 2.6.0):
`mvn -PcontractTest clean test`

#### Coding Style

-   Please follow the style of the existing codebase. Specifically, we use
    [Google Java style](http://google-styleguide.googlecode.com/svn/trunk/javaguide.html),
    with the following changes or deviations:
    -  Maximum line length of **100** characters.
    -  Third-party imports are grouped together to make IDE formatting much simpler.
    -  Class member variable names should be prefixed with `m`, for example `private WorkerClient
       mWorkerClient;`
    -  Static variable names should be prefixed with `s`, for example `public static String
    sUnderFSAddress;`
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
-   Alluxio is using SLF4J for logging with typical usage pattern of:

{% include Contributing-to-Alluxio/slf4j.md %}

-  To verify that the coding standards match, you should run
   [checkstyle](http://checkstyle.sourceforge.net) before sending a pull-request to verify no new
   warnings are introduced:

{% include Contributing-to-Alluxio/checkstyle.md %}

#### FindBugs

Before submitting the pull-request, run the latest code against
[FindBugs](http://findbugs.sourceforge.net/) to verify no new warnings are introduced.

{% include Contributing-to-Alluxio/findbugs.md %}

### IDE

You can generate an Eclipse configuration file by running:

{% include Contributing-to-Alluxio/eclipse-configuration.md %}

Then import the folder into Eclipse.

You may also have to add the classpath variable M2_REPO by running:

{% include Contributing-to-Alluxio/M2_REPO.md %}

If you are using IntelliJ IDEA, you may need to change the Maven profile to 'developer' in order
to avoid import errors. You can do this by going to

    View > Tool Windows > Maven Projects

### Presentations

-   AMPCamp 6 (November, 2015)
[SlideShare](http://www.slideshare.net/AlluxioNexus/alluxio-presentation-at-ampcamp-6-november-2015)
-   Strata and Hadoop World 2015 (September, 2015)
[SlideShare](http://www.slideshare.net/AlluxioNexus/alluxio-an-open-source-memorycentric-distributed-storage-system)
-   Strata and Hadoop World 2014 (October, 2014)
[pdf](http://www.cs.berkeley.edu/~haoyuan/talks/Alluxio_2014-10-16-Strata.pdf)
[pptx](http://www.cs.berkeley.edu/~haoyuan/talks/Alluxio_2014-10-16-Strata.pptx)
-   Spark Summit 2014 (July, 2014) [pdf](http://goo.gl/DKrE4M)
-   Strata and Hadoop World 2013 (October, 2013) [pdf](http://goo.gl/AHgz0E)

### Readings

-   [Alluxio: Reliable, Memory Speed Storage for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2014_socc_alluxio.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Scott Shenker, Ion Stoica, *SOCC 2014*.
-   [Reliable, Memory Speed Storage for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2014_EECS_alluxio.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Scott Shenker, Ion Stoica, *UC Berkeley EECS 2014*.
-   [Alluxio: Memory Throughput I/O for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2013_ladis_alluxio.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Eric Baldeschwieler, Scott Shenker, Ion Stoica, *LADIS 2013*.
