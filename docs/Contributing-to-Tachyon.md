---
layout: global
title: Contributing to Tachyon
---

Thank you for your interest in Tachyon! We greatly appreciate any new features or fixes.

* [Submitting Code](#submitting-code)
  * [Testing](#testing)
  * [Coding Style](#coding-style)
  * [FindBugs](#findbugs)
* [Startup Tasks for New Contributors](#startup-tasks-for-new-contributors)
* [IDE](#ide)
* [Presentations](#presentations)
* [Readings](#readings)

### Submitting Code

-   We encourage you to break your work into small, single-purpose patches if possible. It’s much
    harder to merge in a large change with a lot of disjoint features.

-   We track issues and features in our [JIRA](https://tachyon.atlassian.net/). If you haven't
registered an account, please do so!

-   Open a ticket in [JIRA](https://tachyon.atlassian.net/) detailing the proposed change and what
purpose it serves.

-   Submit the patch as a GitHub pull request. For a tutorial, see the GitHub guides on
    [forking a repo](https://help.github.com/articles/fork-a-repo) and
    [sending a pull request](https://help.github.com/articles/using-pull-requests).

-   In your pull request title, make sure to reference the JIRA ticket, for example:

~~~~~
[TACHYON-100] Awesome New Feature
~~~~~

This will connect the ticket to the proposed code changes. In the description field of the pull
request, please include a link to the JIRA ticket.

-   For pull requests that only address typos or formating issues, it is not
    required to create the JIRA ticket and reference this JIRA. Instead, you
    can make the title of your pull requests prefixed by "[SMALLFIX]", for example:

~~~~~
[SMALLFIX] Fix a typo in Foo
~~~~~

### Testing

-   Run all unit tests with ``mvn test`` (will use the local filesystem as the under filesystem and
HDFS 1.0.4 as the under filesystem in the HDFS module.). ``mvn -Dhadoop.version=2.4.0 test`` will use
HDFS 2.4.0 as the under filesystem for the HDFS module tests.

-   To run tests against specific under filesystems, execute the maven command from the desired
submodule directory, for example for HDFS, tachyon/underfs/hdfs.

-   In GlusterFS environment, GlusterFS unit tests can be run from tachyon/underfs/glusterfs with:
``mvn -PglusterfsTest -Dhadoop.version=2.3.0 -Dtachyon.underfs.glusterfs.mounts=/vol
-Dtachyon.underfs.glusterfs.volumes=testvol test`` (use GlusterFS as under filesystem,
where /vol is a valid GlusterFS mount point)

-   Run a single unit test: ``mvn -Dtest=TestCircle#mytest test`` ; e.g.
``mvn -Dtest=TachyonFSTest#createFileTest test`` ;

-   To quickly test the working of some APIs in an interactive manner, you may
leverage the Scala shell, as discussed in this
[blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding).

-   Run tests with a different Hadoop version: ``mvn -Dhadoop.version=2.2.0 clean test``

-   Run tests with Hadoop FileSystem contract tests (uses hadoop 2.6.0):
``mvn -PcontractTest clean test``

### Coding Style

-   Please follow the style of the existing codebase. Specifically, we use
    [Google Java style](http://google-styleguide.googlecode.com/svn/trunk/javaguide.html),
    with the following changes or deviations:
    -  Maximum line length of **100** characters.
    -  Imported packages should be in [this order](resources/order.importorder), then in
    **alphabetical order** in each group.
    -  `i ++` instead of `i++`
    -  `i + j` instead of `i+j`
    -  Class and member modifiers, when present, appear in the order recommended by the Java
    Language Specification: **public protected private abstract static final transient volatile
    synchronized native strictfp**, then as **alphabetical order**.
    -  Class member variable names should be prefixed with `m`, for example `private WorkerClient mWorkerClient;`
    -  Static variable names should be prefixed with `s`, for example `public static String sUnderFSAddress;`
    -  Do not add `public` or `abstract` modifier to methods defined in an Java interface because
       method declaration in the body of an interface is implicitly public and abstract.
       (http://docs.oracle.com/javase/specs/jls/se7/html/jls-9.html#jls-9.4)
-   You can download our [Eclipse formatter](resources/tachyon-code-formatter-eclipse.xml)
    -  If you use IntelliJ IDEA:
       - you can either use our formatter with the help from
         [Eclipse Code Formatter](https://github.com/krasa/EclipseCodeFormatter#instructions)
         or use [Eclipse Code Formatter Plugin](http://plugins.jetbrains.com/plugin/6546) in IntelliJ
         IDEA.
       - To automatically format the **import**, configure in Preferences->Code Style->Java->Imports->Import Layout
         according to [this order](resources/order.importorder)
       - To automatically reorder methods alphabetically, try the
         [Rearranger Plugin](http://plugins.jetbrains.com/plugin/173), open Preferences, search for rearranger,
         remove the unnecessary comments, then right click, choose "Rearrange", codes will be formatted to what you want
-   Tachyon is using SLF4J for logging with typical usage pattern of:

        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        public MyClass {

          private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

          public void someMethod() {
            LOG.info("Hello world");
          }
        }
-  To verify that the coding standards match, you should run [checkstyle](http://checkstyle.sourceforge.net)
   before sending a pull-request to verify no new warnings are introduced:

        $ mvn checkstyle:checkstyle

### FindBugs

Before submitting the pull-request, run the latest code against
[FindBugs](http://findbugs.sourceforge.net/) to verify no new warnings are introduced.

    $ mvn compile findbugs:findbugs findbugs:gui

### Startup Tasks for New Contributors

Here are a few things that everyone should do before developing on Tachyon.

1.  [Running Tachyon Locally](Running-Tachyon-Locally.html)

2.  [Running Tachyon on a Cluster](Running-Tachyon-on-a-Cluster.html)
    (Optional)

3.  Read
    [Configuration-Settings](Configuration-Settings.html)
    (Optional) and
    [Command-Line Interface](Command-Line-Interface.html)
    (Optional)

4.  Read and understand [an example](https://github.com/amplab/tachyon/blob/master/examples/src/main/java/tachyon/examples/BasicOperations.java).

5.  [Building Tachyon Master Branch](Building-Tachyon-Master-Branch.html).

6.  Fork the repository, add unit tests or javadoc for one or two files in the
following list, and then submit a pull request. You are also welcome to address
issues in our [JIRA](https://tachyon.atlassian.net/browse/TACHYON).
Here are a list of
[tasks](https://tachyon.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20NewContributor%20AND%20status%20%3D%20OPEN)
for New Contributors. Please limit 2 tasks per New Contributor.
Afterwards, try some Beginner/Intermediate tasks, or ask in the
[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/tachyon-users).
For a tutorial, see the GitHub guides on
[forking a repo](https://help.github.com/articles/fork-a-repo) and
[sending a pull request](https://help.github.com/articles/using-pull-requests).

### IDE

You can generate an Eclipse configuration file by running:

    $ mvn clean install -DskipTests
    $ mvn clean -PcontractTest -DskipTests eclipse:eclipse -DdownloadJavadocs=true -DdownloadSources=true

Then import the folder into Eclipse.

You may also have to add the classpath variable M2_REPO by running:

    $ mvn -Declipse.workspace="your Eclipse Workspace" eclipse:configure-workspace

If you are using IntelliJ IDEA, you may need to change the Maven profile to 'contractTest' in order to avoid import errors.
You can do this by going to

    View > Tool Windows > Maven Projects

### Presentations:

-   Strata and Hadoop World 2014 (October, 2014) [pdf](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-10-16-Strata.pdf) [pptx](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-10-16-Strata.pptx)
-   Spark Summit 2014 (July, 2014) [pdf](http://goo.gl/DKrE4M)
-   Strata and Hadoop World 2013 (October, 2013) [pdf](http://goo.gl/AHgz0E)

### Readings

-   [Tachyon: Reliable, Memory Speed Storage for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2014_socc_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Scott Shenker, Ion Stoica, *SOCC 2014*.
-   [Reliable, Memory Speed Storage for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2014_EECS_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Scott Shenker, Ion Stoica, *UC Berkeley EECS 2014*.
-   [Tachyon: Memory Throughput I/O for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2013_ladis_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Eric Baldeschwieler, Scott Shenker, Ion Stoica, *LADIS 2013*.
