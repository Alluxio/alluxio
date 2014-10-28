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

6.  Fork the repository, add unit tests or javadoc for one or two files in the
following list, and then submit a pull request. You are also welcome to address
issues in our [JIRA](https://tachyon.atlassian.net/browse/TACHYON).
Here are a list of
[tasks](https://tachyon.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20Beginner)
for beginners. For a tutorial, see the GitHub guides on
[forking a repo](https://help.github.com/articles/fork-a-repo) and
[sending a pull request](https://help.github.com/articles/using-pull-requests).

### Testing

-   Run all unit tests with ``mvn test`` (will use the local filesystem as the
under filesystem) and ``mvn -Dtest.profile=hdfs -Dhadoop.version=2.4.0 test``
(will use HDFS 2.4.0 as the under filesystem)

-   In GlusterFS environment, also run GlusterFS unit tests: ``mvn
-Dhadoop.version=2.3.0 -Dtest.profile=glusterfs
-Dtachyon.underfs.glusterfs.mounts=/vol
-Dtachyon.underfs.glusterfs.volumes=testvol test`` (use GlusterFS as under
filesystem, where /vol is a valid GlusterFS mount point) and ``mvn
-Dhadoop.version=2.3.0 -Dtest.profile=glusterfs test`` (use localfs as under
filesystem)

-   Run a single unit test: ``mvn -Dtest=TestCircle#mytest test`` ; e.g.
``mvn -Dtest=TachyonFSTest#createFileTest test`` ;

-   To quickly test the working of some APIs in an interactive manner, you may
leverage the Scala shell, as discussed in this
[blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding).

-   Run tests with a different Hadoop version: ``mvn -Dhadoop.version=2.2.0 clean test``

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
    -  If you use IntelliJ IDEA: you can either use our formatter with the help from
       [Eclipse Code Formatter](https://github.com/krasa/EclipseCodeFormatter#instructions)
       or use [Eclipse Code Formatter Plugin](http://plugins.jetbrains.com/plugin/6546) in IntelliJ
       IDEA
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
   
        mvn checkstyle:checkstyle

### FindBugs

Before submitting the pull-request, run the latest code against
[FindBugs](http://findbugs.sourceforge.net/) to verify no new warnings are introduced.

    mvn compile findbugs:findbugs findbugs:gui

### IDE

You can generate an Eclipse configuration file by running:

    mvn clean test -Dtest.profile=hdfs -DskipTests eclipse:eclipse

Then import the folder into Eclipse.

You may also have to add the classpath variable M2_REPO by running:

    mvn -Declipse.workspace="your Eclipse Workspace" eclipse:configure-workspace

### Submitting Code

-   We encourage you to break your work into small, single-purpose patches if possible. It’s much
    harder to merge in a large change with a lot of disjoint features.

-   Make sure that any methods you add maintain the alphabetical ordering of method names in each file.

-   Submit the patch as a GitHub pull request. For a tutorial, see the GitHub guides on
    [forking a repo](https://help.github.com/articles/fork-a-repo) and
    [sending a pull request](https://help.github.com/articles/using-pull-requests).

-   Make sure that your code passes all unit tests: ``mvn test`` and ``mvn -Dintegration test``

### Presentations:

-   Spark Summit 2014 (July, 2014) [pdf](http://goo.gl/DKrE4M)
-   Strata and Hadoop World 2013 (October, 2013) [pdf](http://goo.gl/AHgz0E)

### Readings

-   [Reliable, Memory Speed Storage for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2014_EECS_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Scott Shenker, Ion Stoica, *UC Berkeley EECS 2014*.
-   [Tachyon: Memory Throughput I/O for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2013_ladis_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Eric Baldeschwieler, Scott Shenker, Ion Stoica, *LADIS 2013*.
