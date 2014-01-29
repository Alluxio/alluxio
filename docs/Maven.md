---
layout: global
title: Maven
---

# Mac OSX

Mac OS X comes with Apache Maven 3 built in, and can be located at `/usr/share/maven`

1.  Maven Verification In terminal, issue the command `mvn -version`.

# Linux

1.  Download [Maven Binary](http://maven.apache.org/download.cgi)
2.  Extract the distribution archive, i.e.
    `apache-maven-3.0.5-bin.tar.gz` to the directory you wish to install
    Maven 3.0.5. These instructions assume you chose
    `/usr/local/apache-maven`. The subdirectory `apache-maven-3.0.5`
    will be created from the archive.
3.  In a command terminal, add the `M2_HOME` environment variable, e.g.
    `export M2_HOME=/usr/local/apache-maven/apache-maven-3.0.5`.
4.  Add the `M2` environment variable, e.g. `export M2=$M2_HOME/bin`.
5.  Add `M2` environment variable to your path, e.g.
    `export PATH=$M2:$PATH`.
6.  Make sure that `JAVA_HOME` is set to the location of your JDK, e.g.
    `export JAVA_HOME=/usr/java/jdk1.5.0_02` and that `$JAVA_HOME/bin`
    is in your `PATH` environment variable.
7.  Run `mvn --version` to verify that it is correctly installed.


