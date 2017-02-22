---
layout: global
title: Maven
---

* Table of Contents
{:toc}

## Mac OS X

Before OS X Mavericks, Mac OS X comes with Apache Maven 3 built in, and can be located at
`/usr/share/maven`

Since Mac OS X Mavericks, Maven was removed and hence needs to be installed manually.

1.  Download [Maven Binary](http://maven.apache.org/download.cgi)
2.  Extract the distribution archive, i.e. `apache-maven-<version.number>-bin.tar.gz` to the directory you wish to install `Maven <version.number>`. These instructions assume you chose `/System/Library/Apache-Maven`. The subdirectory `apache-maven-<version.number>` will be created from the archive.
3.  In a command terminal, add the `M2_HOME` environment variable, e.g. `export M2_HOME=/System/Library/Apache-Maven/apache-maven-<version.number>`.
4.  Add the `M2` environment variable, e.g. `export M2=$M2_HOME/bin`.
5.  Add `M2` environment variable to your path, e.g. `export PATH=$M2:$PATH`.
6.  Make sure that `JAVA_HOME` is set to the location of your JDK, e.g. `export JAVA_HOME=/System/Library/Java/JavaVirtualMachines/1.6.0.jdk` and that `$JAVA_HOME/bin` is in your `PATH` environment variable.
7.  Run `mvn --version` to verify that it is correctly installed.

Alternatively, Maven can be installed through [Homebrew](http://brew.sh/) using `brew install maven`.

## Linux

1.  Download [Maven Binary](http://maven.apache.org/download.cgi)
2.  Extract the distribution archive, i.e. `apache-maven-<version.number>-bin.tar.gz` to the directory you wish to install `Maven <version.number>`. These instructions assume you chose `/usr/local/apache-maven`. The subdirectory `apache-maven-<version.number>` will be created from the archive.
3.  In a command terminal, add the `M2_HOME` environment variable, e.g. `export M2_HOME=/usr/local/apache-maven/apache-maven-<version.number>`.
4.  Add the `M2` environment variable, e.g. `export M2=$M2_HOME/bin`.
5.  Add `M2` environment variable to your path, e.g. `export PATH=$M2:$PATH`.
6.  Make sure that `JAVA_HOME` is set to the location of your JDK, e.g. `export JAVA_HOME=/usr/java/jdk1.6.0` and that `$JAVA_HOME/bin` is in your `PATH` environment variable.
7.  Run `mvn --version` to verify that it is correctly installed.

Alternatively, Maven can be installed through a package manager (e.g. `sudo apt-get install maven`).
