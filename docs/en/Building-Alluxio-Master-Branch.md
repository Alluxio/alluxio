---
layout: global
title: Building Alluxio Master Branch
nickname: Building Master Branch
group: Resources
---

* Table of Contents
{:toc}

This guide describes how to compile Alluxio from the beginning.

The prerequisite for this guide is that you have [Java 7 (or above)](Java-Setup.html),
[Maven](Maven.html), and [Thrift 0.9.2](Thrift.html) (Optional) installed.

Checkout the Alluxio master branch from Github and package:

{% include Building-Alluxio-Master-Branch/checkout.md %}

If you are seeing `java.lang.OutOfMemoryError: Java heap space`, please execute:

{% include Building-Alluxio-Master-Branch/OutOfMemoryError.md %}

If you want to build a particular version of Alluxio, for example {{site.TACHYON_RELEASED_VERSION}},
please do `git checkout v{{site.TACHYON_RELEASED_VERSION}}` after `cd alluxio`.

The Maven build system fetches its dependencies, compiles source code, runs unit tests, and packages
the system. If this is the first time you are building the project, it can take a while to download
all the dependencies. Subsequent builds, however, will be much faster.

Once Alluxio is built, you can start it with:

{% include Building-Alluxio-Master-Branch/alluxio-start.md %}

To verify that Alluxio is running, you can visit [http://localhost:19999](http://localhost:19999) or
check the log in the `alluxio/logs` directory. You can also run a simple program:

{% include Building-Alluxio-Master-Branch/alluxio-runTests.md %}

You should be able to see results similar to the following:

{% include Building-Alluxio-Master-Branch/test-result.md %}

You can also stop the system by using:

{% include Building-Alluxio-Master-Branch/alluxio-stop.md %}

# Unit Tests

To run all unit tests:

{% include Building-Alluxio-Master-Branch/unit-tests.md %}

To run all the unit tests with under storage other than local filesystem:

{% include Building-Alluxio-Master-Branch/under-storage.md %}

Currently supported values for `<under-storage-profile>` are:

{% include Building-Alluxio-Master-Branch/supported-values.md %}

To have the logs output to STDOUT, append the following to the `mvn` command

{% include Building-Alluxio-Master-Branch/STDOUT.md %}

# Distro Support

To build Alluxio against one of the different distros of hadoop, you only need to change the
`hadoop.version`.

## Apache

All main builds are from Apache so all Apache releases can be used directly

{% include Building-Alluxio-Master-Branch/Apache.md %}

## Cloudera

To build against Cloudera's releases, just use a version like `$apacheRelease-cdh$cdhRelease`

{% include Building-Alluxio-Master-Branch/Cloudera.md %}

## MapR

To build against a MapR release

{% include Building-Alluxio-Master-Branch/MapR.md %}

## Pivotal

To build against a Pivotal release, just use a version like `$apacheRelease-gphd-$pivotalRelease`

{% include Building-Alluxio-Master-Branch/Pivotal.md %}

## Hortonworks

To build against a Hortonworks release, just use a version like `$apacheRelease.$hortonRelease`

{% include Building-Alluxio-Master-Branch/Hortonworks.md %}

# System Settings

Sometimes you will need to play with a few system settings in order to have the unit tests pass
locally.  A common setting that may need to be set is ulimit.

## Mac

In order to increase the number of files and processes allowed, run the following

{% include Building-Alluxio-Master-Branch/increase-number.md %}

It is also recommended to exclude your local clone of Alluxio from Spotlight indexing. Otherwise,
your Mac may hang constantly trying to re-index the file system during the unit tests.  To do this,
go to `System Preferences > Spotlight > Privacy`, click the `+` button, browse to the directory
containing your local clone of Alluxio, and click `Choose` to add it to the exclusions list.
