---
layout: global
title: Building Tachyon Master Branch
nickname: Building Master Branch
group: Dev Resources
---

* Table of Contents
{:toc}

This guide describes how to compile Tachyon from the beginning.

The prerequisite for this guide is that you have [Java 7 (or above)](Java-Setup.html),
[Maven](Maven.html), and [Thrift 0.9.2](Thrift.html) (Optional) installed.

Checkout the Tachyon master branch from Github and package:

{% include Building-Tachyon-Master-Branch/checkout.md %}

If you are seeing `java.lang.OutOfMemoryError: Java heap space`, please execute:

{% include Building-Tachyon-Master-Branch/OutOfMemoryError.md %}

If you want to build a particular version of Tachyon, for example {{site.TACHYON_RELEASED_VERSION}},
please do `git checkout v{{site.TACHYON_RELEASED_VERSION}}` after `cd tachyon`.

The Maven build system fetches its dependencies, compiles source code, runs unit tests, and packages
the system. If this is the first time you are building the project, it can take a while to download
all the dependencies. Subsequent builds, however, will be much faster.

Once Tachyon is built, you can start it with:

{% include Building-Tachyon-Master-Branch/tachyon-start.md %}

To verify that Tachyon is running, you can visit [http://localhost:19999](http://localhost:19999) or
check the log in the `tachyon/logs` directory. You can also run a simple program:

{% include Building-Tachyon-Master-Branch/tachyon-runTests.md %}

You should be able to see results similar to the following:

{% include Building-Tachyon-Master-Branch/test-result.md %}

You can also stop the system by using:

{% include Building-Tachyon-Master-Branch/tachyon-stop.md %}

# Unit Tests

To run all unit tests:

{% include Building-Tachyon-Master-Branch/unit-tests.md %}

To run all the unit tests with under storage other than local filesystem:

{% include Building-Tachyon-Master-Branch/under-storage.md %}

Currently supported values for `<under-storage-profile>` are:

{% include Building-Tachyon-Master-Branch/supported-values.md %}

To have the logs output to STDOUT, append the following to the `mvn` command

{% include Building-Tachyon-Master-Branch/STDOUT.md %}

# Distro Support

To build Tachyon against one of the different distros of hadoop, you only need to change the
`hadoop.version`.

## Apache

All main builds are from Apache so all Apache releases can be used directly

{% include Building-Tachyon-Master-Branch/Apache.md %}

## Cloudera

To build against Cloudera's releases, just use a version like `$apacheRelease-cdh$cdhRelease`

{% include Building-Tachyon-Master-Branch/Cloudera.md %}

## MapR

To build against a MapR release

{% include Building-Tachyon-Master-Branch/MapR.md %}

## Pivotal

To build against a Pivotal release, just use a version like `$apacheRelease-gphd-$pivotalRelease`

{% include Building-Tachyon-Master-Branch/Pivotal.md %}

## Hortonworks

To build against a Hortonworks release, just use a version like `$apacheRelease.$hortonRelease`

{% include Building-Tachyon-Master-Branch/Hortonworks.md %}

# System Settings

Sometimes you will need to play with a few system settings in order to have the unit tests pass
locally.  A common setting that may need to be set is ulimit.

## Mac

In order to increase the number of files and processes allowed, run the following

{% include Building-Tachyon-Master-Branch/increase-number.md %}

It is also recommended to exclude your local clone of Tachyon from Spotlight indexing. Otherwise,
your Mac may hang constantly trying to re-index the file system during the unit tests.  To do this,
go to `System Preferences > Spotlight > Privacy`, click the `+` button, browse to the directory
containing your local clone of Tachyon, and click `Choose` to add it to the exclusions list.
