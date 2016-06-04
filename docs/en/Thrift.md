---
layout: global
title: Thrift
---

# Mac OS X

Before you can get [Apache Thift](http://thrift.apache.org) installed, you will first need to setup
command-line support.  To do this, you will need to:

Install Xcode from the Mac App Store

Launch Xcode, open the Preferences, select Downloads, and install
    the “Command Line Tools for Xcode” component.

## Homebrew

This section explains how to install Apache Thrift via [Homebrew](http://brew.sh/).

First, install [Homebrew](http://brew.sh/).

Here are the commands for Homebrew installation:

{% include Thrift/install-Homebrew.md %}

Use Homebrew to install autoconf, automake, libtool and pkg-config:

{% include Thrift/install-dependency-brew.md %}

Use Homebrew to install [Boost](http://www.boost.org/)

{% include Thrift/install-Boost-brew.md %}

Install Thrift

{% include Thrift/install-Thrift-brew.md %}

## MacPorts

This section explains how to install Apache Thrift via [MacPorts](http://macports.org).

If you use [MacPorts](http://macports.org), the following instructions may help.

Install MacPorts from [sourceforge](http://sourceforge.net/projects/macports/).

Update Port itself:

{% include Thrift/update-port.md %}

Use Port to install flex, bison, autoconf, automake, libtool and pkgconfig:

{% include Thrift/install-dependency-port.md %}

Use Port to install [Boost](http://www.boost.org/)

{% include Thrift/install-Boost-port.md %}

Try to use Port to install Thrift:

{% include Thrift/install-Thrift-port.md %}

The last command MAY fail, according to this [issue](https://trac.macports.org/ticket/41172). In
this case, we recommend building Thrift 0.9.2 from source (Assuming you use MacPort's default
directory `/opt/local`):

{% include Thrift/build-Thrift-port.md %}

You may change CXXFLAGS. Here we include `/usr/include/4.2.1` for `std::tr1` on Mavericks and
`/opt/local/lib` for libraries installed by port. Without the `-I`, the installation may fail with
`tr1/functional not found`. Without the `-L`, the installation may fail during linking.

# Linux

[Reference](http://thrift.apache.org/docs/install/)

## Debian/Ubuntu

The following command installs all the required tools and libraries to
build and install the Apache Thrift compiler on a Debian/Ubuntu Linux
based system.

{% include Thrift/install-dependency-apt.md %}

or

{% include Thrift/install-dependency-yum.md %}

Then install the Java JDK of your choice. Type javac to see a list of available packages,
pick the one you prefer, and use your package manager to install it.

Debian Lenny Users need some packages from backports:

{% include Thrift/install-lenny-backports.md %}

[Build Thrift](http://thrift.apache.org/docs/BuildingFromSource):

{% include Thrift/build-Thrift-ubuntu.md %}

## CentOS

The following steps can be used to setup a CentOS 6.4 system.

Install dependencies:

{% include Thrift/install-dependency-centos.md %}

Upgrade autoconf to 2.69 (yum will most likely pull 2.63 which won't work with Apache Thrift):

{% include Thrift/update-autoconf.md %}

Download and install Apache Thrift source:

{% include Thrift/download-install-Thrift.md %}

# Generate Java files from Thrift

Alluxio defines a its RPC services using the thrift file located in in:

    ./common/src/thrift/alluxio.thrift

and generates Java files from it into:

    ./common/src/main/java/alluxio/thrift/

To regenerate the java files if the thrift file is modified, you can run:

{% include Thrift/regenerate.md %}
