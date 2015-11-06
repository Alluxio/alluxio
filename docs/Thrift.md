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

```bash
$ ruby -e "$(curl -fsSL https://raw.github.com/Homebrew/homebrew/go/install)"
$ brew doctor
```

Use Homebrew to install autoconf, automake, libtool and pkg-config:

```bash
$ brew install autoconf automake libtool pkg-config libevent
```

Use Homebrew to install [Boost](http://www.boost.org/)

```bash
$ brew install boost
```

Install Thrift

```bash
$ brew install thrift
```

## MacPorts

This section explains how to install Apache Thrift via [MacPorts](http://macports.org).

If you use [MacPorts](http://macports.org), the following instructions may help.

Install MacPorts from [sourceforge](http://sourceforge.net/projects/macports/).

Update Port itself:

```bash
$ sudo port selfupdate
```

Use Port to install flex, bison, autoconf, automake, libtool and pkgconfig:

```bash
$ sudo port install flex, bison, autoconf automake libtool pkgconfig libevent
```

Use Port to install [Boost](http://www.boost.org/)

```bash
$ sudo port install boost
```

Try to use Port to install Thrift:

```bash
$ sudo port install thrift
```

The last command MAY fail, according to this [issue](https://trac.macports.org/ticket/41172). In
this case, we recommend building Thrift 0.9.2 from source (Assuming you use MacPort's default
directory `/opt/local`):

```bash
$ ./configure --prefix=/opt/local/ --with-boost=/opt/local/lib --with-libevent=/opt/local/lib CXXFLAGS="-I/usr/include/4.2.1 -L/opt/local/lib"
$ make
$ make install
```

You may change CXXFLAGS. Here we include `/usr/include/4.2.1` for `std::tr1` on Mavericks and
`/opt/local/lib` for libraries installed by port. Without the `-I`, the installation may fail with 
`tr1/functional not found`. Without the `-L`, the installation may fail during linking.

# Linux

[Reference](http://thrift.apache.org/docs/install/)

## Debian/Ubuntu

The following command installs all the required tools and libraries to
build and install the Apache Thrift compiler on a Debian/Ubuntu Linux
based system.

```bash
$ sudo apt-get install libboost-dev libboost-test-dev libboost-program-options-dev libevent-dev automake libtool flex bison pkg-config g++ libssl-dev ant python-dev
```

or

```bash
$ sudo yum install automake libtool flex bison pkgconfig gcc-c++ boost-devel libevent-devel zlib-devel python-devel ruby-devel ant python-dev
```

Then install the Java JDK of your choice. Type javac to see a list of available packages,
pick the one you prefer, and use your package manager to install it.

Debian Lenny Users need some packages from backports:

```bash
$ sudo apt-get -t lenny-backports install automake libboost-test-dev
```

[Build Thrift](http://thrift.apache.org/docs/BuildingFromSource):

```bash
$ ./configure --with-boost=/usr/local
$ make
$ make install
```

## CentOS

The following steps can be used to setup a CentOS 6.4 system.

Install dependencies:

```bash
$ sudo yum install automake libtool flex bison pkgconfig gcc-c++ make
```

Upgrade autoconf to 2.69 (yum will most likely pull 2.63 which won't work with Apache Thrift):

```bash
$ sudo yum install 'ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/monkeyiq:/centos6updates/CentOS_CentOS-6/noarch/autoconf-2.69-12.2.noarch.rpm'
```

Download and install Apache Thrift source:

```bash
$ wget 'https://github.com/apache/thrift/archive/0.9.2.tar.gz'
$ tar zxvf 0.9.2
$ cd thrift-0.9.2/
$ ./bootstrap.sh
$ ./configure --enable-libs=no
$ make
$ sudo make install
```

# Generate Java files from Thrift

Tachyon defines a its RPC services using the thrift file located in in:

    ./common/src/thrift/tachyon.thrift

and generates Java files from it into:

    ./common/src/main/java/tachyon/thrift/

To regenerate the java files if the thrift file is modified, you can run:

```bash
    $ ./bin/tachyon thriftGen
```
