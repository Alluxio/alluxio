---
layout: global
title: Thrift
---

# Mac OSX

Before you can get [Apache Thift](http://thrift.apache.org) installed, you will first need to setup
command-line support.  To do this, you will need to:

Install Xcode from the Mac App Store

Launch Xcode, open the Preferences, select Downloads, and install
    the “Command Line Tools for Xcode” component.

## Homebrew

This section explains install Apache Thrift via [Homebrew](http://brew.sh/).

First, install [Homebrew](http://brew.sh/)

Here are the commands for Homebrew installation:

    ruby -e "$(curl -fsSL https://raw.github.com/Homebrew/homebrew/go/install)"
    brew doctor

Use Homebrew to install autoconf, automake, libtool and pkg-config:

    brew install autoconf automake libtool pkg-config libevent

Use Homebrew to install [Boost](http://www.boost.org/)

    brew install boost

Install Thrift

    brew install thrift

## MacPorts

This section explains install Apache Thrift via [MacPorts](http://macports.org).

If you use [MacPorts](http://macports.org), the following instruction may help

Install MacPorts from [sourceforge](http://sourceforge.net/projects/macports/)

Update Port itself:

    sudo port selfupdate

Use Port to install flex, bison, autoconf, automake, libtool and pkgconfig:

    sudo port install flex, bison, autoconf automake libtool pkgconfig libevent

Use Port to install [Boost](http://www.boost.org/)

    sudo port install boost

Try to use Port to install Thrift:

    sudo port install thrift

The last command MAY fail, according to this [issue](https://trac.macports.org/ticket/41172). In
this case, we recommend building Thrift 0.9.1 from source (Assuming you use MacPort's default
directory /opt/local):

    ./configure --prefix=/opt/local/ --with-boost=/opt/local/lib --with-libevent=/opt/local/lib CXXFLAGS="-I/usr/include/4.2.1 -L/opt/local/lib"
    make
    make install

You may change CXXFLAGS. Here we include /usr/include/4.2.1 for std::tr1 on Mavericks and
/opt/local/lib for libraries installed by port. Without the -I, you may fail with "tr1/functional
not found". Without the -L, you may fail during linking.

# Linux

[Reference](http://thrift.apache.org/docs/install/)

## Debian/Ubuntu

The following command install all the required tools and libraries to
build and install the Apache Thrift compiler on a Debian/Ubuntu Linux
based system.

    sudo apt-get install libboost-dev libboost-test-dev libboost-program-options-dev libevent-dev automake libtool flex bison pkg-config g++ libssl-dev ant python-dev

or

    sudo yum install automake libtool flex bison pkgconfig gcc-c++ boost-devel libevent-devel zlib-devel python-devel ruby-devel ant python-dev

Then install the Java JDK of your choice. Type javac to see a list of
available packages, pick the one you prefer, and apt-get install it.

Debian Lenny Users need some packages from backports

    sudo apt-get -t lenny-backports install automake libboost-test-dev

[Build The Thrift](http://thrift.apache.org/docs/BuildingFromSource)

    ./configure --with-boost=/usr/local
    make
    make install

## CentOS

The following steps can be used to setup a CentOS 6.4 system.

Install basic utils

    sudo yum install automake libtool flex bison pkgconfig gcc-c++ make

Upgraded autoconf to 2.69 (yum will most likely pull 2.63 which won't work with Apache Thrift)

    sudo yum install 'ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/monkeyiq:/centos6updates/CentOS_CentOS-6/noarch/autoconf-2.69-12.2.noarch.rpm'

Download and install Apache Thrift source

    wget 'https://github.com/apache/thrift/archive/0.9.1.tar.gz'
    tar zxvf 0.9.1
    cd thrift-0.9.1/
    ./bootstrap.sh
    ./configure --enable-libs=no
    make
    sudo make install

# Generate Java files from Thrift

Tachyon defines a .thrift in:

    ./common/src/thrift/tachyon.thrift

And generates java files from it in:

    ./common/src/main/java/tachyon/thrift/

To generate the java files on your own, type:

    ./bin/tachyon thriftGen

