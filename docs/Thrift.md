---
layout: global
title: Thrift
---

# Mac OSX

1.  Grab the latest Thrift distribution (we only support 0.7.0) from
    [http://archive.apache.org/dist/thrift/0.7.0/](http://archive.apache.org/dist/thrift/0.7.0/)
2.  Install Xcode from the Mac App Store
3.  Launch Xcode, open the Preferences, select Downloads, and install
    the “Command Line Tools for Xcode” component.
4.  Install [Homebrew](http://mxcl.github.io/homebrew/)

Here the commands for Homebrew installation:

    ruby -e "$(curl -fsSkL raw.github.com/mxcl/homebrew/go)"
    brew doctor

Use Homebrew to install autoconf, automake, libtool and pkg-config:

    brew install autoconf automake libtool pkg-config libevent

Use Homebrew to install [Boost](http://www.boost.org/)

    brew install boost

Now build Thrift 0.7.0:

    ./configure --prefix=/usr/local/ --with-boost=/usr/local --with-libevent=/usr/local
    make
    make install

# Linux

[Reference](http://thrift.apache.org/docs/install/)

The following command install all the required tools and libraries to
build and install the Apache Thrift compiler on a Debian/Ubuntu Linux
based system.

    sudo apt-get install libboost-dev libboost-test-dev libboost-program-options-dev libevent-dev automake libtool flex bison pkg-config g++ libssl-dev ant python-dev

or

    sudo yum install automake libtool flex bison pkgconfig gcc-c++ boost-devel libevent-devel zlib-devel python-devel ruby-devel ant python-dev

Then install the Java JDK of your choice. Type javac to see a list of
available packages, pick the one you prefer and apt-get install it.

Debian Lenny Users need some packages from backports

    sudo apt-get -t lenny-backports install automake libboost-test-dev

[Build The Thrift](http://thrift.apache.org/docs/BuildingFromSource/)

    ./configure --with-boost=/usr/local
    make
    make install

