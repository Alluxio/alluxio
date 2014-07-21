---
layout: global
title: Thrift
---

# Mac OSX

1.  Grab the Thrift 0.9.0 distribution from
    [http://archive.apache.org/dist/thrift/0.9.0/](http://archive.apache.org/dist/thrift/0.9.0/)
2.  Install Xcode from the Mac App Store
3.  Launch Xcode, open the Preferences, select Downloads, and install
    the “Command Line Tools for Xcode” component.
4.  Install [Homebrew](http://brew.sh/), if you use MacPorts, skip to the corresponding instructions.

Here are the commands for Homebrew installation:

    ruby -e "$(curl -fsSL https://raw.github.com/Homebrew/homebrew/go/install)"
    brew doctor

Use Homebrew to install autoconf, automake, libtool and pkg-config:

    brew install autoconf automake libtool pkg-config libevent

Use Homebrew to install [Boost](http://www.boost.org/)

    brew install boost

Now build Thrift 0.9.0:

    ./configure --prefix=/usr/local/ --with-boost=/usr/local --with-libevent=/usr/local
    make
    make install

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
this case, we recommend building Thrift 0.9.0 from source (Assuming you use MacPort's default
directory /opt/local):

    ./configure --prefix=/opt/local/ --with-boost=/opt/local/lib --with-libevent=/opt/local/lib CXXFLAGS="-I/usr/include/4.2.1 -L/opt/local/lib"
    make
    make install

You may change CXXFLAGS. Here we include /usr/include/4.2.1 for std::tr1 on Mavericks and
/opt/local/lib for libraries installed by port. Without the -I, you may fail with "tr1/functional
not found". Without the -L, you may fail during linking.

# Linux

[Reference](http://thrift.apache.org/docs/install/)

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

# Generate Java files from Thrift

Tachyon defines a .thrift in:

    ./core/src/thrift/tachyon.thrift

And generates java files from it in:

    ./core/src/main/java/tachyon/thrift/

To generate the java files on your own, type:

    ./bin/tachyon thriftGen

