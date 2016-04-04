---
layout: global
title: Thrift
---

# Mac OS X

在安装[Apache Thift](http://thrift.apache.org)之前，你首先需要有命令行支持。为此，你需要：

从Mac App Store中安装Xcode。

启动Xcode，打开Preferences，选择Downloads，安装“Command Line Tools for Xcode”组件。

## Homebrew

这一部分介绍如何通过[Homebrew](http://brew.sh/)安装Apache Thrift。

首先，安装[Homebrew](http://brew.sh/)。

下面是Homebrew的安装命令：

{% include Thrift/install-Homebrew.md %}

使用Homebrew安装autoconf、automake、libtool和pkg-config：

{% include Thrift/install-dependency-brew.md %}

使用Homebrew安装[Boost](http://www.boost.org/)

{% include Thrift/install-Boost-brew.md %}

安装Thrift

{% include Thrift/install-Thrift-brew.md %}

## MacPorts

这一部分介绍如何用[MacPorts](http://macports.org)安装Apache Thrift。

如果你使用[MacPorts](http://macports.org)，下面的介绍会有所帮助。

从[sourceforge](http://sourceforge.net/projects/macports/)安装MacPorts。

自动更新Port：

{% include Thrift/update-port.md %}

使用Port来安装flex、bison、autoconf、automake、libtool和pkgconfig：

{% include Thrift/install-dependency-port.md %}

使用Port安装[Boost](http://www.boost.org/)

{% include Thrift/install-Boost-port.md %}

使用Port安装Thrift:

{% include Thrift/install-Thrift-port.md %}

最新的命令也许不会生效，请参考该[问题](https://trac.macports.org/ticket/41172)。这种情况下，我们建议从源码中构建Thrift 0.9.2（假设你使用的是MacPort的默认目录`/opt/local`）：

{% include Thrift/build-Thrift-port.md %}

你可以更改CXXFLAGS。这里我们是通过port，在Mavericks上为`std::tr1`添加`/usr/include/4.2.1`，并将`/opt/local/lib`添加到库中。如果没有`-I`，安装可能因为`tr1/functional not found`失败。没有`-L`则可能会在连接过程中失败。

# Linux

[参考](http://thrift.apache.org/docs/install/)

## Debian/Ubuntu

下面的命令会安装所有需要的工具包和软件库，以用于在Debian/Ubuntu系统上构建和安装Apache Thrift编译器。

{% include Thrift/install-dependency-apt.md %}

或

{% include Thrift/install-dependency-yum.md %}

然后按照你的需求安装Java JDK。输入javac来查看可用的包，选择一个你倾向使用的，然后使用包管理器来安装。

Debian Lenny用户需要一些来自backports的包：

{% include Thrift/install-lenny-backports.md %}

[构建Thrift](http://thrift.apache.org/docs/BuildingFromSource):

{% include Thrift/build-Thrift-ubuntu.md %}

## CentOS

下面的步骤用于在CentOS 6.4系统上安装。

安装依赖：

{% include Thrift/install-dependency-centos.md %}

将autoconf更新到2.69（yum通常会下载2.63，该版本不适用于Apache Thrift）：

{% include Thrift/update-autoconf.md %}

下载并安装Apache Thrift源码：

{% include Thrift/download-install-Thrift.md %}

# 利用Thrift生成Java文件

Alluxio利用thrift文件定义了其RPC服务，该文件位于：

    ./common/src/thrift/alluxio.thrift

并利用thrift文件生成相应的Java文件，该Java文件位于：

    ./common/src/main/java/alluxio/thrift/

如果thrift文件被修改了，要重新生成Java文件，你可以运行：

{% include Thrift/regenerate.md %}
