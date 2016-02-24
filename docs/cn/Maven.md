---
layout: global
title: Maven
---

# Mac OS X

在OS X Mavericks之前，Mac OS X自带了Apache Maven 3,安装在`/usr/share/maven`目录下。

从Mac OS X Mavericks开始，Maven便被移除了，因此需要手动安装。

1.  下载[Maven二进制文件](http://maven.apache.org/download.cgi)
2.  将`apache-maven-<version.number>-bin.tar.gz`压缩文件解压到你需安装`Maven <version.number>`的目录下，假定你选择的目录为`/System/Library/Apache-Maven`，一个名为`apache-maven-<version.number>`的子目录会自动创建。
3.  在命令行终端，添加`M2_HOME`环境变量，例如，`export M2_HOME=/System/Library/Apache-Maven/apache-maven-<version.number>`。
4.  添加`M2`环境变量，例如，`export M2=$M2_HOME/bin`。
5.  将`M2`添加到你的path路径中，例如，`export PATH=$M2:$PATH`。
6.  确保`JAVA_HOME`已被设置为JDK目录，例如，`export JAVA_HOME=/System/Library/Java/JavaVirtualMachines/1.6.0.jdk`，并且`$JAVA_HOME/bin`在`PATH`环境变量中。
7.  运行`mvn --version`确认是否已正确安装。

另外可以选择通过[Homebrew](http://brew.sh/)使用`brew install maven`命令安装Maven。

# Linux

1.  下载[Maven二进制文件](http://maven.apache.org/download.cgi)
2.  将`apache-maven-<version.number>-bin.tar.gz`压缩文件解压到你需安装`Maven <version.number>`的目录下，假定你选择的目录为`/usr/local/apache-maven`，一个名为`apache-maven-<version.number>`的子目录会自动创建。
3.  在命令行终端，添加`M2_HOME`环境变量，例如，`export M2_HOME=/usr/local/apache-maven/apache-maven-<version.number>`。
4.  添加`M2`环境变量，例如，`export M2=$M2_HOME/bin`。
5.  将`M2`添加到你的path路径中，例如，`export PATH=$M2:$PATH`。
6.  确保`JAVA_HOME`已被设置为JDK目录，例如，`export JAVA_HOME=/usr/java/jdk1.6.0`，并且`$JAVA_HOME/bin`在`PATH`环境变量中。
7.  运行`mvn --version`确认是否已正确安装。

另外可以选择通过安装包管理器安装Maven（例如，`sudo apt-get install maven`）。
