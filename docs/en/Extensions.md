---
layout: global
group: Features
title: Under Storage Extensions
---

* Table of Contents
{:toc}

Alluxio can be extended with the addition of under storage modules at runtime. Under storage
extensions can be built as JARs and included at a specific location to be picked up by core Alluxio
without the need to restart. This page describes the internals of how extensions in Alluxio work,
and provides instructions for building and installing an extension.

## How it Works

### Service Discovery

Alluxio servers use Java
[ServiceLoader](https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html) to discover
implementations of the under storage API. Specifically providers include implementations of the
`alluxio.underfs.UnderFileSystemFactory` interface. A common way to advertise an implementation is
by including a text file
`src/main/resources/META_INF/services/alluxio.underfs.UnderFileSystemFactory` with a single line
pointing to the class implementing the said interface.

### Dependency Management

Implementors are required to include transitive dependencies of the extension in the built JAR.
Simply including all dependencies of an extension project on the server classpath would have created
potential for hard to debug dependency conflicts. To avoid the said issues, the ServiceLoader in
Alluxio uses a custom (per extension)
[ClassLoader](https://docs.oracle.com/javase/7/docs/api/java/lang/ClassLoader.html) for classpath
isolation. The ClassLoader defines how a class is transformed from raw bytes in a `.class` file to
an instance of `java.lang.Class` that can be used in the JVM process. The custom class loader
ensures that the bytes for an extension (and its dependencies) are isolated and do not cause
conflicts with other extensions and core Alluxio.

## Creating an Under Storage Extension

Building a new under storage connector involves: - Implementing the required under storage interface
and declaring the service implementation - Bundling up the implementation and depencies in an uber
JAR

A reference implementation can be found in the [alluxio-extensions](https://github.com/Alluxio
/alluxio-extensions/tree/master/underfs/s3n) repository.

### Implementing the Under Storage Interface

Projects must depend on `org.alluxio:alluxio-core-common` and implement the interface
`alluxio.underfs.UnderFileSystemFactory`. The factory is the entry point for instantiating an under
storage connector. The interface `alluxio.underfs.UnderFileSystem` determines the contract between
core Alluxio and any under storage providers. Implementors can choose to extend
`alluxio.underfs.BaseUnderFileSystem` or `alluxio.underfs.ObjectUnderFileSystem` based on the type
of storage.

The implemented service must be advertised by including a text file
`src/main/resources/META_INF/services/alluxio.underfs.UnderFileSystemFactory` containing the class
name (including package information) of the defining factory implementation.

### Integration Tests

To validate the under storage implementation the class
`alluxio.underfs.AbstractUnderFileSystemContractTest` should be extended. This test suite checks
that the contract between core Alluxio and an under storage connector is satisfied.

### Building the Extension JAR

The built JAR must include all dependencies of the extension project. In addition to avoid
collisions the dependency `alluxio-core-common` must be specified with `provided` scope. For
example, the maven definition would look like:

```xml
<dependencies>
    <!-- Core Alluxio dependencies -->
    <dependency>
      <groupId>org.alluxio</groupId>
      <artifactId>alluxio-core-common</artifactId>
      <scope>provided</scope>
    </dependency>
    ...
</dependencies>
```

## Installing the Extension Extension

JARs are picked up from the extensions directory configured using the property
`alluxio.extensions.dir`. A command line utlity can be used to distribute an exension JAR to hosts
running Alluxio processes. In environments where the CLI is not applicable, simply placing the JAR
in the extensions directory will suffice. For example, when running in containers, a custom image
can be built with extension binaries in the desired location.

### Command Line Utility

A CLI utility is provided to aid extension manangement.

```bash
./bin/alluxio extensions
Usage: alluxio extensions [generic options]
	 [install <URI>]
	 [ls]
	 [uninstall <JAR>]
```

When installing an extension the provided JAR is copied to hosts listed in `conf/masters` and
`conf/workers` using `rsync` and `ssh`. In environments where the tools is not applicable, other
more suitable tools can be used to place the JAR at the location specified in the property
`alluxio.extensions.dir`.

To list the installed extensions on any given host running the Alluxio processes, use `./bin/alluxio
ls`. The utility lists installed extensions using by scanning the local extensions directory.

The `uninstall` command works similar to `install` using hosts specified in `conf/masters` and
`conf/workers`.

### Installing from a Maven Coordinate

To install an extension from maven, it can be downloaded and install as follows:

```
mvn dependency:get -DremoteRepositories=http://repo1.maven.org/maven2/ -DgroupId=<my-extension-group>
\ -DartifactId=<my-extension-artifact> -Dversion=<version> -Dtransitive=false -Ddest=<my-extension>.jar

./bin/alluxio extension install <my-extension.jar>
```

## Validation

Once the extension JAR has been distributed, you should be able to mount your under storage using
the Alluxio CLI as follows:

```
./bin/alluxio fs mount /my-storage <my-scheme>://<path>/ -D<my-access-key>=<value>
```

To run sanity tests:
```
./bin/alluxio runTests --directory /my-storage
```

More rigorous integration tests should also be run from the under storage extension project repository:
```
mvn test -DtestPath=<my-scheme>://<path> -D<my-access-key>=<value>
```
