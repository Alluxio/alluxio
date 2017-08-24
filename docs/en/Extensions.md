---
layout: global
group: Features
title: Under Storage Extensions
priority: 7
---

* Table of Contents
{:toc}

Alluxio can be extended with the addition of under storage modules at runtime. Under storage extensions can be built as JARs and included at a specific location to be picked up by core Alluxio without the need to restart. This page describes the internals of how extensions in Alluxio work, and provides instructions for building and installing an extension.

## How it Works

### Service Discovery
Alluxio servers use Java [ServiceLoader](https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html) to discover implementations of the under storage API. Specifically providers include implementations of the `alluxio.underfs.UnderFileSystemFactory` interface. A common way to advertise an implementation is by including a text file `src/main/resources/META_INF/services/alluxio.underfs.UnderFileSystemFactory` with a single line pointing to the class implementing the said interface.

### Dependency Management
Implementors are required to include dependencies of the extension in the built JAR. Simply including all dependencies of an extension project on the server classpath would have created potential for hard to debug dependency conflicts. To avoid the said issues, the ServiceLoader in Alluxio uses a custom (per extension) [ClassLoader](https://docs.oracle.com/javase/7/docs/api/java/lang/ClassLoader.html) for classpath isolation. The ClassLoader defines how a class is transformed from raw bytes in a `.class` file to an instance of `java.lang.Class` that can be used in the JVM process. The custom class loader ensures that the bytes for an extension (and its dependencies) are isolated and do not cause conflicts with other extensions and core Alluxio.

## Creating an Under Storage Extension

### Implementing the Under Storage Interface

### Building the Extension JAR

### Testing the Extension

## Installing the Extension
Extension JARs are picked up from the extensions directory configured using the property `alluxio.extensions.dir`. A command line utlity can be used to distribute an exension JAR to hosts running Alluxio processes. In environments where the CLI is not applicable, simply placing the JAR in the extensions directory will suffice. For example, when running in containers, a custom image can be built with extension binaries in the desired location.

### Command Line Utility

### Installing from a Maven Coordinate
