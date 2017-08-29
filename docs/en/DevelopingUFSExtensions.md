---
layout: global
group: Resources
title: Developing Under Storage Extensions
---

* Table of Contents
{:toc}

This page is intended for developers of under storage extensions. Please look at [managing
extensions](UFSExtensions.html) for a guide to using existing extensions.

Under storage extensions are built as JARs and included at a specific extensions location to be
picked up by core Alluxio. This page describes the internals of how extensions in Alluxio work, and
provides detailed instructions for developing an under storage extension. Extensions provide a
framework to enable more under storages to work with Alluxio and makes it easy for develepors to
contribute packages.

# How it Works

## Service Discovery

Extension JARs are loaded dynamically at runtime by Alluxio servers, which enables Alluxio to talk
to new under storage systems. Alluxio servers use Java
[ServiceLoader](https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html) to discover
implementations of the under storage API. Specifically providers include implementations of the
`alluxio.underfs.UnderFileSystemFactory` interface. The implementation is advertised by including a
text file `src/main/resources/META_INF/services/alluxio.underfs.UnderFileSystemFactory` with a
single line pointing to the class implementing the said interface.

## Dependency Management

Implementors are required to include transitive dependencies of the extension in the built JAR. To
avoid dependency conflicts from including dependencies of an extension project on the server
classpath, the ServiceLoader in Alluxio uses a custom (per-extension)
[ClassLoader](https://docs.oracle.com/javase/7/docs/api/java/lang/ClassLoader.html) for isolation.

# Implementing an Under Storage Extension

Building a new under storage connector involves: 

- Implementing the required under storage interface and declaring the service implementation
- Bundling up the implementation and dependencies in an uber JAR

A reference implementation can be found in the [alluxio-extensions](https://github.com/Alluxio
/alluxio-extensions/tree/master/underfs/s3n) repository.

## Implement the Under Storage Interface

Refer to [integrating under storage systems](Integrating-Under-Storage-Systems.html) for
instructions for developing and testing an under storage module.

## Build

Include all transitive dependencies of the extension project in the built JAR. In addition, to
avoid collisions specify scope for the dependency `alluxio-core-common` as `provided`. 

For example, the maven definition would look like:

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
