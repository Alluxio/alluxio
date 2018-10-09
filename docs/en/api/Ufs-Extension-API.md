---
layout: global
title: Under File Storage Extension API
nickname: Ufs Extension API
group: APIs
priority: 0
---

* Table of Contents
{:toc}


This page is intended for developers of under storage extensions. Please look at [managing
extensions](UFSExtensions.html) for a guide to using existing extensions.

## Introduction

Under storage extensions provide a framework to enable additional storage systems to work with Alluxio and makes it convenient to develop
modules not already supported by Alluxio. Extensions are built as JARs and included at a specific extensions location to be
picked up by core Alluxio. This page describes the mechanics of how extensions in Alluxio work, and
provides detailed instructions for developing an under storage extension.

If the modules included in core Alluxio do not use the interface supported by your desired storage system, you may choose to implement 
an under storage extension.

## Implementing an Under Storage Extension

Building a new under storage connector involves: 

- Implementing the required under storage interface
- Declaring the service implementation
- Bundling up the implementation and transitive dependencies in an uber JAR

A reference implementation can be found in the [alluxio-extensions](https://github.com/Alluxio
/alluxio-extensions/tree/master/underfs/tutorial) repository. In the rest of this section we
describe the steps involved in writing a new under storage extension. The sample project, called
`DummyUnderFileSystem`, uses maven as the build and dependency management tool, and forwards all
operations to a local filesystem.

### Implement the Under Storage Interface

The [HDFS Submodule](https://github.com/alluxio/alluxio/tree/master/underfs/hdfs) and [S3A Submodule](https://github.com/alluxio/alluxio/tree/master/underfs/s3a) are two good examples of how to enable a storage system to serve as Alluxio's underlying storage.

Step 1: Implement the interface `UnderFileSystem`

The `UnderFileSystem` interface is defined in the module `org.alluxio:alluxio-core-common`. Choose
to extend either `BaseUnderFileSystem` or `ObjectUnderFileSystem` to implement the `UnderFileSystem`
interface. `ObjectUnderFileSystem` is suitable for connecting to object storage and abstracts away
mapping file system operations to an object store.

```java
public class DummyUnderFileSystem extends BaseUnderFileSystem {
	// Implement filesystem operations
	...
}
```

or,

```java
public class DummyUnderFileSystem extends ObjectUnderFileSystem {
	// Implement object store operations 
	...
}
```

Step 2: Implement the interface `UnderFileSystemFactory`

The under storage factory determines defines which paths the `UnderFileSystem` implementation
supports, and how to create the `UnderFileSystem` implementation.

```java
public class DummyUnderFileSystemFactory implements UnderFileSystemFactory {
	...

        @Override
        public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
                // Create the under storage instance
        }

        @Override
	public boolean supportsPath(String path) {
		// Choose which schemes to support, e.g., dummy://
	}
}
```

### Declare the Service

Create a file at `src/main/resources/META_INF/services/alluxio.underfs.UnderFileSystemFactory`
advertising the implemented `UnderFileSystemFactory` to the ServiceLoader.

```
alluxio.underfs.dummy.DummyUnderFileSystemFactory
```

### Build

Include all transitive dependencies of the extension project in the built JAR using either
`maven-shade-plugin` or `maven-assembly`.

In addition, to avoid collisions specify scope for the dependency `alluxio-core-common` as
`provided`. The maven definition would look like:

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

### Test

Extend `AbstractUnderFileSystemContractTest` to test that the defined `UnderFileSystem` adheres to
the contract between Alluxio and an under storage module. Look at the reference implementation to
include parameters such as the working directory for the test.

```java 
public final class DummyUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
    ...
} 
```

## How it Works

### Service Discovery

Extension JARs are loaded dynamically at runtime by Alluxio servers, which enables Alluxio to talk
to new under storage systems without requiring a restart. Alluxio servers use Java
[ServiceLoader](https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html) to discover
implementations of the under storage API. Providers include implementations of the
`alluxio.underfs.UnderFileSystemFactory` interface. The implementation is advertised by including a
text file in `META_INF/services` with a single line pointing to the class implementing the said
interface.

### Dependency Management

Implementors are required to include transitive dependencies in their extension JARs. Alluxio performs
isolated classloading for each extension JARs to avoid dependency conflicts between Alluxio servers and
extensions.

## Contributing your Under Storage extension to Alluxio

Congratulations! You have developed a new under storage extension to Alluxio. Let the community
know by submitting a pull request to the Alluxio
[repository](https://github.com/Alluxio/alluxio/tree/master/docs/en/UFSExtensions.md) to edit the
list of extensions section on the [documentation page](UFSExtensions.html).
