---
layout: global
title: Under File Storage Extension API
nickname: UFS Extension API
group: Storage Integrations
priority: 101
---

This page is intended for developers of under storage extensions. Please look at
[managing extensions]({{ '/en/ufs/Ufs-Extensions.html' | relativize_url }}) for a
guide to using existing extensions.

* Table of Contents
{:toc}

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

A reference implementation can be found in the [alluxio-extensions](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/tutorial)
repository. In the rest of this section, we describe the steps involved in writing a new under
storage extension. The sample project, called `DummyUnderFileSystem`, uses maven as the build and
dependency management tool, and forwards all operations to a local filesystem.

### Implement the Under Storage Interface

The [HDFS Submodule](https://github.com/alluxio/alluxio/tree/master/underfs/hdfs) and [S3
Submodule](https://github.com/alluxio/alluxio/tree/master/underfs/s3a) are good examples of how
to enable a storage system to serve as Alluxio's underlying storage.

Step 1: Implement the interface `UnderFileSystem`

The `UnderFileSystem` interface is defined in the module `org.alluxio:alluxio-core-common`. Choose
to extend either `ConsistentUnderFileSystem` or `ObjectUnderFileSystem` to implement the `UnderFileSystem`
interface. 
- **`ConsistentUnderFileSystem`**: used for storage like HDFS which is not eventually consistent. 
- **`ObjectUnderFileSystem`**: suitable for connecting to object storage and abstracts away
mapping file system operations to an object store.

```java
public class DummyUnderFileSystem extends ConsistentUnderFileSystem {
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
supports and how to create the `UnderFileSystem` implementation.

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

Step 3: Define any properties required to configure the `UnderFileSystem`.
```java
public class DummyUnderFileSystemPropertyKey {
  public static final PropertyKey DUMMY_UFS_PROPERTY =
      new PropertyKey.Builder(Name.DUMMY_UFS_PROPERTY)
          .setDescription("...")
          .setDefaultValue("...")
          .build();

  public static final class Name {
    public static final String DUMMY_UFS_PROPERTY = "fs.dummy.property";
  }
}
```

### Declare the Service

Create a file at `src/main/resources/META-INF/services/alluxio.underfs.UnderFileSystemFactory`
advertising the implemented `UnderFileSystemFactory` to the ServiceLoader.

```
alluxio.underfs.dummy.DummyUnderFileSystemFactory
```

### Build

Include all transitive dependencies of the extension project in the built JAR using either
`maven-shade-plugin` or `maven-assembly`.

In addition, to avoid collisions, specify scope for the dependency `alluxio-core-common` as
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

Build the tarball:

```console
$ mvn package
```

### Install to Alluxio

Install the tarball to Alluxio:

```console
$ ./bin/alluxio extensions install <path>/<to>/<probject>/target/alluxio-underfs-<ufsName>-<version>.jar
```

### Test the Under Storage Extension

To ensure the new under storage module fulfills the minimum requirements to work with Alluxio, 
one can run contract tests to test different workflows with various combinations of operations against the under storage.

```console
$ ./bin/alluxio runUfsTests --path <scheme>://<path>/ -D<key>=<value>
```

In addition, one can also mount the under storage and run other kinds of tests on it. Please refer to 
[managing extensions]({{ '/en/ufs/Ufs-Extensions.html' | relativize_url }}).

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
[repository](https://github.com/Alluxio/alluxio/blob/master/docs/en/ufs/Ufs-Extensions.md) to edit the
list of extensions section on the
[documentation page]({{ '/en/ufs/Ufs-Extensions.html' | relativize_url }}).
