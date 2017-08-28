---
layout: global
group: Features
title: Under Storage Extensions
---

* Table of Contents
{:toc}

Alluxio can be extended with the addition of under storage modules at runtime. Under storage
extensions (built as JARs) can be included at a specific location to be picked up by core Alluxio
without the need to restart any running processes. Adding new under storage connectors to Alluxio
does not require compilation of core Alluxio modules and can be used to enable Alluxio to work with
your choice of storage system. On this page, we provide instructions for managing extensions.

## Installing the Extension Extension

JARs are picked up from the extensions directory configured using the property
`alluxio.extensions.dir`. A command line utlity can be used to distribute an exension JAR to hosts
running Alluxio processes. In environments where the CLI is not applicable, simply placing the JAR
in the extensions directory will suffice. For example, when running in containers, a custom image
can be built with extension binaries in the desired location.

### Command Line Utility

A CLI utility is provided to aid extension manangement.

```bash
bin/alluxio extensions
Usage: alluxio extensions [generic options]
	 [install <URI>]
	 [ls]
	 [uninstall <JAR>]
```

When installing an extension, the provided JAR is copied to hosts listed in `conf/masters` and
`conf/workers` using `rsync` and `ssh`. In environments where the tools is not applicable, other
more suitable tools can be used to place the JAR at the location specified in the property
`alluxio.extensions.dir`.

To list the installed extensions on any given host running the Alluxio processes, use `bin/alluxio
extensios ls`. The utility lists installed extensions by scanning the local extensions directory.

The `bin/alluxio extensions uninstall` command works similar to `install` using hosts specified in
`conf/masters` and `conf/workers`.

### Installing from a Maven Coordinate

To install an extension from maven, it can be downloaded and installed as follows:

```bash
mvn dependency:get -DremoteRepositories=http://repo1.maven.org/maven2/ -DgroupId=<my-extension-group>
\ -DartifactId=<my-extension-artifact> -Dversion=<version> -Dtransitive=false -Ddest=<my-extension>.jar

bin/alluxio extensions install <my-extension.jar>
```

## Validation

Once the extension JAR has been distributed, you should be able to mount your under storage using
the Alluxio CLI as follows:

```bash
bin/alluxio fs mount /my-storage <my-scheme>://<path>/ -D<my-access-key>=<value>
```

To run sanity tests:
```bash
bin/alluxio runTests --directory /my-storage
```