---
layout: global
title: Third-party Under Storage Extensions
nickname: Third-party UFS
group: Storage Integrations
priority: 100
---

* Table of Contents
{:toc}

This page is intended for users of under storage extensions. Please look at [developing
extensions]({{ '/en/ufs/Ufs-Extension-API.html' | relativize_url }}) for an extension development
guide.

Alluxio can be extended with the addition of under storage modules at runtime. Under storage
extensions (built as JARs) can be included at a specific location to be picked up by core Alluxio
without the need to restart any running processes. Adding new under storage connectors to Alluxio
can be used to enable Alluxio to work with new storage systems which may not have existing support.

## List of Extensions

Following is a list of under storage extension projects:

- [S3N](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/s3n)
- [GlusterFS](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/glusterfs)
- [OBS](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/obs)
- [SSH UFS](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/ssh)

## Managing Extensions

Extension JARs are picked up from the extensions directory configured using the property
`alluxio.extensions.dir` (default: `${alluxio-home}/extensions`). The extensions command line utility
manages distribution of extension JARs across an Alluxio cluster. In environments where the CLI is
not applicable (see limitations below), place the JAR in the extensions directory. For example, when
running in containers, build a custom image with extension binaries in the appropriate location.

### Command Line Utility

A command line utility is provided to aid extension manangement.

```console
$ ./bin/alluxio extensions
Usage: alluxio extensions [generic options]
	 [install <URI>]
	 [ls]
	 [uninstall <JAR>]
```

#### Install

The `install` command copies the provided JAR to Alluxio servers listed in `conf/masters` and
`conf/workers` using `rsync` and `ssh`. However, these tools may not be available in all
environments. In such a scenario, use other more suitable tools to place the JAR at the location
specified in the property `alluxio.extensions.dir` on Alluxio servers.

#### List

To list the installed extensions on any given host running the Alluxio processes, use the `ls`
command. The utility lists any installed extensions by scanning the local extensions directory.

#### Uninstall

The `uninstall` command works in a similar manner to `install` using hosts specified in
`conf/masters` and `conf/workers`. Remove the extensions manually from Alluxio servers in case some
hosts are not reachable from the host executing the command.

#### Installing from a Maven Coordinate

To install an extension from maven, download the JAR first and then install as follows:

```console
$ mvn dependency:get -DremoteRepositories=http://repo1.maven.org/maven2/ \ 
  -DgroupId=<extension-group> -DartifactId=<extension-artifact> \ 
  -Dversion=<version> -Dtransitive=false -Ddest=<extension>.jar

$ ./bin/alluxio extensions install <extension.jar>
```

## Validation

Once the extension JAR has been distributed, you should be able to mount your under storage using
the Alluxio CLI as follows:

```console
$ ./bin/alluxio fs mount /my-storage <scheme>://<path>/ --option <key>=<value>
```
where, `<key>=<value>` can be replaced with any required configuration for the under storage.

To run sanity tests execute:

```console
$ ./bin/alluxio runTests --directory /my-storage
```
