---
layout: global
title: Swift
nickname: Swift
group: Under Stores
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with an under storage system supporting the
[Swift API](http://docs.openstack.org/developer/swift/).

## Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-From-Source.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

## Configuring Alluxio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.address=swift://<container>/<folder>
fs.swift.user=<swift-user>
fs.swift.tenant=<swift-tenant>
fs.swift.password=<swift-user-password>
fs.swift.auth.url=<swift-auth-url>
fs.swift.use.public.url=<swift-use-public>
fs.swift.auth.method=<swift-auth-model>
```

Replace `<container>/<folder>` with an existing Swift container location. Possible values of `<swift-use-public>`
are `true`, `false`. Possible values of `<swift-auth-model>` are `keystonev3`, `keystone`, `tempauth`, `swiftauth`.

When using either keystone authentication, the following parameter can optionally be set:

```properties
fs.swift.region=<swift-preferred-region>
```

On the successful authentication, Keystone will return two access URLs: public and private. If
Alluxio is used inside company network and Swift is located on the same network it is adviced to set
value of `<swift-use-public>`  to `false`.

## Options for Swift Object Storage

Using the Swift module makes [Ceph Object Storage](https://ceph.com/ceph-storage/object-storage/) and [IBM SoftLayer](http://www.softlayer.com/object-storage) Object Storage as under storage options for Alluxio. To use Ceph, the [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/) module must be deployed.

## Running Alluxio Locally with Swift

After configuration, you can start an Alluxio cluster:

```bash
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ ./bin/alluxio runTests
```

After this succeeds, you can visit your Swift container to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

```bash
<container>/<folder>/default_tests_files/Basic_CACHE_THROUGH
```

To stop Alluxio, you can run:

```bash
$ ./bin/alluxio-stop.sh local
```

## Running functional tests

```bash
$ mvn test -DtestSwiftContainerKey=swift://<container>
```

## Swift Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying object storage.

The Swift credentials specified in Alluxio (`fs.swift.user`, `fs.swift.tenant` and
`fs.swift.password`) represents a Swift user. Swift service backend checks the user permission to
the container. If the given Swift user does not have the right access permission to the specified
container, a permission denied error will be thrown. When Alluxio security is enabled, Alluxio loads
the container ACL to Alluxio permission on the first time when the metadata is loaded to Alluxio
namespace.

### Mount point sharing

If you want to share the Swift mount point with other users in Alluxio namespace, you can enable
`alluxio.underfs.object.store.mount.shared.publicly`.

### Permission change

In addition, chown/chgrp/chmod to Alluxio directories and files do NOT propagate to the underlying
Swift containers nor objects.
