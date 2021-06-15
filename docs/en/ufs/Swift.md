---
layout: global
title: Swift
nickname: Swift
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with an under storage system supporting the
[Swift API](http://docs.openstack.org/developer/swift/).

## Prerequisites

The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

## Basic Setup

A Swift bucket can be mounted to the Alluxio either at the root of the namespace, or at a nested directory.

### Root Mount Point

Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.master.mount.table.root.ufs=swift://<bucket>/<folder>
alluxio.master.mount.table.root.option.fs.swift.user=<swift-user>
alluxio.master.mount.table.root.option.fs.swift.tenant=<swift-tenant>
alluxio.master.mount.table.root.option.fs.swift.password=<swift-user-password>
alluxio.master.mount.table.root.option.fs.swift.auth.url=<swift-auth-url>
alluxio.master.mount.table.root.option.fs.swift.auth.method=<swift-auth-model>
```

Replace `<bucket>/<folder>` with an existing Swift bucket location. Possible values of
`<swift-use-public>` are `true`, `false`. Possible values of `<swift-auth-model>` are `keystonev3`,
`keystone`, `tempauth`, `swiftauth`. 

When using either keystone authentication, the following parameter can optionally be set:

```properties
alluxio.master.mount.table.root.option.fs.swift.region=<swift-preferred-region>
```

On the successful authentication, Keystone will return two access URLs: public and private. If
Alluxio is used inside company network and Swift is located on the same network it is advised to set
value of `<swift-use-public>`  to `false`.

### Nested Mount Point

An Swift location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's [Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

```console
$ ./bin/alluxio fs mount \
  --option fs.swift.user=<SWIFT_USER> \
  --option fs.swift.tenant=<SWIFT_TENANT> \
  --option fs.swift.password=<SWIFT_PASSWORD> \
  --option fs.swift.auth.url=<AUTH_URL> \
  --option fs.swift.auth.method=<AUTH_METHOD> \
  /mnt/swift swift://<BUCKET>/<FOLDER>
```

## Options for Swift Object Storage

Using the Swift module makes [Ceph Object Storage](https://ceph.com/ceph-storage/object-storage/)
and [IBM SoftLayer](https://www.ibm.com/cloud/object-storage) Object Storage as under storage options
for Alluxio. To use Ceph, the [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/) module must
be deployed.

## Running Alluxio Locally with Swift

Start an Alluxio cluster:

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Visit your Swift bucket to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

```bash
<bucket>/<folder>/default_tests_files/BASIC_CACHE_THROUGH
```

To stop Alluxio, you can run:

```console
$ ./bin/alluxio-stop.sh local
```

## Running functional tests

The following command can be used to test if the given Swift credentials are valid.
Developers can also use it to run functional tests against a Swift endpoint 
to validate the contract between Alluxio and Swift.

```console
$ ./bin/alluxio runUfsTests --path swift://<bucket> \
  -Dfs.swift.user=<SWIFT_USER> \
  -Dfs.swift.tenant=<SWIFT_TENANT> \
  -Dfs.swift.password=<SWIFT_PASSWORD> \
  -Dfs.swift.auth.url=<AUTH_URL> \
  -Dfs.swift.auth.method=<AUTH_METHOD> 
```

## Advanced Setup

### Swift Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying object
storage.

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

In addition, `chown`, `chgrp`, and `chmod` to Alluxio directories and files do **NOT** propagate to the underlying
Swift buckets nor objects.
