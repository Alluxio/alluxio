---
layout: global
title: Configuring Alluxio with Swift
nickname: Alluxio with Swift
group: Under Store
priority: 1
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with an under storage system supporting the
[Swift API](http://docs.openstack.org/developer/swift/).

## Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file with `bootstrapConf` command.
For example, if you are running Alluxio on your local machine, `ALLUXIO_MASTER_HOSTNAME` should be
set to `localhost`

{% include Configuring-Alluxio-with-Swift/bootstrapConf.md %}

Alternatively, you can also create the configuration file from the template and set the contents
manually.

{% include Common-Commands/copy-alluxio-env.md %}

## Configuring Alluxio

You need to configure Alluxio to use Swift as its under storage system by modifying
`conf/alluxio-site.properties`. Modify `conf/alluxio-site.properties` to include:

{% include Configuring-Alluxio-with-Swift/underfs-address.md %}

Where `<swift-container>` is an existing Swift container.

The following configuration should be provided in the `conf/alluxio-site.properties`

{% include Configuring-Alluxio-with-Swift/several-configurations.md %}

Possible values of `<swift-use-public>` are `true`, `false`. Possible values of `<swift-auth-model>`
are `keystonev3`, `keystone`, `tempauth`, `swiftauth`. When using either keystone authentication, the following
parameter can optionally be set

{% include Configuring-Alluxio-with-Swift/keystone-region-configuration.md %}

Alternatively, these configuration settings can be set in the `conf/alluxio-env.sh` file. More
details about setting configuration parameters can be found in
[Configuration Settings](Configuration-Settings.html#environment-variables).

On the successful authentication, Keystone will return two access URLs: public and private. If
Alluxio is used inside company network and Swift is located on the same network it is adviced to set
value of `<swift-use-public>`  to `false`.

## Options for Swift Object Storage

Using the Swift module makes [Ceph Object Storage](https://ceph.com/ceph-storage/object-storage/) and [IBM SoftLayer](http://www.softlayer.com/object-storage) Object Storage as under storage options for Alluxio. To use Ceph, the [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/) module must be deployed. 
 
## Running Alluxio Locally with Swift

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit your Swift container to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-Swift/swift-files.md %}

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}

## Running functional tests

Configure your Swift account credentials under `swiftTest` in `tests/pom.xml`, where `authMethodKey` should be
`keystone` or `tempauth` or `swiftauth`. To run functional tests execute

{% include Configuring-Alluxio-with-Swift/functional-tests.md %}

In case of failures, logs located under `tests/target/logs`. You may also activate heap dump via

{% include Configuring-Alluxio-with-Swift/heap-dump.md %}

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
