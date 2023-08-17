---
layout: global
title: Swift
---


This guide describes how to configure Alluxio with an under storage system supporting the
[Swift API](https://wiki.openstack.org/wiki/Swift){:target="_blank"}.

Swift is a highly available, distributed, eventually consistent object/blob store. Organizations can use Swift to store lots of data efficiently, safely, and cheaply.

For more information about Swift, please read its [documentation](http://docs.openstack.org/developer/swift/){:target="_blank"}.

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using Swift with Alluxio:
<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<SWIFT_BUCKET>`</td>
        <td markdown="span">[Create a new Swift bucket](https://docs.openstack.org/newton/user-guide/cli-swift-create-containers.html){:target="_blank"} or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<SWIFT_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<SWIFT_USER>`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<SWIFT_TENANT>`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<SWIFT_PASSWORD>`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<SWIFT_AUTH_URL>`</td>
        <td markdown="span"></td>
    </tr><tr>
        <td markdown="span" style="width:30%">`<SWIFT_AUTH_METHOD>`</td>
        <td markdown="span"></td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<SWIFT_REGION>`</td>
        <td markdown="span"></td>
    </tr>
</table>


## Basic Setup

To use Swift as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=swift://<SWIFT_BUCKET>/<SWIFT_DIRECTORY>
fs.swift.user=<SWIFT_USER>
fs.swift.tenant=<SWIFT_TENANT>
fs.swift.password=<SWIFT_PASSWORD>
fs.swift.auth.url=<SWIFT_AUTH_URL>
fs.swift.auth.method=<SWIFT_AUTH_METHOD>
```

When using either keystone authentication, the following parameter can optionally be set:

```properties
fs.swift.region=<SWIFT_REGION>
```

On the successful authentication, Keystone will return two access URLs: public and private. If
Alluxio is used inside company network and Swift is located on the same network it is advised to set
value of `<swift-use-public>`  to `false`.

### Options for Swift Object Storage

Using the Swift module makes [Ceph Object Storage](https://ceph.com/ceph-storage/object-storage/){:target="_blank"}
and [IBM SoftLayer Object Storage](https://www.ibm.com/cloud/object-storage){:target="_blank"} as under storage options
for Alluxio. To use Ceph, the [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/){:target="_blank"} module must
be deployed.

See [CephObjectStorage Integration with Alluxio]({{ '/en/ufs/CephObjectStorage.html' | relativize_url }}).

## Running Alluxio Locally with Swift

Once you have configured Alluxio to Swift, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

## Running Functional Tests

The following command can be used to test if the given Swift credentials are valid.
Developers can also use it to run functional tests against a Swift endpoint 
to validate the contract between Alluxio and Swift.

```shell
$ ./bin/alluxio runUfsTests --path swift://<SWIFT_BUCKET> \
  -Dfs.swift.user=<SWIFT_USER> \
  -Dfs.swift.tenant=<SWIFT_TENANT> \
  -Dfs.swift.password=<SWIFT_PASSWORD> \
  -Dfs.swift.auth.url=<SWIFT_AUTH_URL> \
  -Dfs.swift.auth.method=<SWIFT_AUTH_METHOD> 
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
