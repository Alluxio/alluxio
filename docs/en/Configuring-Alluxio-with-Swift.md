---
layout: global
title: Configuring Alluxio with Swift
nickname: Alluxio with Swift
group: Under Store
priority: 1
---

This guide describes how to configure Alluxio with
[Swift](http://docs.openstack.org/developer/swift/) as the under storage system.

# Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio](Building-Alluxio-Master-Branch.html), or
[download the binaries locally](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

{% include Configuring-Alluxio-with-Swift/copy-tachyon-env.md %}

# Configuring Alluxio

To configure Alluxio to use Swift as its under storage system, modifications to the
`conf/tachyon-env.sh` file must be made. The first modification is to specify the Swift under
storage system address. You specify it by modifying `conf/tachyon-env.sh` to include:

{% include Configuring-Alluxio-with-Swift/underfs-address.md %}

Where `<swift-container>` is an existing Swift container.

The following configuration should be provided in the `conf/tachyon-env.sh`

{% include Configuring-Alluxio-with-Swift/several-configurations.md %}
  	
Possible values of `<swift-use-public>` are `true`, `false`.
Possible values of `<swift-auth-model>` are `keystone`,
`tempauth`, `swiftauth`

On the successful authentication, Keystone will return two access URLs: public and private. If Alluxio is used inside company network and Swift is located on the same network it is adviced to set value of `<swift-use-public>`  to `false`.


## Accessing IBM SoftLayer object store

Using the Swift module also makes the IBM SoftLayer object store an option as an under storage system for Alluxio. 
SoftLayer requires `<swift-auth-model>` to be configured as `swiftauth`
 
# Running Alluxio Locally with Swift

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Configuring-Alluxio-with-Swift/start-tachyon.md %}

This should start a Alluxio master and a Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Configuring-Alluxio-with-Swift/runTests.md %}

After this succeeds, you can visit your Swift container to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-Swift/swift-files.md %}

To stop Alluxio, you can run:

{% include Configuring-Alluxio-with-Swift/stop-tachyon.md %}

# Running functional test with IBM SoftLayer

Configure your Swift or SoftLayer account in the `tests/pom.xml`, where `authMethodKey` should be `keystone` or `tempauth` or `swiftauth`.
To run functional tests execute

{% include Configuring-Alluxio-with-Swift/functional-tests.md %}

In case of failures, logs located under `tests/target/logs`. You may also activate heap dump via

{% include Configuring-Alluxio-with-Swift/heap-dump.md %}
