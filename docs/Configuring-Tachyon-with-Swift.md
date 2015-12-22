---
layout: global
title: Configuring Tachyon with Swift
nickname: Tachyon with Swift
group: Under Stores
priority: 1
---

This guide describes how to configure Tachyon with
[Swift](http://docs.openstack.org/developer/swift/) as the under storage system.

# Initial Setup

First, the Tachyon binaries must be on your machine. You can either
[compile Tachyon](Building-Tachyon-Master-Branch.html), or
[download the binaries locally](Running-Tachyon-Locally.html).

Then, if you haven't already done so, create your configuration file from the template:

```bash
$ cp conf/tachyon-env.sh.template conf/tachyon-env.sh
```

# Configuring Tachyon

To configure Tachyon to use Swift as its under storage system, modifications to the
`conf/tachyon-env.sh` file must be made. The first modification is to specify the Swift under
storage system address. You specify it by modifying `conf/tachyon-env.sh` to include:

```bash
export TACHYON_UNDERFS_ADDRESS=swift://<swift-containter>
```

Where `<swift-container>` is an existing Swift container.

The following configuration should be provided in the `conf/tachyon-env.sh`


 	-Dfs.swift.user=<swift-user>
  	-Dfs.swift.tenant=<swift-tenant>
  	-Dfs.swift.apikey=<swift-user-password>
  	-Dfs.swift.auth.url=<swift-auth-url>
  	-Dfs.swift.auth.port=<swift-auth-url-port>
  	-Dfs.swift.use.public.url=<swift-use-public>
  	-Dfs.swift.auth.method=<swift-auth-model>
  	
Possible values of `<swift-use-public>` are `true`, `false`.
Possible values of `<swift-auth-model>` are `keystone`,
`tempauth`, `swiftauth`

On the successfull authentication, Keystone will return two access URLs: public and private. If Tachyon is used inside company network and Swift is located on the same network it is adviced to set value of `<swift-auth-model>`  to `false`.


## Accessing IBM SoftLayer object store

Using the Swift module also makes the IBM SoftLayer object store an option as an under storage system for Tachyon. 
SoftLayer requires `<swift-auth-model>` to be configured as `swiftauth`
 
# Running Tachyon Locally with Swift

After everything is configured, you can start up Tachyon locally to see that everything works.

```bash
$ ./bin/tachyon format
$ ./bin/tachyon-start.sh local
```

This should start a Tachyon master and a Tachyon worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ ./bin/tachyon runTests
```

After this succeeds, you can visit your Swift container to verify the files and directories created
by Tachyon exist. For this test, you should see files named like:

    swift://<SWIFT CONTAINER>/tachyon/data/default_tests_files/BasicFile_STORE_SYNC_PERSIST

To stop Tachyon, you can run:

```bash
$ ./bin/tachyon-stop.sh
```
# Running functional test with IBM SoftLayer

Configure your Swift or SoftLayer account in the `integration-tests/pom.xml`, where `authMethodKey` should be `keystone` or `tempauth` or `swiftauth`. 
To run functional tests execute

```bash
$ mvn test -PswiftTest -pl integration-tests
```
In case of failures, logs located under `integration-tests/target/logs`. You may also activate heap dump via

	<argLine>-XX:+HeapDumpOnOutOfMemoryError 
		-XX:HeapDumpPath=/location/dump</argLine>