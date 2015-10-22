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
export TACHYON_UNDERFS_ADDRESS=swift://containter.swift1
```

Where `container` is an existing Swift container and `swift1` is a profile in `core-sites.xml`. By
default, Tachyon uses the `conf` directory to load `core-sites.xml`. To specify a different location,
please configure `tachyon.underfs.hadoop.configuration` accordingly.

Swift depends on Hadoop version 2.3.0 or later and can be configured via `hadoop-openstack.version`
in the main pom.xml. Make sure to compile Tachyon with the same Hadoop version as
`hadoop-openstack.version`. For example, to compile Tachyon with Swift and Hadoop 2.4.0 use:

```bash
$ mvn -Dhadoop.version=2.4.0 clean package
```

# Configuring Hadoop

After the build is successful, the `core-sites.xml` configuration file needs to be changed. The 
configuration template can be found in the `conf/core-sites.xml.template` file; it
contains three example sections: local Swift based on Keystone authentication model, local Swift
based on temp authentication model, and SoftLayer public object store. The general structure of the
parameters is `fs.swift.service.<PROFILE>.<PARAMETER>` where `<PROFILE>` is a name that will be
later used as a part of the Swift URL. For example, if `<PROFILE>` is “swift1” then the Swift URL
would be

	swift://<SWIFT CONTAINER>.swift1/

Edit `core-sites.xml` and update `fs.swift.service.<PROFILE>.auth.url`.

For Swift using Temp Auth or SoftLayer, update:

	fs.swift.service.<PROFILE>.apikey, fs.swift.service.<PROFILE>.username

For Swift using Keystone Auth, update:

	fs.swift.service.<PROFILE>.region, fs.swift.service.<PROFILE>.tenant,
	fs.swift.service.<PROFILE>.password, fs.swift.service.<PROFILE>.username

## Accessing IBM SoftLayer object store

Using the Swift module also makes the IBM SoftLayer object store an option as an under storage
system for Tachyon. To access SoftLayer there is an additional preliminary step. Currently,
hadoop-openstack implements Keystone authentication model, which is not suitable for SoftLayer
object store. There is a pending [patch](https://issues.apache.org/jira/browse/HADOOP-10420) to
extend hadoop-openstack project with additional type of authentication, which is also good for
accessing SoftLayer object store. As a temporary solution we would like to explain how to apply this
patch on Hadoop and then build Tachyon, enabling access to SoftLayer object store.

Hadoop patch for SoftLayer object store. We demonstrate for version 2.4.0:

1.	Download `hadoop-2.4.0-src.tar.gz` and extract it under `hadoop-2.4.0` folder.
2.	Download https://issues.apache.org/jira/secure/attachment/12662347/HADOOP-10420-007.patch and
save it under `hadoop-2.4.0` folder.
3.	Check that changes are visible and can be applied, by executing:
`git apply --stat HADOOP-10420-007.patch` or `git apply --check HADOOP-10420-007.patch` from
`hadoop-2.4.0` folder.
4.	Apply the patch: `git apply HADOOP-10420-007.patch`.
5.	Navigate to `/hadoop-2.4.0/hadoop-tools/hadoop-openstack` folder and execute: `mvn -DskipTests
install`.
6.	Build Tachyon as described above.

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
$ ./bin/tachyon runTest Basic STORE SYNC_PERSIST
```

After this succeeds, you can visit your Swift container to verify the files and directories created
by Tachyon exist. For this test, you should see a file named:

    swift://<SWIFT CONTAINER>.swift1/tachyon/data/default_tests_files/BasicFile_STORE_SYNC_PERSIST

To stop Tachyon, you can run:

```bash
$ ./bin/tachyon-stop.sh
```
