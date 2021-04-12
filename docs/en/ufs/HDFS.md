---
layout: global
title: HDFS
nickname: HDFS
group: Storage Integrations
priority: 1
---

* Table of Contents
{:toc}

This guide describes the instructions to configure
[HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
as Alluxio's under storage system.

## Prerequisites

To run an Alluxio cluster on a set of machines, you must deploy Alluxio server binaries to each of
these machines. You can either
[download the precompiled binaries directly](https://www.alluxio.io/download)
with the correct Hadoop version (recommended), or
[compile the binaries from Alluxio source code]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }})
(for advanced users).

Note that, when building Alluxio from source code, by default Alluxio server binaries is built to
work with Apache Hadoop HDFS of version `3.3.0`. To work with Hadoop distributions of other
versions, one needs to specify the correct Hadoop profile and run the following in your Alluxio
directory:

```console
$ mvn install -P<YOUR_HADOOP_PROFILE> -D<HADOOP_VERSION> -DskipTests
```

Alluxio provides predefined build profiles for `hadoop-2` and `hadoop-3` (enabled by default) for the major Hadoop versions 2.x and 3.x.
If you want to build Alluxio with a specific Hadoop release version, you can also specify the version in the command. 
For example,

```console
# Build Alluxio for the Apache Hadoop version Hadoop 2.7.1
$ mvn install -Phadoop-2 -Dhadoop.version=2.7.1 -DskipTests
# Build Alluxio for the Apache Hadoop version Hadoop 3.1.0
$ mvn install -Phadoop-3 -Dhadoop.version=3.1.0 -DskipTests
```

Please visit the
[Building Alluxio Master Branch]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}#hadoop-distribution-support)
page for more information about support for other distributions.

If everything succeeds, you should see
`alluxio-assembly-server-{{site.ALLUXIO_VERSION_STRING}}-jar-with-dependencies.jar` created in
the `${ALLUXIO_HOME}/assembly/server/target` directory.

## Basic Setup

To configure Alluxio to use HDFS as under storage, you will need to modify the configuration
file `conf/alluxio-site.properties`.
If the file does not exist, create the configuration file from the template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Edit `conf/alluxio-site.properties` file to set the under storage address to the HDFS namenode
address and the HDFS directory you want to mount to Alluxio. For example, the under storage address
can be `hdfs://localhost:8020` if you are running the HDFS namenode locally with default port and
mapping HDFS root directory to Alluxio, or `hdfs://localhost:8020/alluxio/data` if only the HDFS
directory `/alluxio/data` is mapped to Alluxio.
To find out where HDFS is running, use `hdfs getconf -confKey fs.defaultFS` to get the default hostname
and port HDFS is listening on.

```
alluxio.master.mount.table.root.ufs=hdfs://<NAMENODE>:<PORT>
```

Additionally, you may need to specify the following property to be your HDFS version.
See [mounting HDFS with specific versions]({{ '/en/ufs/HDFS.html' | relativize_url }}#mount-hdfs-with-specific-versions).
```
alluxio.master.mount.table.root.option.alluxio.underfs.version=<HADOOP VERSION>
```

## Example: Running Alluxio Locally with HDFS

Before this step, make sure your HDFS cluster is running and the directory mapped to Alluxio
exists. Start the Alluxio servers:

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

If your ramdisk is not mounted, the format command can fail. 
This is likely because this is the first time you are running Alluxio, 
you may need to start Alluxio with the `SudoMount` option.

```console
$ ./bin/alluxio-start.sh local SudoMount
```

This will start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

If the test fails with permission errors, make sure that the current user (`${USER}`) has
read/write access to the HDFS directory mounted to Alluxio. By default,
the login user is the current user of the host OS. To change the user, set the value of
`alluxio.security.login.username` in `conf/alluxio-site.properties` to the desired username.

After this succeeds, you can visit HDFS web UI at [http://localhost:50070](http://localhost:50070)
to verify the files and directories created by Alluxio exist. For this test, you should see
files named like: `/default_tests_files/BASIC_CACHE_THROUGH` at
[http://localhost:50070/explorer.html](http://localhost:50070/explorer.html)

Stop Alluxio by running:

```console
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Specify HDFS Configuration Location

When HDFS has non-default configurations, you need to configure Alluxio servers to
access HDFS with the proper configuration file.
Note that once this is set, your applications using
Alluxio client do not need any special configuration.

There are two possible approaches:

- Copy or make symbolic links from `hdfs-site.xml` and
`core-site.xml` from your Hadoop installation into `${ALLUXIO_HOME}/conf`. Make sure
this is set up on all servers running Alluxio.

- Alternatively, you can
set the property `alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration` in
`conf/alluxio-site.properties` to point to your `hdfs-site.xml` and `core-site.xml`.
Make sure this configuration is set on all servers running Alluxio.

```
alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/core-site.xml:/path/to/hdfs/conf/hdfs-site.xml
```

### HDFS Namenode HA Mode

To configure Alluxio to work with HDFS namenodes in HA mode, first configure Alluxio servers to [access HDFS with the proper configuration files](#specify-hdfs-configuration-location).

In addition, set the under storage address to `hdfs://nameservice/` (`nameservice` is the name of HDFS
service already configured in `core-site.xml`). To mount an HDFS subdirectory to Alluxio instead
of the whole HDFS namespace, change the under storage address to something like
`hdfs://nameservice/alluxio/data`.

```
alluxio.master.mount.table.root.ufs=hdfs://nameservice/
```

### User/Permission Mapping

Alluxio supports POSIX-like filesystem [user and permission checking]({{ '/en/operation/Security.html' | relativize_url }}).
To ensure that the permission information of files/directories including user, group and mode in
HDFS is consistent with Alluxio (e.g., a file created by user Foo in Alluxio is persisted to
HDFS also with owner as user Foo), the user to start Alluxio master and worker processes
**is required** to be either:

1. [HDFS super user](http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#The_Super-User).
Namely, use the same user that starts HDFS namenode process to also start Alluxio master and
worker processes.

2. A member of [HDFS superuser group](http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#Configuration_Parameters).
Edit HDFS configuration file `hdfs-site.xml` and check the value of configuration property
`dfs.permissions.superusergroup`. If this property is set with a group (e.g., "hdfs"), add the
user to start Alluxio process (e.g., "alluxio") to this group ("hdfs"); if this property is not
set, add a group to this property where your Alluxio running user is a member of this newly added
group.

The user set above is only the identity that starts Alluxio master and worker
processes. Once Alluxio servers started, it is **unnecessary** to run your Alluxio client
applications using this user.

### Connect to Secure HDFS

If your HDFS cluster is Kerberized, first configure Alluxio servers to [access HDFS with the proper configuration files](#specify-hdfs-configuration-location).

In addition, security configuration is needed for Alluxio to be able to
communicate with the HDFS cluster. Set the following Alluxio properties in `alluxio-site.properties`:

```properties
alluxio.master.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.master.principal=hdfs/<_HOST>@<REALM>
alluxio.worker.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.worker.principal=hdfs/<_HOST>@<REALM>
```

If connecting to secure HDFS, run `kinit` on all Alluxio nodes.
Use the principal `hdfs` and the keytab that you configured earlier in `alluxio-site.properties`
A known limitation is that the Kerberos TGT may expire after
the max renewal lifetime. You can work around this by renewing the TGT periodically. Otherwise you
may see `No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)`
when starting Alluxio services.

#### Custom Kerberos Realm/KDC

By default, Alluxio will use machine-level Kerberos configuration to determine the Kerberos realm
and KDC. You can override these defaults by setting the JVM properties
`java.security.krb5.realm` and `java.security.krb5.kdc`.

To set these, set `ALLUXIO_JAVA_OPTS` in `conf/alluxio-env.sh`.

```bash
ALLUXIO_JAVA_OPTS+=" -Djava.security.krb5.realm=<YOUR_KERBEROS_REALM> -Djava.security.krb5.kdc=<YOUR_KERBEROS_KDC_ADDRESS>"
```

### Mount HDFS with Specific Versions

There are multiple ways for a user to mount an HDFS cluster with a specified version as an under storage into Alluxio namespace.
Before mounting HDFS with a specific version, make sure you have built a client with that specific version of HDFS.
You can check the existence of this client by going to the `lib` directory under the Alluxio directory.

If you have built Alluxio from source, you can build additional client jar files by running `mvn` command under the `underfs` directory in the Alluxio source tree. 
For example, issuing the following command would build the client jar for the 2.8.0 version.

```console
$ mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests \
-Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true \
-Pufs-hadoop-2 -Dufs.hadoop.version=2.8.0
```

#### Using Mount Command-line
When using the mount Alluxio shell command, one can pass through the mount option `alluxio.underfs.version` to specify which version of HDFS to mount. If no such a version is specified, by default Alluxio treats it as Apache HDFS 2.7.

For example, the following commands mount two HDFS deployments—one is HDFS 2.2 and the other is 2.7—into Alluxio namespace under directory `/mnt/hdfs22` and `/mnt/hdfs27`.

```console
$ ./bin/alluxio fs mount \
  --option alluxio.underfs.version=2.2 \
  /mnt/hdfs12 hdfs://namenode1:8020/
$ ./bin/alluxio fs mount \
  --option alluxio.underfs.version=2.7 \
  /mnt/hdfs27 hdfs://namenode2:8020/
```

#### Using Site Properties

When mounting the under storage of Alluxio root directory with a specific HDFS version, one can add the
following line to the site properties file (`conf/alluxio-site.properties`)

```
alluxio.master.mount.table.root.ufs=hdfs://namenode1:8020
alluxio.master.mount.table.root.option.alluxio.underfs.version=2.2
```

#### Supported HDFS Versions

Alluxio supports the following versions of HDFS as a valid argument of mount option `alluxio.underfs.version`:

- Apache Hadoop: 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0, 3.1, 3.2, 3.3

Note: Apache Hadoop 1.0 and 1.2 are still supported, but not included in the default download.
To build this module yourself, build the shaded hadoop client and then the UFS module. 
Please refer to [Building from Source]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}) instructions 
to see how to build additional UFS libraries. 

### Use Hadoop Native Library

Hadoop comes with a native library that provides better performance and additional features compared to its Java implementation.
For example, when the native library is used, the HDFS client can use native checksum function which is more efficient than the default Java implementation.
To use the Hadoop native library with Alluxio HDFS under filesystem, first install the native library on Alluxio nodes by following the
instructions on [this page](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/NativeLibraries.html).
Once the hadoop native library is installed on the machine, update Alluxio startup Java parameters in `conf/alluxio-env.sh` by adding the following line:

```
ALLUXIO_JAVA_OPTS+=" -Djava.library.path=<local_path_containing_hadoop_native_library> "
```

Make sure to restart Alluxio services for the change to take effect.
