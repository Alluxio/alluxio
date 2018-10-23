---
layout: global
title: HDFS
nickname: HDFS
group: Under Stores
priority: 1
---

* Table of Contents
{:toc}

This guide describes the instructions to configure
[HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
as Alluxio's under storage system.

## Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio server binaries to each of
these machines. You can either
[download the precompiled binaries directly](http://www.alluxio.org/download)
with the correct Hadoop version (recommended), or
[compile the binaries from Alluxio source code]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %})
(for advanced users).

Note that, when building Alluxio from source code, by default Alluxio server binaries is built to
work with Apache Hadoop HDFS of version `2.2.0`. To work with Hadoop distributions of other
versions, one needs to specify the correct Hadoop profile and run the following in your Alluxio
directory:

```bash
$ mvn install -P<YOUR_HADOOP_PROFILE> -D<HADOOP_VERSION> -DskipTests
```

Alluxio provides predefined build profiles including `hadoop-1`, `hadoop-2` (enabled by default),
`hadoop-3` for the major Hadoop versions 1.x, 2.x and 3.x. If you want to build Alluxio with a specific
Hadoop release version, you can also specify the version in the command. For example,

```bash
# Build Alluxio for the Apache Hadoop version Hadoop 2.7.1
$ mvn install -Phadoop-2 -Dhadoop.version=2.7.1 -DskipTests
# Build Alluxio for the Apache Hadoop version Hadoop 2.7.1
$ mvn install -Phadoop-3 -Dhadoop.version=3.0.0 -DskipTests
```

Please visit the
[Building Alluxio Master Branch]({{ site.baseurl }}{% link
en/contributor/Building-Alluxio-From-Source.md %}#hadoop-distribution-support) page for more information about
support for other distributions.

If everything succeeds, you should see
`alluxio-assembly-server-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` created in
the `${ALLUXIO_HOME}/assembly/server/target` directory.

## Configuring Alluxio

To configure Alluxio to use HDFS as under storage, you will need to modify the configuration
file `conf/alluxio-site.properties`.
If the file does not exist, create the configuration file from the template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

### Basic Configuration

Edit `conf/alluxio-site.properties` file to set the under storage address to the HDFS namenode
address and the HDFS directory you want to mount to Alluxio. For example, the under storage address
can be `hdfs://localhost:9000` if you are running the HDFS namenode locally with default port and
mapping HDFS root directory to Alluxio, or `hdfs://localhost:9000/alluxio/data` if only the HDFS
directory `/alluxio/data` is mapped to Alluxio.

```
alluxio.underfs.address=hdfs://<NAMENODE>:<PORT>
```

### HDFS namenode HA mode

To configure Alluxio to work with HDFS namenodes in HA mode, you need to configure Alluxio servers to
access HDFS with the proper configuration file. Note that once this is set, your applications using
Alluxio client do not need any special configuration.

There are two possible approaches:
- Copy or make symbolic links from `hdfs-site.xml` and
`core-site.xml` from your Hadoop installation into `${ALLUXIO_HOME}/conf`. Make sure
this is set up on all servers running Alluxio.

- Alternatively, you can
set the property `alluxio.underfs.hdfs.configuration` in `conf/alluxio-site.properties` to point to
your `hdfs-site.xml` and `core-site.xml`. Make sure this configuration is set on all servers running Alluxio.

```
alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/core-site.xml:/path/to/hdfs/conf/hdfs-site.xml
```

Set the under storage address to `hdfs://nameservice/` (`nameservice` is the name of HDFS
service already configured in `core-site.xml`). To mount an HDFS subdirectory to Alluxio instead
of the whole HDFS namespace, change the under storage address to something like
`hdfs://nameservice/alluxio/data`.

```
alluxio.underfs.address=hdfs://nameservice/
```

### User/Permission Mapping

Alluxio supports POSIX-like filesystem [user and permission checking]({{ site.baseurl }}{% link en/advanced/Security.md %}).
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

### HDFS Security Configuration

If your HDFS cluster is Kerberized, security configuration is needed for Alluxio to be able to
communicate with the HDFS cluster. Set the following Alluxio properties in `alluxio-site.properties`:

```properties
alluxio.master.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.master.principal=hdfs/<_HOST>@<REALM>
alluxio.worker.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.worker.principal=hdfs/<_HOST>@<REALM>
```

#### Custom Kerberos Realm/KDC

By default, Alluxio will use machine-level Kerberos configuration to determine the Kerberos realm
and KDC. You can override these defaults by setting the JVM properties
`java.security.krb5.realm` and `java.security.krb5.kdc`.

To set these, set `ALLUXIO_JAVA_OPTS` in `conf/alluxio-env.sh`.

```bash
ALLUXIO_JAVA_OPTS+=" -Djava.security.krb5.realm=<YOUR_KERBEROS_REALM> -Djava.security.krb5.kdc=<YOUR_KERBEROS_KDC_ADDRESS>"
```

## Running Alluxio Locally with HDFS

Before this step, make sure your HDFS cluster is running and the directory mapped to Alluxio
exists.

If connecting to secure HDFS, run `kinit` on all Alluxio nodes.
Use the principal `hdfs` and the keytab that you configured earlier in `alluxio-site.properties`
A known limitation is that the Kerberos TGT may expire after
the max renewal lifetime. You can work around this by renewing the TGT periodically. Otherwise you
may see `No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)`
when starting Alluxio services.

Finally, you are ready to start the Alluxio servers!

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

This will start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```bash
$ bin/alluxio runTests
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

```bash
$ bin/alluxio-stop.sh local
```
