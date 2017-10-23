---
layout: global
title: Configuring Alluxio with HDFS
nickname: Alluxio with HDFS
group: Under Store
priority: 3
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
[compile the binaries from Alluxio source code](building-Alluxio-Master-Branch.html)
(for advanced users).

Note that, when building Alluxio from source code, by default Alluxio server binaries is built to
work with Apache Hadoop HDFS of version `2.2.0`. To work with Hadoop distributions of other
versions, one needs to specify  the correct Hadoop profile and run the following in your Alluxio
directory:

```bash
$ mvn install -P<YOUR_HADOOP_PROFILE> -DskipTests
```

Alluxio provides predefined build profiles including `hadoop-1`, `hadoop-2.2`, `hadoop-2.3` ...
`hadoop-2.8` for different distributions of Hadoop. If you want to build Alluxio with a specific
Hadoop release version, you can also specify the version `<YOUR_HADOOP_VERSION>` in the command.
For example,

```bash
$ mvn install -Phadoop-2.7 -Dhadoop.version=2.7.1 -DskipTests
```

would compile Alluxio for the Apache Hadoop version 2.7.1.
Please visit the
[Building Alluxio Master Branch](Building-Alluxio-Master-Branch.html#distro-support) page for more
information about support for other distributions.

If everything succeeds, you should see
`alluxio-assembly-server-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` created in
the `assembly/server/target` directory and this is the jar file you can use to run both Alluxio
Master and Worker.

## Configuring Alluxio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

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
alluxio.underfs.address=hdfs://NAMENODE:PORT
```

### HDFS namenode HA mode

If HDFS namenodes are running in HA mode, both Alluxio servers and clients should be configured
properly in order to access HDFS.

For Alluxio servers (masters and workers), copy or make symbolic links from `hdfs-site.xml` and
`core-site.xml` from your hadoop installation into `${ALLUXIO_HOME}/conf`. Alternatively, you can
set `alluxio.underfs.hdfs.configuration` to the hadoop property file `hdfs-site.xml` (or
`core-site.xml`) in `conf/alluxio-site.properties` (make sure all the relative configurations are
available in the file).

Then, set the under storage address to `hdfs://nameservice/` (`nameservice` is the name of HDFS
service already configured in `core-site.xml`) if you are mapping HDFS root directory to Alluxio,
or `hdfs://nameservice/alluxio/data` if only the HDFS directory `/alluxio/data` is mapped to
Alluxio.

```
alluxio.underfs.address=hdfs://nameservice/
```
Next, for Alluxio clients, `alluxio.underfs.hdfs.configuration` should also be set to the hadoop
property file `hdfs-site.xml` (or `core-site.xml`).

## Ensure Correct Permission Mapping

Since v1.3, Alluxio supports filesystem [user and permission checking](Security.html) by default.
To ensure that the permission information of files/directories including user, group and mode in
HDFS is consistent with Alluxio, the user to start Alluxio master and worker processes
**is required** to be either case:

1. [HDFS super user](http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#The_Super-User).
Namely, use the same user that starts HDFS namenode process to also start Alluxio master and
worker processes.

2. A member of [HDFS superuser group](http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#Configuration_Parameters).
Edit HDFS configuration file `hdfs-site.xml` and check the value of configuration property
`dfs.permissions.superusergroup`. If this property is set with a group (e.g., "hdfs"), add the
user to start Alluxio process (e.g., "alluxio") to this group ("hdfs"); if this property is not
set, add a group to this property where your Alluxio running user is a member of this newly added
group.

Note that, the user set above is only the identity that starts Alluxio master and worker
processes. Once Alluxio servers started, it is **unnecessary** to run your Alluxio client
applications using this user.

## Running Alluxio Locally with HDFS

Before this step, please make sure your HDFS cluster is running and the directory mapped to Alluxio
exists. After everything is configured, you can start up Alluxio locally to see that everything
works.

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

This should start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ bin/alluxio runTests
```
After this succeeds, you can visit HDFS web UI at [http://localhost:50070](http://localhost:50070)
to verify the files and directories created by Alluxio exist. For this test, you should see
files named like: `/default_tests_files/BASIC_CACHE_THROUGH`

You can stop Alluxio any time by running:

```bash
$ bin/alluxio-stop.sh local
```
