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

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines. You can either
[compile the binaries from Alluxio source code](Building-Alluxio-Master-Branch.html), or
[download the precompiled binaries directly](Running-Alluxio-Locally.html).

Note that, by default, Alluxio binaries are built to work with Hadoop HDFS version `2.2.0`. To use
another Hadoop version, one needs to recompile Alluxio binaries from source code with the correct
Hadoop version set by either of following approaches. Assume `${ALLUXIO_HOME}` is the root directory
of Alluxio source code.

* Modify the `hadoop.version` tag defined in `${ALLUXIO_HOME}/pom.xml`. E.g., to work with Hadoop
`2.6.0`, modify this pom file to set "`<hadoop.version>2.6.0</hadoop.version>`" instead of
"`<hadoop.version>2.2.0</hadoop.version>`". Then recompile the source using maven.
To make compiling faster, you can add `-DskipTests` option to skip unit tests.

{% include Configuring-Alluxio-with-HDFS/mvn-package.md %}

* Alternatively, you can also pass the correct Hadoop version in command line when compiling with
maven. For example, if you want Alluxio to work with Hadoop HDFS `2.6.0`:

{% include Configuring-Alluxio-with-HDFS/mvn-Dhadoop-package.md %}

If everything succeeds, you should see
`alluxio-assembly-server-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` created in the
`assembly/server/target` directory and this is the jar file you can use to run both Alluxio Master and
Worker.

## Configuring Alluxio

To run Alluxio binary, we must setup configuration files. Create your configuration file with
`bootstrapConf` command. For example, if you are running Alluxio on your local machine,
`ALLUXIO_MASTER_HOSTNAME` should be set to `localhost`

{% include Configuring-Alluxio-with-HDFS/bootstrapConf.md %}

Alternatively, you can also create the configuration file from the template and set the contents
manually.

{% include Common-Commands/copy-alluxio-env.md %}

Then edit `conf/alluxio-site.properties` file to set the under storage address to the HDFS namenode
address and the HDFS directory you want to mount to Alluxio. For example, the under storage address
can be `hdfs://localhost:9000` if you are running the HDFS namenode locally with default port and
mapping HDFS root directory to Alluxio, or `hdfs://localhost:9000/alluxio/data` if only the HDFS
directory `/alluxio/data` is mapped to Alluxio.

{% include Configuring-Alluxio-with-HDFS/underfs-address.md %}

### Configuring Alluxio with HDFS namenode HA mode

If HDFS namenodes are running in HA mode, both Alluxio servers and clients should be configured
properly in order to access HDFS.

For Alluxio servers (masters and workers), copy `hdfs-site.xml` and `core-site.xml` from your hadoop
installation into `${ALLUXIO_HOME}/conf`. Alternatively, you can set
`alluxio.underfs.hdfs.configuration` to the hadoop property file `hdfs-site.xml` (or
`core-site.xml`) in `conf/alluxio-site.properties` (make sure all the relative configurations are
available in the file).

Then, set the under storage address to `hdfs://nameservice/` (`nameservice` is the name of HDFS
service already configured in `core-site.xml`) if you are mapping HDFS root directory to Alluxio, or
`hdfs://nameservice/alluxio/data` if only the HDFS directory `/alluxio/data` is mapped to Alluxio.

{% include Configuring-Alluxio-with-HDFS/underfs-address-ha.md %}

Next, for Alluxio clients, `alluxio.underfs.hdfs.configuration` should also be set to the hadoop
property file `hdfs-site.xml` (or `core-site.xml`).

## Configuring Permission Mapping between Alluxio and HDFS

Since v1.3, Alluxio supports filesystem [user and permission checking](Security.html) by default.
To ensure that the permission information of files/directories including user, group and mode in HDFS is consistent with Alluxio, the user to start Alluxio master and worker processes **is required** to be either case:

1. [HDFS super user](http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#The_Super-User). Namely, use the same user that starts HDFS namenode process to also start Alluxio master and worker processes.

2. A member of [HDFS superuser group](http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#Configuration_Parameters). Edit HDFS configuration file `hdfs-site.xml` and check the value of configuration property `dfs.permissions.superusergroup`. If this property is set with a group (e.g., "hdfs"), add the user to start Alluxio process (e.g., "alluxio") to this group ("hdfs"); if this property is not set, add a group to this property where your Alluxio running user is a member of this newly added group.

Note that, the user set above is only the identity that starts Alluxio master and worker processes. Once Alluxio servers started, it is **unnecessary** to run your Alluxio client applications using this user.

## Running Alluxio Locally with HDFS

Before this step, please make sure your HDFS cluster is running and the directory mapped to Alluxio
exists. After everything is configured, you can start up Alluxio locally to see that everything
works.

{% include Common-Commands/start-alluxio.md %}

This should start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit HDFS web UI at [http://localhost:50070](http://localhost:50070)
to verify the files and directories created by Alluxio exist. For this test, you should see
files named like: `/default_tests_files/BasicFile_STORE_SYNC_PERSIST`

You can stop Alluxio any time by running:

{% include Common-Commands/stop-alluxio.md %}
