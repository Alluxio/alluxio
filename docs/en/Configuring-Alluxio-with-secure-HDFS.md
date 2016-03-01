---
layout: global
title: Configuring Alluxio with secure HDFS
nickname: Alluxio with secure HDFS
group: Under Store
priority: 3
---

This guide
describes how to configure Alluxio with secure [HDFS](https://hadoop.apache
.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide
.html)
as the under storage system. Alluxio supports secure HDFS as the under filesystem, with
[Kerberos](http://web.mit.edu/kerberos/) authentication.

Note: Kerberos authentication with secure HDFS is not Alluxio internal authentication via Kerberos.

# Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines. You can either
[compile the binaries from Alluxio source code](Building-Alluxio-Master-Branch.html), or
[download the precompiled binaries directly](http://alluxio.org/downloads/).

Note that, by default, Alluxio binaries are built to work with Hadoop HDFS version `2.2.0`. To use
another Hadoop version, one needs to recompile Alluxio binaries from source code with the correct
Hadoop version set by either of following approaches. Assume `${ALLUXIO_HOME}` is the root directory
of Alluxio source code.

* Modify the `hadoop.version` tag defined in `${ALLUXIO_HOME}/pom.xml`. E.g., to work with Hadoop
`2.6.0`, modify this pom file to set "`<hadoop.version>2.6.0</hadoop.version>`" instead of
"`<hadoop.version>2.2.0</hadoop.version>`". Then recompile the source using maven.

{% include Configuring-Alluxio-with-HDFS/mvn-package.md %}

* Alternatively, you can also pass the correct Hadoop version to the command line when compiling
with maven. For example, if you want Alluxio to work with Hadoop HDFS `2.6.0`:

{% include Configuring-Alluxio-with-HDFS/mvn-Dhadoop-package.md %}

If everything succeeds, you should see
`alluxio-assemblies-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` created in the
`assembly/target` directory. This is the jar file you can use to run both Alluxio Master and Worker.

# Configuring Alluxio

To run the Alluxio binary, we must setup configuration files. Create your configuration file from
 the template:

{% include Common-Commands/copy-alluxio-env.md %}

Then edit `alluxio-env.sh` file to set the under storage address to the HDFS namenode address
(e.g., `hdfs://localhost:9000` if you are running the HDFS namenode locally with the default port).

{% include Configuring-Alluxio-with-HDFS/underfs-address.md %}

Copy secure HDFS conf xml files (`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`) to
`${ALLUXIO_HOME}/conf/`

Set the following Alluxio properties:

{% include Configuring-Alluxio-with-secure-HDFS/alluxio-properties-for-secure-hdfs-kerberos.md %}

# Running Alluxio Locally with secure HDFS

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit HDFS web UI at [http://localhost:50070](http://localhost:50070)
to verify the files and directories created by Alluxio exist. For this test, you should see
files named like: `/alluxio/data/default_tests_files/BasicFile_STORE_SYNC_PERSIST`

You can stop Alluxio any time by running:

{% include Common-Commands/stop-alluxio.md %}
