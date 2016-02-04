---
layout: global
title: Configuring Alluxio with HDFS
nickname: Alluxio with HDFS
group: Under Store
priority: 3
---

This guide describes how to configure Alluxio with
[HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
as the under storage system.

# Initial Setup

To run a Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines. You can either
[compile the binaries from Alluxio source code](Building-Alluxio-Master-Branch.html), or
[download the precompiled binaries directly](Running-Alluxio-Locally.html).

Note that, by default, Alluxio binaries are built to work with Hadoop HDFS version `2.2.0`. To use
another Hadoop version, one needs to recompile Alluxio binaries from source code with the correct
Hadoop version set by either of following approaches. Assume `${TACHYON_HOME}` is the root directory
of Alluxio source code.

* Modify the `hadoop.version` tag defined in `${TACHYON_HOME}/pom.xml`. E.g., to work with Hadoop
`2.6.0`, modify this pom file to set "`<hadoop.version>2.6.0</hadoop.version>`" instead of
"`<hadoop.version>2.2.0</hadoop.version>`". Then recompile the source using maven.

{% include Configuring-Alluxio-with-HDFS/mvn-package.md %}

* Alternatively, you can also pass the correct Hadoop version in command line when compiling with
maven. For example, if you want Alluxio to work with Hadoop HDFS `2.6.0`:

{% include Configuring-Alluxio-with-HDFS/mvn-Dhadoop-package.md %}

If everything succeeds, you should see
`tachyon-assemblies-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar` created in the
`assembly/target` directory and this is the jar file you can use to run both Alluxio Master and Worker.

# Configuring Alluxio

To run Alluxio binary, we must setup configuration files. Create your configuration file from the
template:

{% include Configuring-Alluxio-with-HDFS/copy-tachyon-env.md %}

Then edit `tachyon-env.sh` file to set the under storage address to the HDFS namenode address
(e.g., `hdfs://localhost:9000` if you are running the HDFS namenode locally with default port).

{% include Configuring-Alluxio-with-HDFS/underfs-address.md %}

# Running Alluxio Locally with HDFS

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Configuring-Alluxio-with-HDFS/start-tachyon.md %}

This should start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Configuring-Alluxio-with-HDFS/runTests.md %}

After this succeeds, you can visit HDFS web UI at [http://localhost:50070](http://localhost:50070)
to verify the files and directories created by Alluxio exist. For this test, you should see
files named like: `/tachyon/data/default_tests_files/BasicFile_STORE_SYNC_PERSIST`

You can stop Alluxio any time by running:

{% include Configuring-Alluxio-with-HDFS/stop-tachyon.md %}
