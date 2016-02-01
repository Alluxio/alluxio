---
layout: global
title: Configuring Tachyon with HDFS
nickname: Tachyon with HDFS
group: Under Store
priority: 3
---

This guide describes how to configure Tachyon with
[HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
as the under storage system.

# Initial Setup

To run a Tachyon cluster on a set of machines, you must deploy Tachyon binaries to each of these
machines. You can either
[compile the binaries from Tachyon source code](Building-Tachyon-Master-Branch.html), or
[download the precompiled binaries directly](Running-Tachyon-Locally.html).

Note that, by default, Tachyon binaries are built to work with Hadoop HDFS version `2.2.0`. To use
another Hadoop version, one needs to recompile Tachyon binaries from source code with the correct
Hadoop version set by either of following approaches. Assume `${TACHYON_HOME}` is the root directory
of Tachyon source code.

* Modify the `hadoop.version` tag defined in `${TACHYON_HOME}/pom.xml`. E.g., to work with Hadoop
`2.6.0`, modify this pom file to set "`<hadoop.version>2.6.0</hadoop.version>`" instead of
"`<hadoop.version>2.2.0</hadoop.version>`". Then recompile the source using maven.

```bash
$ mvn clean package
```

* Alternatively, you can also pass the correct Hadoop version in command line when compiling with
maven. For example, if you want Tachyon to work with Hadoop HDFS `2.6.0`:

```bash
$ mvn -Dhadoop.version=2.6.0 clean package
```

If everything succeeds, you should see
`tachyon-assemblies-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar` created in the
`assembly/target` directory and this is the jar file you can use to run both Tachyon Master and Worker.

# Configuring Tachyon

To run Tachyon binary, we must setup configuration files. Create your configuration file from the
template:

```bash
$ cp conf/tachyon-env.sh.template conf/tachyon-env.sh
```

Then edit `tachyon-env.sh` file to set the under storage address to the HDFS namenode address
(e.g., `hdfs://localhost:9000` if you are running the HDFS namenode locally with default port).

```bash
export TACHYON_UNDERFS_ADDRESS=hdfs://NAMENODE:PORT
```

# Running Tachyon Locally with HDFS

After everything is configured, you can start up Tachyon locally to see that everything works.

```bash
$ ./bin/tachyon format
$ ./bin/tachyon-start.sh local
```

This should start one Tachyon master and one Tachyon worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ ./bin/tachyon runTests
```

After this succeeds, you can visit HDFS web UI at [http://localhost:50070](http://localhost:50070)
to verify the files and directories created by Tachyon exist. For this test, you should see
files named like: `/tachyon/data/default_tests_files/BasicFile_STORE_SYNC_PERSIST`

You can stop Tachyon any time by running:

```bash
$ ./bin/tachyon-stop.sh all
```
