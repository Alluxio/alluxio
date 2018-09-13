---
layout: global
title: Configuring Alluxio with secure HDFS
nickname: Alluxio with secure HDFS
group: Under Stores
priority: 3
---

* Table of Contents
{:toc}

This guide describes the instruction to configure a
[secure HDFS](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SecureMode.html)
as Alluxio's under storage system. Alluxio supports secure HDFS as the under filesystem, with
[Kerberos](http://web.mit.edu/kerberos/) authentication.

> Note: Kerberos authentication with secure HDFS is not Alluxio internal authentication via Kerberos.

## Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio server binaries to each of
these machines. You can either
[download the precompiled binaries directly](http://www.alluxio.org/download)
with the correct Hadoop version (recommended), or
[compile the binaries from Alluxio source code](Building-Alluxio-From-Source.html)
(for advanced users).

Note that, when building Alluxio from source code, by default Alluxio server binaries is built to
work with Apache Hadoop HDFS of version `2.2.0`. To work with Hadoop distributions of other
versions, one needs to specify  the correct Hadoop profile and run the following in your Alluxio
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
[Building Alluxio Master Branch](Building-Alluxio-From-Source.html#distro-support) page for more
information about support for other distributions.

If everything succeeds, you should see
`alluxio-assembly-server-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` created in
the `${ALLUXIO_HOME}/assembly/server/target` directory.


## Configuring Alluxio

### Basic Configuration

First create the configuration file from the template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Then edit `conf/alluxio-site.properties` file to set the under storage address to the HDFS namenode
address and the HDFS directory you want to mount to Alluxio. For example, the under storage address
can be `hdfs://localhost:9000` if you are running the HDFS namenode locally with default port and
mapping HDFS root directory to Alluxio, or `hdfs://localhost:9000/alluxio/data` if only the HDFS
directory `/alluxio/data` is mapped to Alluxio.

```
alluxio.underfs.address=hdfs://<NAMENODE>:<PORT>
```

### HDFS configuration files

To ensure Alluxio servers pick up HDFS configurations in classpath, please copy secure HDFS
configuration xml files (`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`) to
`${ALLUXIO_HOME}/conf/`

### [Optional] Custom Kerberos configuration

Optionally, you can set jvm-level system properties for customized Kerberos configurations:
`java.security.krb5.realm` and `java.security.krb5.kdc`. Those Kerberos configurations route java
libraries to specified Kerberos realm and KDC server address.
If both are set to empty, Kerberos library will respect
the default Kerberos configuration on the machine. These Java system properties should be set
for the Alluxio processes. To do so, you can add to `ALLUXIO_JAVA_OPTS` in `conf/alluxio-env.sh`.

```bash
ALLUXIO_JAVA_OPTS+=" -Djava.security.krb5.realm=<YOUR_KERBEROS_REALM> -Djava.security.krb5.kdc=<YOUR_KERBEROS_KDC_ADDRESS>"
```

### Alluxio server Kerberos credential

Set the following Alluxio properties in `alluxio-site.properties`:

```properties
alluxio.master.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.master.principal=hdfs/<_HOST>@<REALM>
alluxio.worker.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.worker.principal=hdfs/<_HOST>@<REALM>
```

## Running Alluxio Locally with secure HDFS

Before this step, please make sure your HDFS cluster is running and the directory mounted to
Alluxio exists.

Please `kinit` on the Alluxio nodes with the corresponding principal and keytab file
to provide a Kerberos ticket cache. As in the previous example, you should `kinit` with principal `hdfs` and
`<YOUR_HDFS_KEYTAB_FILE_PATH>`. A known limitation is that the Kerberos TGT may expire after
the max renewal lifetime. You can work around this by renewing the TGT periodically. Otherwise you
may see the following error when starting Alluxio service:

```
javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid
credentials provided (Mechanism level: Failed to find any Kerberos tgt)]
```

After everything is configured, you can start up Alluxio locally to see that everything works.

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

For this test to succeed, you need to make sure that the login user of Alluxio cli has
read/write access to the HDFS directory mounted to Alluxio. By default,
the login user is the current user of the host OS. To change the default configuration, set the value of
`alluxio.security.login.username` in `./conf/alluxio-site.properties` to the desired username.
The HDFS directory is specified with `alluxio.underfs.address`.

After this succeeds, you can visit HDFS web UI at [http://localhost:50070](http://localhost:50070)
to verify the files and directories created by Alluxio exist. For this test, you should see
files named like: `/default_tests_files/Basic_CACHE_THROUGH`

You can stop Alluxio any time by running:

```bash
$ bin/alluxio-stop.sh local
```
