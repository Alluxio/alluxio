---
layout: global
title: Configuring Alluxio with secure HDFS
nickname: Alluxio with secure HDFS
group: Under Store
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

### Basic Configuration

First create the configuration file from the template.

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Then edit `conf/alluxio-site.properties` file to set the under storage address to the HDFS namenode
address and the HDFS directory you want to mount to Alluxio. For example, the under storage address
can be `hdfs://localhost:9000` if you are running the HDFS namenode locally with default port and
mapping HDFS root directory to Alluxio, or `hdfs://localhost:9000/alluxio/data` if only the HDFS
directory `/alluxio/data` is mapped to Alluxio.

```
alluxio.underfs.address=hdfs://NAMENODE:PORT
```

### HDFS configuration files

To ensure Alluxio client picks up HDFS configurations in classpath, please copy secure HDFS
configuration xml files (`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`) to
`${ALLUXIO_HOME}/conf/`

### Kerberos configuration

Optionally, you can set jvm-level system properties for customized Kerberos configurations:
`java.security.krb5.realm` and `java.security.krb5.kdc`. Those Kerberos configurations route java
libraries to specified Kerberos realm and KDC server address.
If both are set to empty, Kerberos library will respect
the default Kerberos configuration on the machine. For example:

* If you use Hadoop, you can add to `HADOOP_OPTS` in `{HADOOP_CONF_DIR}/hadoop-env.sh`.

```bash
$ export HADOOP_OPTS="$HADOOP_OPTS -Djava.security.krb5.realm=<YOUR_KERBEROS_REALM> -Djava.security.krb5.kdc=<YOUR_KERBEROS_KDC_ADDRESS>"
```

* If you use Spark, you can add to `SPARK_JAVA_OPTS` in `{SPARK_CONF_DIR}/spark-env.sh`.

```properties
SPARK_JAVA_OPTS+=" -Djava.security.krb5.realm=<YOUR_KERBEROS_REALM> -Djava.security.krb5.kdc=<YOUR_KERBEROS_KDC_ADDRESS>"
```

* If you use Alluxio shell, you can add to `ALLUXIO_JAVA_OPTS` in `conf/alluxio-env.sh`.

```properties
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

Alternatively, these configuration settings can be set in the `conf/alluxio-env.sh` file. More
details about setting configuration parameters can be found in
[Configuration Settings](Configuration-Settings.html).

## Running Alluxio Locally with secure HDFS

Before this step, please make sure your HDFS cluster is running and the directory mounted to
Alluxio exists.

Please `kinit` on the Alluxio nodes with the corresponding master/worker principal and keytab file
to provide a Kerberos ticket cache. A known limitation is that the Kerberos TGT may expire after
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

After this succeeds, you can visit HDFS web UI at [http://localhost:50070](http://localhost:50070)
to verify the files and directories created by Alluxio exist. For this test, you should see
files named like: `/default_tests_files/Basic_CACHE_THROUGH`

You can stop Alluxio any time by running:

```bash
$ bin/alluxio-stop.sh local
```
