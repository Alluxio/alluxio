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

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these machines. You can either [compile the binaries from Alluxio source code](Building-Alluxio-Master-Branch.html), or [download the precompiled binaries directly](http://alluxio.org/downloads/).

Note that, by default, Alluxio binaries are built to work with Hadoop HDFS version `2.2.0`. To use
another Hadoop version, one needs to recompile Alluxio binaries from source code with the correct
Hadoop version set by either of following approaches. Assume `${ALLUXIO_HOME}` is the root directory
of Alluxio source code.

* Modify the `hadoop.version` tag defined in `${ALLUXIO_HOME}/pom.xml`. E.g., to work with Hadoop
`2.6.0`, modify this pom file to set "`<hadoop.version>2.6.0</hadoop.version>`" instead of
"`<hadoop.version>2.2.0</hadoop.version>`". Then recompile the source using maven.
To make compiling faster, you can add `-DskipTests` option to skip unit tests.

{% include Configuring-Alluxio-with-HDFS/mvn-package.md %}

* Alternatively, you can also pass the correct Hadoop version to the command line when compiling
with maven. For example, if you want Alluxio to work with Hadoop HDFS `2.6.0`:

{% include Configuring-Alluxio-with-HDFS/mvn-Dhadoop-package.md %}

If everything succeeds, you should see
`alluxio-assemblies-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` created in the
`assembly/target` directory. This is the jar file you can use to run both Alluxio Master and Worker.

## Configuring Alluxio

To run Alluxio binary, we must setup configuration files. Create your configuration file with `bootstrapConf` command.
For example, if you are running Alluxio on your local machine, `ALLUXIO_MASTER_HOSTNAME` should be set to `localhost`

{% include Configuring-Alluxio-with-HDFS/bootstrapConf.md %}

Alternatively, you can also create the configuration file from the template and set the contents manually.

{% include Common-Commands/copy-alluxio-env.md %}

Then edit `conf/alluxio-site.properties` file to set the under storage address to the HDFS namenode address
and the HDFS directory you want to mount to Alluxio. For example, the under storage address can be
`hdfs://localhost:9000` if you are running the HDFS namenode locally with default port and mounting HDFS root directory to Alluxio,
or `hdfs://localhost:9000/alluxio/data` if only the HDFS directory `/alluxio/data` is mounted to Alluxio.

{% include Configuring-Alluxio-with-HDFS/underfs-address.md %}

### HDFS configuration files

To ensure Alluxio client picks up HDFS configurations in classpath, please copy secure HDFS configuration xml files
(`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`) to `${ALLUXIO_HOME}/conf/`

### Kerberos configuration

Optionally, you can set jvm-level system properties for customized Kerberos configurations:
`java.security.krb5.realm` and `java.security.krb5.kdc`. Those Kerberos configurations route java libraries to specified Kerberos realm and KDC server address.
If both are set to empty, Kerberos library will respect
the default Kerberos configuration on the machine. For example:

* If you use Hadoop, you can add to `HADOOP_OPTS` in `{HADOOP_CONF_DIR}/hadoop-env.sh`.

{% include Configuring-Alluxio-with-secure-HDFS/hadoop-opts.md %}

* If you use Spark, you can add to `SPARK_JAVA_OPTS` in `{SPARK_CONF_DIR}/spark-env.sh`.

{% include Configuring-Alluxio-with-secure-HDFS/spark-opts.md %}

* If you use Alluxio shell, you can add to `ALLUXIO_JAVA_OPTS` in `conf/alluxio-env.sh`.

{% include Configuring-Alluxio-with-secure-HDFS/alluxio-opts.md %}

### Alluxio server Kerberos credential

Set the following Alluxio properties in `alluxio-site.properties`:

{% include Configuring-Alluxio-with-secure-HDFS/alluxio-properties-for-secure-hdfs-kerberos.md %}

Alternatively, these configuration settings can be set in the `conf/alluxio-env.sh` file. More
details about setting configuration parameters can be found in
[Configuration Settings](Configuration-Settings.html).

## Running Alluxio Locally with secure HDFS

Before this step, please make sure your HDFS cluster is running and the directory mounted to Alluxio exists.

Please `kinit` on the Alluxio nodes with the corresponding master/worker principal and keytab file
to provide a Kerberos ticket cache. A known limitation is that the Kerberos TGT may expire after the max renewal lifetime. You can work around this by renewing the TGT periodically. Otherwise you may see the following error when starting Alluxio service:

```
javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)]
```

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit HDFS web UI at [http://localhost:50070](http://localhost:50070)
to verify the files and directories created by Alluxio exist. For this test, you should see
files named like: `/default_tests_files/Basic_CACHE_THROUGH`

You can stop Alluxio any time by running:

{% include Common-Commands/stop-alluxio.md %}
