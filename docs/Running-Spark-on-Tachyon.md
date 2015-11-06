---
layout: global
title: Running Spark on Tachyon
nickname: Apache Spark
group: Frameworks
priority: 0
---

This guide describes how to run [Apache Spark](http://spark-project.org/) on Tachyon and use HDFS as
a running example of Tachyon under storage system. Note that, Tachyon supports many other under
storage systems in addition to HDFS and enables frameworks like Spark to read data from or write
data to those systems.

## Compatibility

If the versions of Spark and Tachyon form one of the the following pairs, they will work together
out-of-the-box.

<table class="table">
<tr><th>Spark Version</th><th>Tachyon Version</th></tr>
<tr>
  <td> 1.0.x and Below </td>
  <td> v0.4.1 </td>
</tr>
<tr>
  <td> 1.1.x </td>
  <td> v0.5.0 </td>
</tr>
<tr>
  <td> 1.2.x </td>
  <td> v0.5.0 </td>
</tr>
<tr>
  <td> 1.3.x </td>
  <td> v0.5.0 </td>
</tr>
<tr>
  <td> 1.4.x </td>
  <td> v0.6.4 </td>
</tr>
<tr>
  <td> 1.5.x </td>
  <td> v0.7.1 </td>
</tr>
</table>

If the version of Spark is not supported by your Tachyon installation by default (e.g., you are
trying out the latest Tachyon release with some older Spark installation), one can recompile Spark
by updating the correct version of tachyon-client in Spark dependency. To do that, edit
`spark/core/pom.xml` and change the dependency version of `tachyon-client` to
`your_tachyon_version`:

```xml
<dependency>
  <groupId>org.tachyonproject</groupId>
  <artifactId>tachyon-client</artifactId>
  <version>your_tachyon_version</version>
  ...
</dependency>
```

## Prerequisites

* Make sure your Spark installation is compatible with Tachyon installation. Check
[Compatibility](#compatibility) session for more instructions.
* Tachyon cluster has been set up in accordance to these guides for either
[Local Mode](Running-Tachyon-Locally.html) or [Cluster Mode](Running-Tachyon-on-a-Cluster.html).
* If Spark version is earlier than `1.0.0`, please add the following line to
`spark/conf/spark-env.sh`.

```bash
export SPARK_CLASSPATH=/pathToTachyon/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar:$SPARK_CLASSPATH
```

* If Tachyon is run on top of a Hadoop 1.x cluster, create a new file `spark/conf/core-site.xml`
with the following content:

```xml
<configuration>
  <property>
    <name>fs.tachyon.impl</name>
    <value>tachyon.hadoop.TFS</value>
  </property>
</configuration>
```


* If you are running tachyon in fault tolerant mode with zookeeper and the Hadoop cluster is a 1.x,
add the following additionally entry to the previously created `spark/conf/core-site.xml`:

```xml
<property>
  <name>fs.tachyon-ft.impl</name>
  <value>tachyon.hadoop.TFSFT</value>
</property>
```

and the following line to `spark/conf/spark-env.sh`:

```bash
export SPARK_JAVA_OPTS="
  -Dtachyon.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181 \
  -Dtachyon.zookeeper.enabled=true \
  $SPARK_JAVA_OPTS
"
```

## Use Tachyon as Input and Output

This section shows how to use Tachyon as input and output sources for your Spark applications.

Put a file `foo` into HDFS, assuming namenode is running on `localhost`:

```bash
$ hadoop fs -put -f foo hdfs://localhost:9000/foo
```

Run the following commands from `spark-shell`, assuming Tachyon Master is running on `localhost`:

```scala
> val s = sc.textFile("tachyon://localhost:19998/foo")
> val double = s.map(line => line + line)
> double.saveAsTextFile("tachyon://localhost:19998/bar")
```

Open your browser and check [http://localhost:19999](http://localhost:19999). There should be an
output file `bar` which doubles each line in the file `foo`.

When running Tachyon with fault tolerant mode, you can point to any Tachyon master:

```scala
> val s = sc.textFile("tachyon-ft://stanbyHost:19998/foo")
> val double = s.map(line => line + line)
> double.saveAsTextFile("tachyon-ft://activeHost:19998/bar")
```

## Persist Spark RDDs into Tachyon

This feature requires Spark 1.0 or later and Tachyon 0.4.1 or later.  Please refer to
[Spark guide](http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence) for
more details about RDD persistence.

To persist Spark RDDs, your Spark programs need to have two parameters set:
`spark.externalBlockStore.url` and `spark.externalBlockStore.baseDir`.

* `spark.externalBlockStore.url` is the URL of the Tachyon Filesystem in the TachyonStore. By
default, its value is `tachyon://localhost:19998`.
* `spark.externalBlockStore.baseDir` is the base directory in the Tachyon Filesystem to store the
RDDs. It can be a comma-separated list of multiple directories in Tachyon. By default, its value is
`java.io.tmpdir`.

To persist an RDD into Tachyon, you need to pass the `StorageLevel.OFF_HEAP` parameter. The
following is an example with Spark shell:

```bash
$ ./spark-shell
> val rdd = sc.textFile(inputPath)
> rdd.persist(StorageLevel.OFF_HEAP)
```

Check the `spark.externalBlockStore.baseDir` using Tachyon's web UI (the default URI is
[http://localhost:19999](http://localhost:19999)), when the Spark application is running. You should
see a number of files there; they are the persisted RDD blocks. Currently, the files will be cleaned
up when the Spark application finishes.
