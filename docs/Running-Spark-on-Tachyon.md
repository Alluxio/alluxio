---
layout: global
title: Running Spark on Tachyon
---

## Compatibility

If you plan to run Spark on Tachyon, the following version pairings will work together
out-of-the-box. If you plan to use a different version than the default supported version, please
recompile Spark with the right version of tachyon-client by changing the version in
`spark/core/pom.xml`.

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
  <td> 1.5.x and Above </td>
  <td> v0.7.1 </td>
</tr>
</table>

## Input/Output data with Tachyon

The additional prerequisite for this part is [Spark](http://spark-project.org/docs/latest/) (0.6 or
later). We also assume that the user is running on Tachyon {{site.TACHYON_RELEASED_VERSION}} or
later and has set up Tachyon and Hadoop in accordance to these guides
[Local Mode](Running-Tachyon-Locally.html) or [Cluster Mode](Running-Tachyon-on-a-Cluster.html).

If you are running a Spark version less than 1.0.0, please add the following line to
`spark/conf/spark-env.sh`.

    export SPARK_CLASSPATH=/pathToTachyon/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar:$SPARK_CLASSPATH

If running a hadoop 1.x cluster, create a new file `spark/conf/core-site.xml` Add the following to it:

    <configuration>
      <property>
        <name>fs.tachyon.impl</name>
        <value>tachyon.hadoop.TFS</value>
      </property>
    </configuration>

Put a file X into HDFS and run the Spark shell:

    $ ./spark-shell
    $ val s = sc.textFile("tachyon://localhost:19998/X")
    $ s.count()
    $ s.saveAsTextFile("tachyon://localhost:19998/Y")

Take a look at [http://localhost:19999](http://localhost:19999). There should be an output file
`Y` which contains the number of words in the file `X`.
Put a file X into HDFS and run the Spark shell:

If you are invoking spark job using sbt or from other frameworks like play using sbt:

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.tachyon.impl", "tachyon.hadoop.TFS")


If you are running tachyon in fault tolerant mode with zookeeper and the hadoop cluster is a 1.x cluster,
additionally add new entry in previously created `spark/conf/core-site.xml`:

    <property>
        <name>fs.tachyon-ft.impl</name>
        <value>tachyon.hadoop.TFSFT</value>
    </property>

Add the following line to `spark/conf/spark-env.sh`:

    export SPARK_JAVA_OPTS="
      -Dtachyon.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181
      -Dtachyon.usezookeeper=true
      $SPARK_JAVA_OPTS
    "

Put a file X into HDFS. When running a Spark shell, you can now point to any tachyon master:

    $ ./spark-shell
    $ val s = sc.textFile("tachyon-ft://stanbyHost:19998/X")
    $ s.count()
    $ s.saveAsTextFile("tachyon-ft://activeHost:19998/Y")

## Persist Spark RDDs into Tachyon

For this feature, you need to run [Spark](http://spark-project.org/) (1.0 or
later) and Tachyon (0.4.1 or later).  Please refer to
[Spark Doc](http://spark.apache.org/docs/latest/programming-guide.html) on the benefit of this
feature.

Your Spark programs need to set two parameters, `spark.externalBlockStore.url` and
`spark.externalBlockStore.baseDir`. `spark.externalBlockStore.url` (by default `tachyon://localhost:19998`) is
the URL of the Tachyon filesystem in the TachyonStore. `spark.externalBlockStore.baseDir` (by default
`java.io.tmpdir`) is the base directory in the Tachyon File System that will store the RDDs. It can
be a comma-separated list of multiple directories in Tachyon.

To persist an RDD into Tachyon, you need to use the `StorageLevel.OFF_HEAP` parameter. The following
is an example with Spark shell:

    $ ./spark-shell
    $ val rdd = sc.textFile(inputPath)
    $ rdd.persist(StorageLevel.OFF_HEAP)

Take a look at the `spark.externalBlockStore.baseDir` on Tachyon's WebUI (the default URI is
[http://localhost:19999](http://localhost:19999)), when the Spark application is running. There
should be a bunch of files there; they are RDD blocks. Currently, the files will be cleaned up when
the spark application finishes.

You can also use Tachyon as input and output sources for your Spark applications. The above section
shows the instructions.
