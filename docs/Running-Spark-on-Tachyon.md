---
layout: global
title: Running Spark on Tachyon
---

## Compatibility

By default, Spark 1.0.x is bundled with Tachyon 0.4.1. If you run a different version Tachyon,
please recompile Spark with the right version of Tachyon, by changing the Tachyon version in
spark/core/pom.xml.

## Input/Output data with Tachyon

The additional prerequisite for this part is [Spark](http://spark-project.org/docs/latest/) (0.6 or
later). We also assume that the user is running on Tachyon {{site.TACHYON_RELEASED_VERSION}} or
later and have set up Tachyon and Hadoop in accordance to these guides
[Local Mode](Running-Tachyon-Locally.html) or [Cluster Mode](Running-Tachyon-on-a-Cluster.html).

If you run Spark version smaller than 1.0.0, please edit Spark `spark/conf/spark-env.sh`, add:

    export SPARK_CLASSPATH=/pathToTachyon/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar:$SPARK_CLASSPATH

Create a new file `spark/conf/core-site.xml` Add the following to it

    <configuration>
      <property>
        <name>fs.tachyon.impl</name>
        <value>tachyon.hadoop.TFS</value>
      </property>
    </configuration>

Put a file X into HDFS. Run Spark Shell:

    $ ./spark-shell
    $ val s = sc.textFile("tachyon://localhost:19998/X")
    $ s.count()
    $ s.saveAsTextFile("tachyon://localhost:19998/Y")

Take a look at [http://localhost:19999](http://localhost:19999), there should be a file info
there.

If you are running tachyon in fault tolerant mode along with zookeeper additionally add new entry in
previously created `spark/conf/core-site.xml`

    <property>
        <name>fs.tachyon-ft.impl</name>
        <value>tachyon.hadoop.TFS</value>
    </property>

Edit `spark/conf/spark-env.sh`, add:

    export SPARK_JAVA_OPTS="
      -Dtachyon.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181
      -Dtachyon.usezookeeper=true
      $SPARK_JAVA_OPTS
    "

Put a file X into HDFS. Run Spark Shell, you can now point to any tachyon master:

    $ ./spark-shell
    $ val s = sc.textFile("tachyon-ft://stanbyHost:19998/X")
    $ s.count()
    $ s.saveAsTextFile("tachyon-ft://activeHost:19998/Y")

## Persist Spark RDDs into Tachyon

For this feature, you need to run [Spark](http://spark-project.org/) (1.0 or
later) and Tachyon (0.4.1 or later).  Please refer to
[Spark Doc](http://spark.apache.org/docs/latest/programming-guide.html) on the benefit of this
feature.

Your Spark programs need to set two parameters, `spark.tachyonStore.url` and
`spark.tachyonStore.baseDir`. The `spark.tachyonStore.url` is the URL of the Tachyon
file system in the TachyonStore, with a default value `tachyon://localhost:19998`. The
`spark.tachyonStore.baseDir` is the directories of the Tachyon File System that store RDDs, with a
default value `java.io.tmpdir`. It can be a comma-separated list of multiple directories in Tachyon.

To persist an RDD into Tachyon, you need to persist the RDD with the `StorageLevel.OFF_HEAP`
parameter. The following is an example with Spark shell:

    $ ./spark-shell
    $ val rdd = sc.textFile(inputPath)
    $ rdd.persist(StorageLevel.OFF_HEAP)

Take a look at the `spark.tachyonStore.baseDir` on Tachyon's WebUI (default URI is
[http://localhost:19999](http://localhost:19999)), when the Spark applications is running. There
should be a bunch of files there, they are RDD blocks. Currently, the files will be cleaned up
when the spark application finishes.

You can also use Tachyon as you Spark applications' input and output sources. The above section
shows the instructions.
