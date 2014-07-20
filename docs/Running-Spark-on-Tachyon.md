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
later and has set up Tachyon and Hadoop in accordance to these guides
[Local Mode](Running-Tachyon-Locally.html) or [Cluster Mode](Running-Tachyon-on-a-Cluster.html).

If you are running a Spark version less than 1.0.0, please add the following line to
`spark/conf/spark-env.sh`.

    export SPARK_CLASSPATH=/pathToTachyon/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar:$SPARK_CLASSPATH

Create a new file `spark/conf/core-site.xml` and add the following to it:

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

If you are running tachyon in fault tolerant mode along with ZooKeeper, add a new entry in the
previously created `spark/conf/core-site.xml`:

    <property>
        <name>fs.tachyon-ft.impl</name>
        <value>tachyon.hadoop.TFS</value>
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

Your Spark programs need to set two parameters, `spark.tachyonStore.url` and
`spark.tachyonStore.baseDir`. `spark.tachyonStore.url` (by default `tachyon://localhost:19998`) is
the URL of the Tachyon filesystem in the TachyonStore. `spark.tachyonStore.baseDir` (by default
`java.io.tmpdir`) is the base directory in the Tachyon File System that will store the RDDs. It can
be a comma-separated list of multiple directories in Tachyon.

To persist an RDD into Tachyon, you need to use the `StorageLevel.OFF_HEAP` parameter. The following
is an example with Spark shell:

    $ ./spark-shell
    $ val rdd = sc.textFile(inputPath)
    $ rdd.persist(StorageLevel.OFF_HEAP)

Take a look at the `spark.tachyonStore.baseDir` on Tachyon's WebUI (the default URI is
[http://localhost:19999](http://localhost:19999)), when the Spark application is running. There
should be a bunch of files there; they are RDD blocks. Currently, the files will be cleaned up when
the spark application finishes.

You can also use Tachyon as input and output sources for your Spark applications. The above section
shows the instructions.
