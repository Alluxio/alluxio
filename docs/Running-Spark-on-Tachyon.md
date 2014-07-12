---
layout: global
title: Running Spark on Tachyon
---

## Get Spark running on Tachyon

The additional prerequisite for this part is [Spark](http://spark-project.org/docs/latest/) (0.6 or
later). We also assume that the user is running on Tachyon {{site.TACHYON_RELEASED_VERSION}} or
later and have set up Tachyon and Hadoop in accordance to these guides
[Local Mode](Running-Tachyon-Locally.html) or [Cluster Mode](Running-Tachyon-on-a-Cluster.html).

Edit Spark `spark/conf/spark-env.sh`, add:

    export SPARK_CLASSPATH=/pathToTachyon/tachyon/target/tachyon-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar:$SPARK_CLASSPATH

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

The additional prerequisite for this part is [Spark](http://spark-project.org/docs/latest/) (1.0 or
later). We also assume that the user is running on Tachyon {{site.TACHYON_RELEASED_VERSION}} or
later and have set up Tachyon and Hadoop in accordance to these guides
[Local Mode](Running-Tachyon-Locally.html) or [Cluster Mode](Running-Tachyon-on-a-Cluster.html).

There are two parameters need to be set in your programs, namely `spark.tachyonStore.url` and
`spark.tachyonStore.baseDir`. The `spark.tachyonStore.url` is the URL of the underlying Tachyon
file system in the TachyonStore. It’s default value is tachyon://localhost:19998 etc. The
`spark.tachyonStore.baseDir` is the directories of the Tachyon File System that store RDDs. The
Tachyon file system's URL is set by `spark.tachyonStore.url`. It can be a comma-separated list of
multiple directories on Tachyon file system. If not set, the value of the system property of
"java.io.tmpdir" will be used by default.

To persist an RDD into Tachyon is easy. You just need to persist the RDD with the
`StorageLevel.OFF_HEAP` parameter. Run Spark Shell:

    $ ./spark-shell
    $ val rdd = sc.textFile(inputPath)
    $ rdd.persist(StorageLevel.OFF_HEAP)

Take a look at the `spark.tachyonStore.baseDir` on Tachyon's WebUI, like
[http://localhost:19999](http://localhost:19999), during the Spark applications is running. There
should be a bunch of files there, they are RDD blocks. Currently, the files will be cleaned up
when the spark application is finished.

You can also adopt Tachyon as you Spark applications' input and output sources. The directions are
shown in the previous section.
