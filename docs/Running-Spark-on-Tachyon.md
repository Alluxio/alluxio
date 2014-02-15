---
layout: global
title: Running Spark on Tachyon
---

The additional prerequisite for this part is [Spark](http://spark-project.org/docs/latest/) (0.6 or
later). We also assume that the user is running on Tachyon 0.4.0 or later and have set up Tachyon
and Hadoop in accordance to these guides [Local Mode](Running-Tachyon-Locally.html) or
[Cluster Mode](Running-Tachyon-on-a-Cluster.html).

Edit Spark `spark/conf/spark-env.sh`, add:

    export SPARK_CLASSPATH=/pathToTachyon/tachyon/target/tachyon-0.4.0-jar-with-dependencies.jar:$SPARK_CLASSPATH

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

If you are running tachyon in fault tolerant mode along with zookeeper additionally add new entry in previously created `spark/conf/core-site.xml`

    <property>
        <name>fs.tachyon-ft.impl</name>
        <value>tachyon.hadoop.TFS</value>
    </property>

Edit `spark/conf/spark-env.sh`, add:

    export SPARK_OPTS="
    -Dtachyon.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181
    $SPARK_OPTS
    "
    
Put a file X into HDFS. Run Spark Shell, you can now point to any tachyon master:

    $ ./spark-shell
    $ val s = sc.textFile("tachyon-ft://stanbyHost:19998/X")
    $ s.count()
    $ s.saveAsTextFile("tachyon-ft://activeHost:19998/Y")