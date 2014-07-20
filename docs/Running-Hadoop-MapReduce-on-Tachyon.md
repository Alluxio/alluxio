---
layout: global
title: Running Hadoop MapReduce on Tachyon
---

This guide describes how to get Tachyon running with Hadoop MapReduce, so that you can easily use
your MapReduce programs with files stored on Tachyon.

# Prerequisites

The prerequisite for this part is that you have
[Java](https://github.com/amplab/tachyon/wiki/Java-setup/). We also assume that you have set up
Tachyon and Hadoop in accordance to these guides [Local Mode](Running-Tachyon-Locally.html) or
[Cluster Mode](Running-Tachyon-on-a-Cluster.html)

Ensure that the `hadoop/conf/core-site.xml` file in your Hadoop installation's conf directory has
the following property added:

    <property>
      <name>fs.tachyon.impl</name>
      <value>tachyon.hadoop.TFS</value>
    </property>

This will allow your MapReduce jobs to use Tachyon for their input and output files. If you are
using HDFS as the underlying store for Tachyon, it may be necessary to add this property to the
`hdfs-site.xml` conf file as well.

# Distributing Tachyon Executables

In order for the MapReduce job to be able to use files via Tachyon, we will need to distribute the
Tachyon jar amongst all the nodes in the cluster. This will allow the TaskTracker and JobClient to
have all the requisite executables to interface with Tachyon.

We are presented with three options that for distributing the jars as outlined by this
[guide](http://blog.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/)
from Cloudera.

Assuming that Tachyon will be used prominently, it is best to ensure that the Tachyon jar will
permanently reside on each node, so that we do not rely on the Hadoop DistributedCache to avoid the
network costs of distributing the jar for each job (Option 1), and don't significantly increase our
job jar size by packaging Tachyon with it (Option 2). For this reason, of the three options laid
out, it is highly recommended to consider the third route, by installing the Tachyon jar on each
node.

-   For installing Tachyon on each node, you must place the
    `tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar`, located in the
    `tachyon/client/target` directory, in the `$HADOOP_HOME/lib` directory of each node, and then
    restart all of the TaskTrackers. One downfall of this approach is that the jars must be
    installed again for each update to a new release.

-   You can also run a job by using the `-libjars` command line option when using `hadoop jar...`, and
    specifying
    `/pathToTachyon/core/target/tachyon-client={{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar`
    as the argument. This will place the jar in the Hadoop DistributedCache, and is desirable only
    if you are updating the Tachyon jar a non-trivial number of times.

-   For those interested in the second option, please revisit the Cloudera guide for more assistance.
    One must simply package the Tachyon jar in the `lib` subdirectory of the job jar. This option is
    the most undesirable since for every change in Tachyon, we must recreate the job jar, thereby
    incurring a network cost for every job by increasing the size of the job jar.

In order to make the Tachyon executables available to the JobClient, one can also install the
Tachyon jar in the `$HADOOP_HOME/lib` directory, or modify `HADOOP_CLASSPATH` by changing `hadoop-env.sh` to:

    $ export HADOOP_CLASSPATH=/pathToTachyon/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar

This will allow the code that creates the Job and submits it to reference Tachyon if necessary.

# Example

For simplicity, we will assume a psuedo-distributed Hadoop cluster.

    $ cd $HADOOP_HOME
    $ ./bin/stop-all.sh
    $ ./bin/start-all.sh

Because we have a psuedo-distributed cluster, copying the Tachyon jar into `$HADOOP_HOME/lib` makes
the Tachyon executables available to both the TaskTrackers and the JobClient. We can now verify that
it is working by the following:

    $ cd $HADOOP_HOME
    $ ./bin/hadoop jar hadoop-examples-1.0.4.jar wordcount -libjars /pathToTachyon/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar tachyon://localhost:19998/X tachyon://localhost:19998/X-wc

Where X is some file on Tachyon and, the results of the wordcount job is in the X-wc directory.

For example, say you have text files in HDFS directory `/user/hduser/gutenberg/`. You can run the
following:

    $ cd $HADOOP_HOME
    $ ./bin/hadoop jar hadoop-examples-1.0.4.jar wordcount -libjars /pathToTachyon/client/target/tachyon-client-{{site.TACHYON_RELEASED_VERSION}}-jar-with-dependencies.jar tachyon://localhost:19998/user/hduser/gutenberg tachyon://localhost:19998/user/hduser/output

The above command tell the wordcount to load the files from HDFS directory `/user/hduser/gutenberg/`
into Tachyon and then save the output result to `/user/hduser/output/` in Tachyon.
