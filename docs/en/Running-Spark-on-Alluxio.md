---
layout: global
title: Running Spark on Alluxio
nickname: Apache Spark
group: Frameworks
priority: 0
---

This guide describes how to run [Apache Spark](http://spark-project.org/) on Alluxio and use HDFS as
a running example of Alluxio under storage system. Note that, Alluxio supports many other under
storage systems in addition to HDFS and enables frameworks like Spark to read data from or write
data to those systems.

## Compatibility

If the versions of Spark and Alluxio form one of the the following pairs, they will work together
out-of-the-box.

<table class="table table-striped">
<tr><th>Spark Version</th><th>Alluxio Version</th></tr>
{% for item in site.data.table.versions-of-Spark-and-Alluxio %}
<tr>
  <td>{{item.Spark-Version}}</td>
  <td>{{item.Alluxio-Version}}</td>
</tr>
{% endfor %}
</table>

If the version of Spark is not supported by your Alluxio installation by default (e.g., you are
trying out the latest Alluxio release with some older Spark installation), one can recompile Spark
by updating the correct version of alluxio-core-client in Spark dependency. To do that, edit
`spark/core/pom.xml` and change the dependency version of `alluxio-core-client` to
`your_alluxio_version`:

{% include Running-Spark-on-Alluxio/your_Alluxio_version.md %}

## Prerequisites

* Make sure your Spark installation is compatible with Alluxio installation. Check
[Compatibility](#compatibility) session for more instructions.
* Alluxio cluster has been set up in accordance to these guides for either
[Local Mode](Running-Alluxio-Locally.html) or [Cluster Mode](Running-Alluxio-on-a-Cluster.html).
* If Spark version is earlier than `1.0.0`, please add the following line to
`spark/conf/spark-env.sh`.

{% include Running-Spark-on-Alluxio/earlier-spark-version-bash.md %}

* If Alluxio is run on top of a Hadoop 1.x cluster, create a new file `spark/conf/core-site.xml`
with the following content:

{% include Running-Spark-on-Alluxio/Hadoop-1.x-configuration.md %}


* If you are running alluxio in fault tolerant mode with zookeeper and the Hadoop cluster is a 1.x,
add the following additionally entry to the previously created `spark/conf/core-site.xml`:

{% include Running-Spark-on-Alluxio/fault-tolerant-mode-with-zookeeper-xml.md %}

and the following line to `spark/conf/spark-env.sh`:

{% include Running-Spark-on-Alluxio/fault-tolerant-mode-with-zookeeper-bash.md %}

## Use Alluxio as Input and Output

This section shows how to use Alluxio as input and output sources for your Spark applications.

Put a file `foo` into HDFS, assuming namenode is running on `localhost`:

{% include Running-Spark-on-Alluxio/foo.md %}

Run the following commands from `spark-shell`, assuming Alluxio Master is running on `localhost`:

{% include Running-Spark-on-Alluxio/Alluxio-In-Out-Scala.md %}

Open your browser and check [http://localhost:19999](http://localhost:19999). There should be an
output file `bar` which doubles each line in the file `foo`.

When running Alluxio with fault tolerant mode, you can point to any Alluxio master:

{% include Running-Spark-on-Alluxio/any-Alluxio-master.md %}

## Persist Spark RDDs into Alluxio

This feature requires Spark 1.0 or later and Alluxio 0.4.1 or later.  Please refer to
[Spark guide](http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence) for
more details about RDD persistence.

To persist Spark RDDs, your Spark programs need to have two parameters set:
`spark.externalBlockStore.url` and `spark.externalBlockStore.baseDir`.

* `spark.externalBlockStore.url` is the URL of the Alluxio Filesystem in the AlluxioStore. By
default, its value is `alluxio://localhost:19998`.
* `spark.externalBlockStore.baseDir` is the base directory in the Alluxio Filesystem to store the
RDDs. It can be a comma-separated list of multiple directories in Alluxio. By default, its value is
`java.io.tmpdir`.

To persist an RDD into Alluxio, you need to pass the `StorageLevel.OFF_HEAP` parameter. The
following is an example with Spark shell:

{% include Running-Spark-on-Alluxio/off-heap-Spark-shell.md %}

Check the `spark.externalBlockStore.baseDir` using Alluxio's web UI (the default URI is
[http://localhost:19999](http://localhost:19999)), when the Spark application is running. You should
see a number of files there; they are the persisted RDD blocks. Currently, the files will be cleaned
up when the Spark application finishes.

## Data Locality

If Spark task locality is `ANY` while it should be `NODE_LOCAL`, it is probably because Alluxio and
Spark use different network address representations, maybe one of them uses hostname while
another uses IP address. Please refer to [this jira ticket](
https://issues.apache.org/jira/browse/SPARK-10149) for more details, where you can find solutions
from the Spark community.

Note: Alluxio uses hostname to represent network address except in version 0.7.1 where IP address is
used. Spark v1.5.x ships with Alluxio v0.7.1 by default, in this case, by default, Spark and Alluxio
both use IP address to represent network address, so data locality should work out of the box.
But since release 0.8.0, to be consistent with HDFS, Alluxio represents network address by hostname.
There is a workaround when launching Spark to achieve data locality. Users can explicitly specify
hostnames by using the following script offered in Spark. Start Spark Worker in each slave node with
slave-hostname:

{% include Running-Spark-on-Alluxio/slave-hostname.md %}

For example:

{% include Running-Spark-on-Alluxio/slave-hostname-example.md %}

You can also set the `SPARK_LOCAL_HOSTNAME` in `$SPARK_HOME/conf/spark-env.sh` to achieve this. For
example:

{% include Running-Spark-on-Alluxio/spark-local-hostname-example.md %}

In either way, the Spark Worker addresses become hostnames and Locality Level becomes NODE_LOCAL as shown
in Spark WebUI below.

![hostname]({{site.data.img.screenshot_datalocality_sparkwebui}})

![locality]({{site.data.img.screenshot_datalocality_tasklocality}})

