---
layout: global
title: Running Tachyon with Yarn on EC2
nickname: Tachyon on EC2 with Yarn
group: User Guide
priority: 5
---

Tachyon can be started and managed by Yarn. This guide demonstrates how to launch Tachyon with Yarn on EC2 machines using the [Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant) that come with Tachyon.

# Prerequisites

* Install Tachyon and Hadoop by following instructions from [Running Tachyon on EC2](Running-Tachyon-on-EC2.html). After this step, you should have Tachyon and Hadoop (v2.4.1 by default) installed on each of your EC2 cluster. 

* Build the Yarn integration (including the client and ApplicationMaster) for Tachyon. Note that, by default Tachyon binaries do not include this YARN integration. You must compile Tachyon with profile yarn specified.

```
$ vagrant ssh TachyonMaster
$ cd /tachyon
$ mvn clean install -Dhadoop.version=2.4.1 -Pyarn
``` 

* Set properties for Tachyon master and worker properties (e.g., tiered storage setup on each worker), one can refer to [Configuration settings](Configuration-Settings.html) for more information. To ensure your configuration can be read by both the ApplicationMaster and Tachyon master/workers, put `tachyon-site.properties` under `${TACHYON_HOME}/conf` on each EC2 machine.
 

# Start Tachyon

Use script `integration/bin/tachyon-yarn.sh` to start Tachyon. This script requires three parameters in order:
1. A dir pointing to `${TACHYON_HOME}` on each machine so Yarn NodeManager could access Tachyon 
scripts and binaries to launch masters and workers. With our EC2 setup, this dir is `/tachyon`.
2. The total number of Tachyon workers to start.
3. A HDFS path to distribute the binaries for Tachyon ApplicationMaster.
 
For example, here we launch a Tachyon cluster with 3 worker nodes, where an HDFS temp directory is `hdfs://TachyonMaster:9000/tmp/` and each Yarn container can access Tachyon in `/tachyon` 

```
$ /tachyon/integration/bin/tachyon-yarn.sh /tachyon 3 hdfs://TachyonMaster:9000/tmp/
```

This script will first upload the binaries with Yarn client and ApplicationMaster to the HDFS path specified, then inform Yarn to run the client binary jar. The script will keep running with ApplicationMaster status reported. You can also check `http://TachyonMaster:8088` in the browser to access the Web UIs and watch the status of Tachyon job as well as the application ID.

NOTE: currently Tachyon Yarn framework does not guarantee to start the Tachyon master on the TachyonMaster machine; use the Yarn Web UI to find out which machine is Tachyon master running on.

# Stop Tachyon

Tachyon can be stopped by using the following Yarn command where the application ID of Tachyon can
 be either retrieved from Yarn web UI or the output of `tachyon-yarn.sh`.

```
$ /hadoop/bin/yarn application -kill TACHYON_APPLICATION_ID
```
