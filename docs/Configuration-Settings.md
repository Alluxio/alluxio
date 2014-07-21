---
layout: global
title: Configuration Settings
---

Tachyon configuration parameters fall into four categories: Master, Worker, Common (Master and
Worker), and User configurations. The environment configuration file responsible for setting system
properties is under `conf/tachyon-env.sh`. These variables should be set as variables under the
`TACHYON_JAVA_OPTS` definition. A template is provided with the zip: `conf/tachyon-env.sh.template`.

Additional Java VM options can be added to `TACHYON_MASTER_JAVA_OPTS` for Master and
`TACHYON_WORKER_JAVA_OPTS` for Worker configuration. In the template file, `TACHYON_JAVA_OPTS` is
included in both `TACHYON_MASTER_JAVA_OPTS` and `TACHYON_WORKER_JAVA_OPTS`.

For example if you would like to enable Java remote debugging at port 7001 in the Master you can modify 
`TACHYON_MASTER_JAVA_OPTS` like this:

`export TACHYON_MASTER_JAVA_OPTS="$TACHYON_JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=7001"`

# Common Configuration

The common configuration contains constants which specify paths and the log appender name.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.home</td>
  <td>"/mnt/tachyon_default_home"</td>
  <td>Tachyon installation folder.</td>
</tr>
<tr>
  <td>tachyon.underfs.address</td>
  <td>$tachyon.home + "/underfs"</td>
  <td>Tachyon folder in the underlayer file system.</td>
</tr>
<tr>
  <td>tachyon.data.folder</td>
  <td>$tachyon.underfs.address + "/tachyon/data"</td>
  <td>Tachyon's data folder in the underlayer file system.</td>
</tr>
<tr>
  <td>tachyon.workers.folder</td>
  <td>$tachyon.underfs.address + "/tachyon/workers"</td>
  <td>Tachyon's workers folders in the underlayer file system.</td>
</tr>
<tr>
  <td>tachyon.usezookeeper</td>
  <td>false</td>
  <td>If setup master fault tolerant mode using ZooKeeper.</td>
</tr>
<tr>
  <td>tachyon.zookeeper.address</td>
  <td>null</td>
  <td>ZooKeeper address for master fault tolerance.</td>
</tr>
<tr>
  <td>tachyon.zookeeper.election.path</td>
  <td>"/election"</td>
  <td>Election folder in ZooKeeper.</td>
</tr>
<tr>
  <td>tachyon.zookeeper.leader.path</td>
  <td>"/leader"</td>
  <td>Leader folder in ZooKeeper.</td>
</tr>
<tr>
  <td>tachyon.underfs.hdfs.impl</td>
  <td>"org.apache.hadoop.hdfs.DistributedFileSystem"</td>
  <td>The implementation class of the HDFS, if using it as the under FS.</td>
</tr>
<tr>
  <td>tachyon.max.columns</td>
  <td>1000</td>
  <td>Maximum number of columns allowed in RawTable, must be set on the client and server side</td>
</tr>
<tr>
  <td>tachyon.table.metadata.byte</td>
  <td>5242880</td>
  <td>Maximum amount of bytes allowed to be store as RawTable metadata, must be set on the server side</td>
</tr>
<tr>
  <td>fs.s3n.awsAccessKeyId</td>
  <td>null</td>
  <td>S3 aws access key id if using S3 as the under FS.</td>
</tr>
<tr>
  <td>fs.s3n.awsSecretAccessKey</td>
  <td>null</td>
  <td>S3 aws secret access key id if using S3 as the under FS.</td>
</tr>
<tr>
  <td>tachyon.underfs.glusterfs.mounts</td>
  <td>null</td>
  <td>Glusterfs volume mount points, e.g. /vol</td>
</tr>
<tr>
  <td>tachyon.underfs.glusterfs.volumes</td>
  <td>null</td>
  <td>Glusterfs volume names, e.g. tachyon_vol</td>
</tr>
<tr>
  <td>tachyon.underfs.glusterfs.mapred.system.dir</td>
  <td>glusterfs:///mapred/system</td>
  <td>Optionally specify subdirectory under GLusterfs for intermediary MapReduce data.</td>
</tr>
</table>

# Master Configuration

The master configuration specifies information regarding the master node, such as address and port
number.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.master.journal.folder</td>
  <td>$tachyon.home + "/journal/"</td>
  <td>The folder to store master journal log.</td>
</tr>
<tr>
  <td>tachyon.master.hostname</td>
  <td>localhost</td>
  <td>The externally visible hostname of Tachyon's master address.</td>
</tr>
<tr>
  <td>tachyon.master.port</td>
  <td>19998</td>
  <td>The port Tachyon's master node runs on.</td>
</tr>
<tr>
  <td>tachyon.master.web.port</td>
  <td>19999</td>
  <td>The port Tachyon's web interface runs on.</td>
</tr>
<tr>
  <td>tachyon.master.whitelist</td>
  <td>/</td>
  <td>The comma-separated list of prefixes of the paths which are cacheable, separated by semi-colons. Tachyon will try to cache the cacheable file when it is read for the first time.</td>
</tr>
</table>

# Worker Configuration

The worker configuration specifies information regarding the worker nodes, such as address and port
number.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.worker.port</td>
  <td>29998</td>
  <td>The port Tachyon's worker node runs on.</td>
</tr>
<tr>
  <td>tachyon.worker.data.port</td>
  <td>29999</td>
  <td>The port Tachyon's worker's data server runs on.</td>
</tr>
<tr>
  <td>tachyon.worker.data.folder</td>
  <td>/mnt/ramdisk</td>
  <td>The path to the data folder for Tachyon's worker nodes. Note for macs the value should be "/Volumes/"</td>
</tr>
<tr>
  <td>tachyon.worker.memory.size</td>
  <td>128 MB</td>
  <td>Memory capacity of each worker node.</td>
</tr>
</table>

# User Configuration

The user configuration specifies values regarding file system access.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.user.failed.space.request.limits</td>
  <td>3</td>
  <td>The number of times to request space from the file system before aborting</td>
</tr>
<tr>
  <td>tachyon.user.quota.unit.bytes</td>
  <td>8 MB</td>
  <td>The minimum number of bytes that will be requested from a client to a worker at a time</td>
</tr>
<tr>
  <td>tachyon.user.file.buffer.bytes</td>
  <td>1 MB</td>
  <td>The size of the file buffer to use for file system reads/writes.</td>
</tr>
<tr>
  <td>tachyon.user.default.block.size.byte</td>
  <td>1 GB</td>
  <td>Default block size for Tachyon files.</td>
</tr>
<tr>
  <td>tachyon.user.remote.read.buffer.size.byte</td>
  <td>1 MB</td>
  <td>The size of the file buffer to read data from remote Tachyon worker.</td>
</tr>
</table>
