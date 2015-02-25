---
layout: global
title: Configuration Settings
---

There are two types of configuration parameters for Tachyon:

1. Configuration properties, which is used to configure the runtime setting of Tachyon
2. System environment properties, which controls the Tachyon Java VM options

# Configuration properties

Tachyon introduces default and site specific configuration properties files to set the configuration properties.

Each site deployment and application client can override the default via tachyon.site.properties file.
This file has to be located in the classpath of the Java VM where Tachyon is running.

The easiest way is to put the site properties file in a directory specified by `$TACHYON_CONF_DIR`, which is by default is
set to `$TACHYON_HOME/conf`.

The Tachyon configuration properties fall into four categories: Master, Worker, Common (Master and
Worker), and User configurations.

## Common Configuration

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
  <td>$tachyon.underfs.address + "/tmp/tachyon/data"</td>
  <td>Tachyon's data folder in the underlayer file system.</td>
</tr>
<tr>
  <td>tachyon.workers.folder</td>
  <td>$tachyon.underfs.address + "/tmp/tachyon/workers"</td>
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
<tr>
  <td>tachyon.underfs.hadoop.prefixes</td>
  <td>hdfs:// s3:// s3n:// glusterfs:///</td>
  <td>Optionally specify which prefixes should run through the Apache Hadoop's implementation of UnderFileSystem.  The delimiter is any whitespace and/or ','</td>
</tr>
<tr>
  <td>tachyon.master.retry</td>
  <td>29</td>
  <td>How many times to try to reconnect with master.</td>
</tr>
</table>

## Master Configuration

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
<tr>
  <td>tachyon.master.web.threads</td>
  <td>1</td>
  <td>How many threads to use for the web server.</td>
</tr>
<tr>
  <td>tachyon.master.keytab.file</td>
  <td></td>
  <td>Kerberos keytab file for Tachyon master.</td>
</tr>
<tr>
  <td>tachyon.master.principal</td>
  <td></td>
  <td>Kerberos principal for Tachyon master.</td>
</tr>
</table>

## Worker Configuration

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
  <td>/tachyonworker/</td>
  <td>The relative path in each storage directory as the data folder for Tachyon's worker nodes.</td>
</tr>
<tr>
  <td>tachyon.worker.memory.size</td>
  <td>128 MB</td>
  <td>Memory capacity of each worker node.</td>
</tr>
<tr>
  <td>tachyon.worker.hierarchystore.level.max</td>
  <td>1</td>
  <td>The max level of storage layers.</td>
</tr>
<tr>
  <td>tachyon.worker.hierarchystore.level0.alias</td>
  <td>MEM</td>
  <td>The alias of top storage layer.</td>
</tr>
<tr>
  <td>tachyon.worker.hierarchystore.level0.dirs.path</td>
  <td>/mnt/ramdisk/</td>
  <td>The path of storage directory path for top storage layer. Note for macs the value should be "/Volumes/"</td>
</tr>
<tr>
  <td>tachyon.worker.hierarchystore.level0.dirs.quota</td>
  <td>${tachyon.worker.memory.size}</td>
  <td>The capacity of top storage layer.</td>
</tr>
<tr>
  <td>tachyon.worker.allocate.strategy</td>
  <td>MAX_FREE</td>
  <td>The strategy that worker allocate space among storage directories in certain storage layer.</td>
</tr>
<tr>
  <td>tachyon.worker.evict.strategy</td>
  <td>LRU</td>
  <td>The strategy that worker evict block files when a storage layer runs out of space.</td>
</tr>
<tr>
  <td>tachyon.worker.network.type</td>
  <td>NETTY</td>
  <td>Selects networking stack to run the worker with.  Valid options are NETTY and NIO.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.channel</td>
  <td>EPOLL</td>
  <td>Selects netty's channel implementation.  On linux, epoll is used; valid options are NIO and EPOLL.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.boss.threads</td>
  <td>1</td>
  <td>How many threads to use for accepting new requests.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.worker.threads</td>
  <td>0</td>
  <td>How many threads to use for processing requests. Zero defaults to #cpuCores * 2</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.file.transfer</td>
  <td>MAPPED</td>
  <td>When returning files to the user, select how the data is transferred; valid options are MAPPED (uses java MappedByteBuffer) and TRANSFER (uses Java FileChannel.transferTo).</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.watermark.high</td>
  <td>32768</td>
  <td>Determines how many bytes can be in the write queue before channels isWritable is set to false.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.watermark.low</td>
  <td>8192</td>
  <td>Once the high watermark limit is reached, the queue must be flushed down to the low watermark before switching back to writable.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.backlog</td>
  <td>128 on linux</td>
  <td>How many requests can be queued up before new requests are rejected; this value is platform dependent.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.buffer.send</td>
  <td>platform specific</td>
  <td>Sets SO_SNDBUF for the socket; more details can be found in the socket man page.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.buffer.receive</td>
  <td>platform specific</td>
  <td>Sets SO_RCVBUF for the socket; more details can be found in the socket man page.</td>
</tr>
<tr>
  <td>tachyon.worker.keytab.file</td>
  <td></td>
  <td>Kerberos keytab file for Tachyon worker.</td>
</tr>
<tr>
  <td>tachyon.worker.principal</td>
  <td></td>
  <td>Kerberos principal for Tachyon worker.</td>
</tr>
</table>

## User Configuration

The user configuration specifies values regarding file system access.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.user.failed.space.request.limits</td>
  <td>3</td>
  <td>The number of times to request space from the file system before aborting</td>
</tr>
<tr>
  <td>tachyon.user.file.writetype.default</td>
  <td>CACHE_THROUGH</td>
  <td>Default write type for Tachyon files in CLI copyFromLocal and Hadoop compatitable interface. It can be any type in WriteType.</td>
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
<tr>
  <td>tachyon.worker.network.netty.process.threads</td>
  <td>16</td>
  <td>How many threads to use to process block requests.</td>
</tr>
</table>

## Working with Apache Hadoop MapReduce Configuration

In certain deployments there could be situation where client needs to send configuration property
override to Hadoop MapReduce (MR) for Tachyon Hadoop compatible file system (TFS) when the
tachyon-site.properties file is not available in nodes where the MR is running.

To support this, the MR application client needs to do these steps:

1. Get the TachyonConf instance from call to TachyonConf.get().
2. Store the encoded TachyonConf object into Hadoop MR job’s Configuration using `ConfUtils.storeToHadoopConfiguration` call.
3. During initialization of the TFS, it will check if the key exists from the job’s Configuration
and if it does it will merge the override properties to the current TachyonConf instance via `ConfUtil.loadFromHadoopConfiguration`.

# System environment properties

The system environment variables is configured using the configuration file, which responsible for
setting system properties, is located under `conf/tachyon-env.sh`.

The location of the `tachyon-env.sh` can be set by environment variable `TACHYON_CONF_DIR`.

These variables should be set as variables under the `TACHYON_JAVA_OPTS` definition.

A template is provided with the zip: `conf/tachyon-env.sh.template`.

Additional Java VM options can be added to `TACHYON_MASTER_JAVA_OPTS` for Master and
`TACHYON_WORKER_JAVA_OPTS` for Worker configuration. In the template file, `TACHYON_JAVA_OPTS` is
included in both `TACHYON_MASTER_JAVA_OPTS` and `TACHYON_WORKER_JAVA_OPTS`.

For example if you would like to enable Java remote debugging at port 7001 in the Master you can modify
`TACHYON_MASTER_JAVA_OPTS` like this:

`export TACHYON_MASTER_JAVA_OPTS="$TACHYON_JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=7001"`
