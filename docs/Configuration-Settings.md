---
layout: global
title: Configuration Settings
group: Features
priority: 1
---

* Table of Contents
{:toc}

There are two types of configuration parameters for Tachyon:

1. [Configuration properties](#configuration-properties) are used to configure the runtime settings
of Tachyon system, and
2. [System environment properties](#system-environment-properties) control the Java VM options to
run Tachyon as well as some basic very setting.

# Configuration properties

On startup, Tachyon loads the default (and optionally a site specific) configuration properties file
to set the configuration properties.

1. The default values of configuration properties of Tachyon are defined in
`tachyon-default.properties`. This file can be found in Tachyon source tree and is typically
distributed with Tachyon binaries. We do not recommend beginner users to edit this file directly.

2. Each site deployment and application client can also override the default property values via
`tachyon-site.properties` file. Note that, this file **must be in the classpath** of the Java VM in
which Tachyon is running. The easiest way is to put the site properties file in directory
`$TACHYON_HOME/conf`.

All Tachyon configuration properties fall into one of the five categories:
[Common](#common-configuration) (shared by Master and Worker),
[Master specific](#master-configuration), [Worker specific](#worker-configuration), and
[User specific](#user-configuration), and [Cluster specific](#cluster-management) (used for running
Tachyon with cluster managers like Mesos and YARN).

## Common Configuration

The common configuration contains constants shared by different components.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.debug</td>
  <td>false</td>
  <td>Set to true to enable debug mode which has additional logging and info in the Web UI.</td>
</tr>
<tr>
  <td>tachyon.home</td>
  <td>/mnt/tachyon_default_home</td>
  <td>Tachyon installation folder.</td>
</tr>
<tr>
  <td>tachyon.logs.dir</td>
  <td>${tachyon.home}/logs</td>
  <td>The path to store log files.</td>
</tr>
<tr>
  <td>tachyon.keyvalue.enabled</td>
  <td>false</td>
  <td>Whether the key-value service is enabled.</td>
</tr>
<tr>
  <td>tachyon.keyvalue.partition.size.bytes.max</td>
  <td>512MB</td>
  <td>Maximum allowable size (in bytes) of a single key-value partition in a store. This value
  should be no larger than the block size (tachyon.user.block.size.bytes.default)</td>
</tr>
<tr>
  <td>tachyon.metrics.conf.file</td>
  <td>${tachyon.home}/conf/metrics.properties</td>
  <td>The file path of the metrics system configuration file. By default it is `metrics.properties`
  in the `conf` directory.</td>
</tr>
<tr>
  <td>tachyon.network.host.resolution.&#8203;timeout.ms</td>
  <td>5000</td>
  <td>During startup of the Master and Worker processes Tachyon needs to ensure that they are
    listening on externally resolvable and reachable host names.  To do this, Tachyon will
    automatically attempt to select an appropriate host name if one was not explicitly specified.
    This represents the maximum amount of time spent waiting to determine if a candidate host name
    is resolvable over the network.</td>
</tr>
<tr>
  <td>tachyon.test.mode</td>
  <td>false</td>
  <td>Flag used only during tests to allow special behavior.</td>
</tr>
<tr>
  <td>tachyon.underfs.address</td>
  <td>${tachyon.home}/underFSStorage</td>
  <td>Tachyon folder in the underlayer file system.</td>
</tr>
<tr>
  <td>tachyon.underfs.glusterfs.impl</td>
  <td>org.apache.hadoop.fs.glusterfs.&#8203;GlusterFileSystem</td>
  <td>Glusterfs hook with hadoop.</td>
</tr>
<tr>
  <td>tachyon.underfs.glusterfs.mapred.&#8203;system.dir</td>
  <td>glusterfs:///mapred/system</td>
  <td>Optionally, specify subdirectory under GlusterFS for intermediary MapReduce data.</td>
</tr>
<tr>
  <td>tachyon.underfs.hdfs.configuration</td>
  <td>${tachyon.home}/conf/core-site.xml</td>
  <td>Location of the hdfs configuration file.</td>
</tr>
<tr>
  <td>tachyon.underfs.hdfs.impl</td>
  <td>org.apache.hadoop.hdfs.&#8203;DistributedFileSystem</td>
  <td>The implementation class of the HDFS as the under storage system.</td>
</tr>
<tr>
  <td>tachyon.underfs.hdfs.prefixes</td>
  <td>hdfs://,glusterfs:///</td>
  <td>Optionally, specify which prefixes should run through the Apache Hadoop implementation of
    UnderFileSystem. The delimiter is any whitespace and/or ','.</td>
</tr>
<tr>
  <td>tachyon.underfs.s3.proxy.host</td>
  <td>No default</td>
  <td>Optionally, specify a proxy host for communicating with S3.</td>
</tr>
<tr>
  <td>tachyon.underfs.s3.proxy.https.only</td>
  <td>true</td>
  <td>If using a proxy to communicate with S3, determine whether to talk to the proxy using https.</td>
</tr>
<tr>
  <td>tachyon.underfs.s3.proxy.port</td>
  <td>No default</td>
  <td>Optionally, specify a proxy port for communicating with S3.</td>
</tr>
<tr>
  <td>tachyon.web.resources</td>
  <td>${tachyon.home}/core/server/src/main/webapp</td>
  <td>Path to the web application resources.</td>
</tr>
<tr>
  <td>tachyon.web.threads</td>
  <td>1</td>
  <td>How many threads to use for the web server.</td>
</tr>
<tr>
  <td>tachyon.zookeeper.election.path</td>
  <td>/election</td>
  <td>Election folder in ZooKeeper.</td>
</tr>
<tr>
  <td>tachyon.zookeeper.enabled</td>
  <td>false</td>
  <td>If true, setup master fault tolerant mode using ZooKeeper.</td>
</tr>
<tr>
  <td>tachyon.zookeeper.leader.path</td>
  <td>/leader</td>
  <td>Leader folder in ZooKeeper.</td>
</tr>
<tr>
  <td>tachyon.zookeeper.leader.inquiry.retry</td>
  <td>10</td>
  <td>The number of retries to inquire leader from ZooKeeper.</td>
</tr>
</table>

## Master Configuration

The master configuration specifies information regarding the master node, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.master.bind.host</td>
  <td>0.0.0.0</td>
  <td>The hostname that Tachyon master binds to. See <a href="#configure-multihomed-networks">multi-homed networks</a></td>
</tr>
<tr>
  <td>tachyon.master.heartbeat.interval.ms</td>
  <td>1000</td>
  <td>The interval (in milliseconds) between Tachyon master's heartbeats</td>
</tr>
<tr>
  <td>tachyon.master.hostname</td>
  <td>localhost</td>
  <td>The hostname of Tachyon master.</td>
</tr>
<tr>
  <td>tachyon.master.format.file_prefix</td>
  <td>"_format_"</td>
  <td>The file prefix of the file generated in the journal directory when the journal is
    formatted. The master will search for a file with this prefix when determining of the journal
    was once formatted.</td>
</tr>
<tr>
  <td>tachyon.master.journal.folder</td>
  <td>${tachyon.home}/journal/</td>
  <td>The path to store master journal logs.</td>
</tr>
<tr>
  <td>tachyon.master.journal.formatter.class</td>
  <td>tachyon.master.journal.&#8203;ProtoBufJournalFormatter</td>
  <td>The class to serialize the journal in a specified format.</td>
</tr>
<tr>
  <td>tachyon.master.journal.log.size.bytes.max</td>
  <td>10MB</td>
  <td>If a log file is bigger than this value, it will rotate to next file</td>
</tr>
<tr>
  <td>tachyon.master.journal.tailer.&#8203;shutdown.quiet.wait.time.ms</td>
  <td>5000</td>
  <td>Before the standby master shuts down its tailer thread, there should be no update to the
    leader master's journal in this specified time period (in milliseconds).</td>
</tr>
<tr>
  <td>tachyon.master.journal.tailer.sleep.time.ms</td>
  <td>1000</td>
  <td>Time (in milliseconds) the standby master sleeps for when it cannot find anything new in leader
    master's journal.</td>
</tr>
<tr>
  <td>tachyon.master.lineage.checkpoint.interval.ms</td>
  <td>600000</td>
  <td>
  The interval (in milliseconds) between Tachyon's checkpoint scheduling.
  </td>
</tr>
<tr>
  <td>tachyon.master.lineage.checkpoint.class</td>
  <td>tachyon.master.lineage.checkpoint.&#8203;CheckpointLatestScheduler</td>
  <td>
  The class name of the checkpoint strategy for lineage output files. The default strategy is to
  checkpoint the latest completed lineage, i.e. the lineage whose output files are completed.
  </td>
</tr>
<tr>
  <td>tachyon.master.lineage.recompute.interval.ms</td>
  <td>600000</td>
  <td>
  The interval (in milliseconds) between Tachyon's recompute execution. The executor scans the
  all the lost files tracked by lineage, and re-executes the corresponding jobs.
  every 10 minutes.
  </td>
</tr>
<tr>
  <td>tachyon.master.lineage.recompute.log.path</td>
  <td>${tachyon.home}/logs/recompute.log</td>
  <td>
  The path to the log that the recompute executor redirects the job's stdout into.
  </td>
</tr>
<tr>
  <td>tachyon.master.port</td>
  <td>19998</td>
  <td>The port that Tachyon master node runs on.</td>
</tr>
<tr>
  <td>tachyon.master.retry</td>
  <td>29</td>
  <td>The number of retries that the client connects to master</td>
</tr>
<tr>
  <td>tachyon.master.ttlchecker.interval.ms</td>
  <td>3600000</td>
  <td>Time interval (in milliseconds) to periodically delete the files with expired ttl value.</td>
</tr>
<tr>
  <td>tachyon.master.web.bind.host</td>
  <td>0.0.0.0</td>
  <td>The hostname Tachyon master web UI binds to. See <a href="#configure-multihomed-networks">multi-homed networks</a></td>
</tr>
<tr>
  <td>tachyon.master.web.hostname</td>
  <td>localhost</td>
  <td>The hostname of Tachyon Master web UI.</td>
</tr>
<tr>
  <td>tachyon.master.web.port</td>
  <td>19999</td>
  <td>The port Tachyon web UI runs on.</td>
</tr>
<tr>
  <td>tachyon.master.whitelist</td>
  <td>/</td>
  <td>A comma-separated list of prefixes of the paths which are cacheable, separated by
    semi-colons. Tachyon will try to cache the cacheable file when it is read for the first
    time.</td>
</tr>
<tr>
  <td>tachyon.master.worker.threads.max</td>
  <td>2048</td>
  <td>The max number of workers that connect to master</td>
</tr>
<tr>
  <td>tachyon.master.worker.timeout.ms</td>
  <td>10000</td>
  <td>Timeout (in milliseconds) between master and worker indicating a lost worker.</td>
</tr>
<tr>
  <td>tachyon.master.tieredstore.global.levels</td>
  <td>3</td>
  <td>The total number of storage tiers in the system</td>
</tr>
<tr>
  <td>tachyon.master.tieredstore.global.level0.alias</td>
  <td>MEM</td>
  <td>The name of the highest storage tier in the entire system</td>
</tr>
<tr>
  <td>tachyon.master.tieredstore.global.level1.alias</td>
  <td>SSD</td>
  <td>The name of the second highest storage tier in the entire system</td>
</tr>
<tr>
  <td>tachyon.master.tieredstore.global.level2.alias</td>
  <td>HDD</td>
  <td>The name of the third highest storage tier in the entire system</td>
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

The worker configuration specifies information regarding the worker nodes, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.worker.allocator.class</td>
  <td>tachyon.worker.block.allocator.&#8203;MaxFreeAllocator</td>
  <td>The strategy that a worker uses to allocate space among storage directories in certain storage
  layer. Valid options include: `tachyon.worker.block.allocator.MaxFreeAllocator`,
  `tachyon.worker.block.allocator.GreedyAllocator`,
  `tachyon.worker.block.allocator.RoundRobinAllocator`.</td>
</tr>
<tr>
  <td>tachyon.worker.bind.host</td>
  <td>0.0.0.0</td>
  <td>The hostname Tachyon's worker node binds to. See <a href="#configure-multihomed-networks">multi-homed networks</a></td>
</tr>
<tr>
  <td>tachyon.worker.block.heartbeat.interval.ms</td>
  <td>1000</td>
  <td>The interval (in milliseconds) between block worker's heartbeats</td>
</tr>
<tr>
  <td>tachyon.worker.block.heartbeat.timeout.ms</td>
  <td>10000</td>
  <td>The timeout value (in milliseconds) of block worker's heartbeat</td>
</tr>
<tr>
  <td>tachyon.worker.block.threads.max</td>
  <td>2048</td>
  <td>The maximum number of incoming RPC requests to block worker that can be handled. This value is used for configuring the Thrift server for RPC with block worker.</td>
</tr>
<tr>
  <td>tachyon.worker.block.threads.min</td>
  <td>1</td>
  <td>The minimum number of incoming RPC requests to block worker that can be handled. This value is used for configuring the Thrift server for RPC with block worker.</td>
</tr>
<tr>
  <td>tachyon.worker.data.bind.host</td>
  <td>0.0.0.0</td>
  <td>The hostname that the Tachyon worker's data server runs on. See
    <a href="#configure-multihomed-networks">multi-homed networks</a></td>
</tr>
<tr>
  <td>tachyon.worker.data.folder</td>
  <td>/tachyonworker/</td>
  <td>A relative path within each storage directory used as the data folder for Tachyon worker to put data for tiered store.</td>
</tr>
<tr>
  <td>tachyon.worker.data.port</td>
  <td>29999</td>
  <td>The port Tachyon's worker's data server runs on.</td>
</tr>
<tr> <td>tachyon.worker.data.server.class</td>
  <td>tachyon.worker.netty.&#8203;NettyDataServer</td>
  <td>Selects the networking stack to run the worker with. Valid options are:
  `tachyon.worker.netty.NettyDataServer`, `tachyon.worker.nio.NIODataServer`.</td>
</tr>
<tr>
  <td>tachyon.worker.evictor.class</td>
  <td>tachyon.worker.block.&#8203;evictor.LRUEvictor</td>
  <td>The strategy that a worker uses to evict block files when a storage layer runs out of space. Valid
  options include `tachyon.worker.block.evictor.LRFUEvictor`,
  `tachyon.worker.block.evictor.GreedyEvictor`, `tachyon.worker.block.evictor.LRUEvictor`.</td>
</tr>
<tr>
  <td>tachyon.worker.evictor.lrfu.attenuation.factor</td>
  <td>2.0</td>
  <td>A attenuation factor in [2, INF) to control the behavior of LRFU.</td>
</tr>
<tr>
  <td>tachyon.worker.evictor.lrfu.step.factor</td>
  <td>0.25</td>
  <td>A factor in [0, 1] to control the behavior of LRFU: smaller value makes LRFU more similar to
  LFU; and larger value makes LRFU closer to LRU.</td>
</tr>
<tr>
  <td>tachyon.worker.filesystem.heartbeat.interval.ms</td>
  <td>1000</td>
  <td>
  The heartbeat interval (in milliseconds) between the worker and file system master.
  </td>
</tr>
<tr>
  <td>tachyon.worker.hostname</td>
  <td>localhost</td>
  <td>The hostname of Tachyon worker.</td>
</tr>
<tr>
  <td>tachyon.worker.memory.size</td>
  <td>128 MB</td>
  <td>Memory capacity of each worker node.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.boss.threads</td>
  <td>1</td>
  <td>How many threads to use for accepting new requests.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.file.transfer</td>
  <td>MAPPED</td>
  <td>When returning files to the user, select how the data is transferred; valid options are
    `MAPPED` (uses java MappedByteBuffer) and `TRANSFER` (uses Java FileChannel.transferTo).</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.shutdown.quiet.period</td>
  <td>2</td>
  <td>The quiet period (in seconds). When the netty server is shutting down, it will ensure that no
    RPCs occur during the quiet period. If an RPC occurs, then the quiet period will restart before
    shutting down the netty server.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.shutdown.timeout</td>
  <td>15</td>
  <td>Maximum amount of time to wait (in seconds) until the netty server is shutdown (regardless of
    the quiet period).</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.watermark.high</td>
  <td>32768</td>
  <td>Determines how many bytes can be in the write queue before switching to non-writable.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.watermark.low</td>
  <td>8192</td>
  <td>Once the high watermark limit is reached, the queue must be flushed down to the low watermark
    before switching back to writable.</td>
</tr>
<tr>
  <td>tachyon.worker.network.netty.worker.threads</td>
  <td>0</td>
  <td>How many threads to use for processing requests. Zero defaults to #cpuCores * 2.</td>
</tr>
<tr>
  <td>tachyon.worker.port</td>
  <td>29998</td>
  <td>The port Tachyon's worker node runs on.</td>
</tr>
<tr>
  <td>tachyon.worker.session.timeout.ms</td>
  <td>10000</td>
  <td>Timeout (in milliseconds) between worker and client connection indicating a lost session
  connection.</td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.block.locks</td>
  <td>1000</td>
  <td>Total number of block locks for a Tachyon block worker. Larger value leads to finer locking
  granularity, but uses more space.</td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.levels</td>
  <td>1</td>
  <td>The number of storage tiers on the worker</td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.level0.alias</td>
  <td>MEM</td>
  <td>The alias of the highest storage tier on this worker. It must match one of
  the global storage tiers from the master configuration. We disable placing an
  alias lower in the global hierarchy before an alias with a higher postion on
  the worker hierarchy. So by default, SSD cannot come before MEM on any worker.
  </td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.level0.dirs.path</td>
  <td>/mnt/ramdisk/</td>
  <td>The path of storage directory path for the top storage layer. Note for MacOS the value
  should be `/Volumes/`</td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.level0.dirs.quota</td>
  <td>${tachyon.worker.memory.size}</td>
  <td>The capacity of the top storage layer.</td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.level0.reserved.ratio</td>
  <td>0.1</td>
  <td>The portion of space reserved in the top storage layer (a value between 0 and 1).</td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.reserver.enabled</td>
  <td>false</td>
  <td>Whether to enable tiered store reserver service or not.</td>
</tr>
<tr>
  <td>tachyon.worker.tieredstore.reserver.interval.ms</td>
  <td>1000</td>
  <td>The time period (in milliseconds) of space reserver service, which keeps certain portion of
  available space on each layer.</td>
</tr>
<tr>
  <td>tachyon.worker.web.bind.host</td>
  <td>0.0.0.0</td>
  <td>The hostname Tachyon worker's web server binds to. See <a href="#configure-multihomed-networks">multi-homed networks</a></td>
</tr>
<tr>
  <td>tachyon.worker.web.hostname</td>
  <td>localhost</td>
  <td>The hostname Tachyon worker's web UI binds to.</td>
</tr>
<tr>
  <td>tachyon.worker.web.port</td>
  <td>30000</td>
  <td>The port Tachyon worker's web UI runs on.</td>
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

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.user.block.master.client.threads</td>
  <td>10</td>
  <td>The number of threads used by a block master client to talk to the block master.</td>
</tr>
<tr>
  <td>tachyon.user.block.worker.client.threads</td>
  <td>10000</td>
  <td>How many threads to use for block worker client pool to read from a local block worker.</td>
</tr>
<tr>
  <td>tachyon.user.block.remote.read.buffer.size.bytes</td>
  <td>8 MB</td>
  <td>The size of the file buffer to read data from remote Tachyon worker.</td>
</tr>
<tr>
  <td>tachyon.user.block.remote.reader.class</td>
  <td>tachyon.client.netty.&#8203;NettyRemoteBlockReader</td>
  <td>Selects networking stack to run the client with. Currently only
    `tachyon.client.netty.NettyRemoteBlockReader` (read remote data using netty) is valid.</td>
</tr>
<tr>
  <td>tachyon.user.block.remote.writer.class</td>
  <td>tachyon.client.netty.&#8203;NettyRemoteBlockWriter</td>
  <td>Selects networking stack to run the client with for block writes.</td>
</tr>
<tr>
  <td>tachyon.user.block.size.bytes.default</td>
  <td>512MB</td>
  <td>Default block size for Tachyon files.</td>
</tr>
<tr>
  <td>tachyon.user.failed.space.request.limits</td>
  <td>3</td>
  <td>The number of times to request space from the file system before aborting.</td>
</tr>
<tr>
  <td>tachyon.user.file.buffer.bytes</td>
  <td>1 MB</td>
  <td>The size of the file buffer to use for file system reads/writes.</td>
</tr>
<tr>
  <td>tachyon.user.file.master.client.threads</td>
  <td>10</td>
  <td>The number of threads used by a file master client to talk to the file master.</td>
</tr>
<tr>
  <td>tachyon.user.file.waitcompleted.poll.ms</td>
  <td>1000</td>
  <td>The time interval to poll a file for its completion status when using waitCompleted.</td>
</tr>
<tr>
  <td>tachyon.user.file.write.location.policy.class</td>
  <td>tachyon.client.file.policy.LocalFirstPolicy</td>
  <td>The default location policy for choosing workers for writing a file's blocks</td>
</tr>
<tr>
  <td>tachyon.user.file.readtype.default</td>
  <td>CACHE_PROMOTE</td>
  <td>Default write type when creating Tachyon files.
    Valid options are `CACHE_PROMOTE` (move data to highest tier if already in Tachyon storage,
    write data into highest tier of local Tachyon if data needs to be read from under storage),
    `CACHE` (write data into highest tier of local Tachyon if data needs to be read from under
    storage), `NO_CACHE` (no data interaction with Tachyon, if the read is from Tachyon data
    migration or eviction will not occur).</td>
</tr>
<tr>
  <td>tachyon.user.file.writetype.default</td>
  <td>MUST_CACHE</td>
  <td>Default write type when creating Tachyon files.
    Valid options are `MUST_CACHE` (write will only go to Tachyon and must be stored in Tachyon),
    `CACHE_THROUGH` (try to cache, write to UnderFS synchronously), `THROUGH` (no cache, write to
    UnderFS synchronously).</td>
</tr>
<tr>
  <td>tachyon.user.heartbeat.interval.ms</td>
  <td>1000</td>
  <td>The interval (in milliseconds) between Tachyon worker's heartbeats</td>
</tr>
<tr>
  <td>tachyon.user.lineage.enabled</td>
  <td>false</td>
  <td>Flag to enable lineage feature.</td>
</tr>
<tr>
  <td>tachyon.user.lineage.master.client.threads</td>
  <td>10</td>
  <td>The number of threads used by a lineage master client to talk to the lineage master.</td>
</tr>
<tr>
  <td>tachyon.user.network.netty.timeout.ms</td>
  <td>3000</td>
  <td>The maximum number of milliseconds for a netty client (for block reads and block writes) to
  wait for a response from the data server.</td>
</tr>
<tr>
  <td>tachyon.user.network.netty.worker.threads</td>
  <td>0</td>
  <td>How many threads to use for remote block worker client to read from remote block workers.</td>
</tr>
<tr>
  <td>tachyon.user.quota.unit.bytes</td>
  <td>8 MB</td>
  <td>The minimum number of bytes that will be requested from a client to a worker at a time.</td>
</tr>
</table>

## Cluster Management

When running Tachyon with cluster managers like Mesos and YARN, Tachyon has additional
configuration options.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td>tachyon.integration.master.resource.cpu</td>
  <td>1</td>
  <td>CPU resource in terms of number of cores required to run a Tachyon master.</td>
</tr>
<tr>
  <td>tachyon.integration.master.resource.mem</td>
  <td>1024 MB</td>
  <td>Memory resource required to run a Tachyon master.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.executor.dependency.path</td>
  <td>https://s3.amazonaws.com/tachyon-mesos</td>
  <td>The URL from which Mesos executor can download Tachyon dependencies.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.jre.path</td>
  <td>jre1.7.0_79</td>
  <td>The relative path to the JRE directory included in the tarball available at the JRE URL.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.jre.url</td>
  <td>https://s3.amazonaws.com/tachyon-mesos/jre-7u79-linux-x64.tar.gz</td>
  <td>The URL from which Mesos executor can download the JRE to use.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.master.name</td>
  <td>TachyonMaster</td>
  <td>The Mesos task name for the Tachyon master task.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.master.node.count</td>
  <td>1</td>
  <td>The number of Tachyon master processes to start.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.principal</td>
  <td>tachyon</td>
  <td>Tachyon framework’s identity.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.role</td>
  <td>*</td>
  <td>Role that Tachyon framework in Mesos cluster may belong to.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.secret</td>
  <td></td>
  <td>Tachyon framework’s secret.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.user</td>
  <td>root</td>
  <td>Account used by the Mesos executor to run Tachyon workers.</td>
</tr>
<tr>
  <td>tachyon.integration.mesos.worker.name</td>
  <td>TachyonWorker</td>
  <td>The Mesos task name for the Tachyon worker task.</td>
</tr>
<tr>
  <td>tachyon.integration.yarn.max.workers.per.host</td>
  <td>1</td>
  <td>The maximum number of Tachyon workers which may be allocated to a single host.</td>
</tr>
<tr>
  <td>tachyon.integration.worker.resource.cpu</td>
  <td>1</td>
  <td>CPU resource in terms of number of cores required to run a Tachyon worker.</td>
</tr>
<tr>
  <td>tachyon.integration.worker.resource.mem</td>
  <td>1024 MB</td>
  <td>Memory resource required to run a Tachyon worker. This memory does not include the memory
  configured for tiered storage.</td>
</tr>
</table>

## Configure multihomed networks

Tachyon configuration provides a way to take advantage of multi-homed networks. If you have more
than one NICs and you want your Tachyon master to listen on all NICs, you can specify
`tachyon.master.bind.host` to be `0.0.0.0`. As a result, Tachyon clients can reach the master node
from connecting to any of its NIC. This is also the same case for other properties suffixed with
`bind.host`.

# System environment properties

To run Tachyon, it also requires some system environment variables being set which by default are
configured in file `conf/tachyon-env.sh`. If this file does not exist yet, you can create one from a
template we provided in the source code using:

```bash
$ cp conf/tachyon-env.sh.template conf/tachyon-env.sh
```

There are a few frequently used Tachyon configuration properties that can be set via environment
variables. One can either set these variables through shell or modify their default values specified
in `conf/tachyon-env.sh`.

* `$TACHYON_MASTER_ADDRESS`: Tachyon master address, default to localhost.
* `$TACHYON_UNDERFS_ADDRESS`: under storage system address, default to
`${TACHYON_HOME}/underFSStorage` which is a local file system.
* `$TACHYON_JAVA_OPTS`: Java VM options for both Master and Worker.
* `$TACHYON_MASTER_JAVA_OPTS`: additional Java VM options for Master configuration.
* `$TACHYON_WORKER_JAVA_OPTS`: additional Java VM options for Worker configuration. Note that, by
default `TACHYON_JAVA_OPTS` is included in both `TACHYON_MASTER_JAVA_OPTS` and
`TACHYON_WORKER_JAVA_OPTS`.

For example, if you would like to connect Tachyon to HDFS running at localhost and enable Java
remote debugging at port 7001, you can do so using:

```bash
$ export TACHYON_UNDERFS_ADDRESS="hdfs://localhost:9000"
$ export TACHYON_MASTER_JAVA_OPTS="$TACHYON_JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=7001“
```
