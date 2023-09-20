---
layout: global
title: Journal Management
---


Alluxio keeps the history of all metadata related changes, such as creating files or renaming directories,
in edit logs referred to as "journal".
Upon startup, the Alluxio master will replay all the steps recorded in the journal to recover its last saved state.
Also when the leading master falls back to a different master for
[high availability (HA)]({{ '/en/deploy/Install-Alluxio-Cluster-with-HA.html' | relativize_url }}) mode,
the new leading master also replays the journal to recover the last state of the leading master.
The purpose of this documentation is to help Alluxio administrators understand and manage the Alluxio journal.

## Embedded Journal vs UFS Journal

There are two types of journals that Alluxio supports, `EMBEDDED` or `UFS`.
The embedded journal stores edit logs on each master's local file system and
coordinates multiple masters in HA mode to access the logs
based on a self-managed consensus protocol;
whereas UFS journal stores edit logs in an external shared UFS storage,
and relies on an external Zookeeper for coordination for HA mode.
Starting from 2.2, the default journal type is `EMBEDDED`.
This can be changed by setting the property `alluxio.master.journal.type` to `UFS`
instead of `EMBEDDED`.

To choose between the default Embedded Journal and UFS journal,
here are some aspects to consider:

- **External Dependency:**
Embedded journal does not rely on extra services.
UFS journal requires an external Zookeeper cluster in HA mode to coordinate who is the leading
master writing the journal, and requires a UFS for persistent storage.
If the UFS and Zookeeper clusters are not readily available and stable,
it is recommended to use the embedded journal over the UFS journal.
- **Fault tolerance:**
With `n` masters, using the embedded journal can tolerate only `floor(n/2)` master failures,
compared to `n-1` for UFS journal.
For example, With `3` masters, UFS journal can tolerate `2` failures,
while embedded journal can only tolerate `1`.
However, UFS journal depends on Zookeeper,
which similarly only supports `floor(#zookeeper_nodes / 2)` failures.
- **Journal Storage Type:**
When using a single Alluxio master, UFS journal can be local storage;
when using multiple Alluxio masters for HA mode,
this UFS storage must be shared among masters with reading and writing access.
To get reasonable performance, the UFS journal requires a UFS that supports fast streaming writes,
such as HDFS or NFS. In contrast, S3 is not recommended for the UFS journal.

## Configuring Embedded Journal

### Required configuration

The following configuration must be configured to a local path on the masters. The default
value is local directory `${alluxio.work.dir}/journal`.
```properties
alluxio.master.journal.folder=/local/path/to/store/journal/files/
```

Set the addresses of all masters in the cluster. The default embedded journal port is `19200`.
This must be set on all Alluxio servers, as well as Alluxio clients.

```properties
alluxio.master.embedded.journal.addresses=master_hostname_1:19200,master_hostname_2:19200,master_hostname_3:19200
```

### Optional configuration

* `alluxio.master.embedded.journal.port`: The port masters use for embedded journal communication. Default: `19200`.
* `alluxio.master.rpc.port`: The port masters use for RPCs. Default: `19998`.
* `alluxio.master.rpc.addresses`: A list of comma-separated `host:port` RPC addresses where the client should look for masters
when using multiple masters without Zookeeper. This property is not used when Zookeeper is enabled, since Zookeeper already stores the master addresses.
If this is not set, clients will look for masters using the hostnames from `alluxio.master.embedded.journal.addresses`
and the master rpc port (Default:`19998`).

### Configuring the Job service

It is usually best not to set any of these - by default the job master will use the same hostnames as the Alluxio master,
so it is enough to set only `alluxio.master.embedded.journal.addresses`. These properties only need to be set
when the job service runs independently of the rest of the system or using a non-standard port.

* `alluxio.job.master.embedded.journal.port`: the port job masters use for embedded journal communications. Default: `20003`.
* `alluxio.job.master.embedded.journal.addresses`: a comma-separated list of journal addresses for all job masters in the cluster.
The format is `hostname1:port1,hostname2:port2,...`.
* `alluxio.job.master.rpc.addresses`: A list of comma-separated host:port RPC addresses where the client should look for job masters
when using multiple job masters without Zookeeper. This property is not used when Zookeeper is enabled,
since Zookeeper already stores the job master addresses. If this property is not defined, clients will look for job masters using
`[alluxio.master.rpc.addresses]:alluxio.job.master.rpc.port` addresses first, then for
`[alluxio.job.master.embedded.journal.addresses]:alluxio.job.master.rpc.port`.

## Configuring UFS Journal

The most important configuration value to set for the journal is
`alluxio.master.journal.folder`. This must be set to a filesystem folder that is
available to all masters. In single-master mode, use a local filesystem path for simplicity. 
With multiple masters distributed across different machines, the folder must
be in a distributed system where all masters can access it. The journal folder
should be in a filesystem that supports flush such as HDFS or NFS. It is not
recommended to put the journal in an object store like S3. With an object store, every
metadata operation requires a new object to be created, which is
prohibitively slow for most serious use cases.

UFS journal options can be configured using the configuration prefix:

`alluxio.master.journal.ufs.option.<some alluxio property>`

**Configuration examples:**

Use HDFS to store the journal:
```properties
alluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/alluxio_journal
alluxio.master.journal.ufs.option.alluxio.underfs.version=2.6
```

Use the local file system to store the journal:
```properties
alluxio.master.journal.folder=/opt/alluxio/journal
```

## Formatting the journal

Formatting the journal deletes all of its content and restores it to a fresh state.
Before starting Alluxio for the first time, the journal must be formatted.

**Warning**: the following command permanently deletes all Alluxio metadata,
so be careful with this command.

```shell
$ ./bin/alluxio init format --skip-worker
```

Alternatively, each master node can format their local journal files with the following command:

```shell
$ ./bin/alluxio journal format
```

## Advanced

### Managing the journal size

When running with a single master, the journal folder size will grow indefinitely
as metadata operations are written to journal log files. To address this, production
deployments should run in HA mode with multiple Alluxio masters. The standby
masters will create checkpoints of the master state and clean up the logs that
were written before the checkpoints. For example, if 3 million Alluxio files were
created and then 2 million were deleted, the journal logs would contain 5 million
total entries. Then if a checkpoint is taken, the checkpoint will contain only the
metadata for the 1 million remaining files, and the original 5 million entries will be deleted.

By default, checkpoints are automatically taken every 2 million entries. This can be configured by
setting `alluxio.master.journal.checkpoint.period.entries` on the masters. Setting
the value lower will reduce the amount of disk space needed by the journal at the
cost of additional work for the standby masters.

When the metadata are stored in RocksDB, Alluxio 2.9 added support to checkpointing with multiple threads.
`alluxio.master.metastore.rocks.parallel.backup=true` will turn on multi-threaded checkpointing and
make the checkpointing a few times faster(depending how many threads are used). 
`alluxio.master.metastore.rocks.parallel.backup.threads` controls how many threads to use.
`alluxio.master.metastore.rocks.parallel.backup.compression.level` specifies the compression level, 
where smaller means bigger file and less CPU consumption, and larger means smaller file and more CPU consumption. 

#### Checkpointing on the leading master

Checkpointing requires a pause in master metadata changes and causes temporary service
unavailability while the leading master is writing a checkpoint.
This operation may take hours depending on Alluxio's namespace size.
Therefore, Alluxio's leading master will not create checkpoints by default.

Restarting the current leading master to transfer the leadership to another running master periodically
can help avoid leading master journal logs from growing unbounded when Alluxio is running in HA mode.

Starting from version 2.4, Alluxio embedded journal HA mode supports automatically transferring checkpoints from standby masters to the leading master.
The leading master can use those checkpoints as taken locally to truncate its journal size without causing temporary service unavailability.
No need to manually transfer leadership anymore.

If HA mode is not an option, the following command can be used to manually trigger the checkpoint:

```shell
$ ./bin/alluxio journal checkpoint
```

The `checkpoint` command should be used on an off-peak time to avoid interfering with other users of the system.

### Exiting upon Demotion

By default, Alluxio will transition masters from primaries to standbys.
During this process the JVM is _not_ shut down at any point.
This occasionally leaves behind resources and may lead to a bloated memory footprint.
To avoid taking up too much memory this, there is a flag which forces a master JVM to exit once it
has been demoted from a primary to a standby.
This moves the responsibility of restarting the process to join the quorum as a standby to a
process supervisor such as a Kubernetes cluster manager or systemd.

To configure this behavior for an Alluxio master, set the following configuration inside of 
`alluxio-site.properties`

```properties
alluxio.master.journal.exit.on.demotion=true
```
