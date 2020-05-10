---
layout: global
title: Journal Management
nickname: Journal Management
group: Operations
priority: 5
---

* Table of Contents
{:toc}

Alluxio keeps the history of all metadata related changes, such as creating files or renaming directories,
in edit logs referred to as "journal".
Upon startup, the Alluxio master will replay all the steps recorded in the journal to recover its last saved state.
Also when the leading master falls back to a different master for
[high availability (HA)]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url }}) mode,
the new leading master also replays the journal to recover the last state of the leading master.
The purpose of this documentation is to help Alluxio administrators understand and manage the Alluxio journal.

## Embedded Journal vs UFS Journal

There are two types of journals that Alluxio supports, `EMBEDDED` or `UFS`.
The embedded journal stores edit logs on each master's local file system and
coordinates multiple masters in HA mode to access the logs
based on a self-managed consensus protocal;
whereas UFS journal stores edit logs in an external shared UFS storage,
and relies on external Zookeeper for coordination for HA mode.
Starting from 2.2, the default journal type is `EMBEDDED`.
This can be changed by setting the property "`alluxio.master.journal.type`" to "`UFS`"
instead of "`EMBEDDED`".

To choose between the default Embedded Journal and UFS journal,
here are some aspects to consider:

- **External Dependency:**
Embedded journal does not rely on extra services.
UFS journal requires an external Zookeeper cluster in HA mode to coordinate who is the leading
master writing the journal, and requires on a UFS for persistent storage.
If UFS and Zookeeper cluster are not readily available and stable,
it is recommended to use the embedded journal over UFS journal.
- **Fault tolerance:**
With `n` masters, using the embedded journal can tolerate only `floor(n/2)` master failures,
compared to `n-1` for UFS journal.
For example, With `3` masters, UFS journal can tolerate `2` failures,
while embedded journal can only tolerate `1`.
However, UFS journal depends on Zookeeper,
which similarly only supports `floor(#zookeeper_nodes / 2)` failures.
- **Journal Storage Type:**
When using a single Alluxio master, UFS journal can be a local storage;
when using multiple Alluxio masters for HA mode,
this UFS storage must be shared among masters for read and write access.
To get reasonable performance, the UFS journal requires a UFS that supports fast streaming writes,
such as HDFS or NFS. In contrast, S3 is not recommended for UFS journal.

## Configuring UFS Journal

The most important configuration value to set for the journal is
`alluxio.master.journal.folder`. This must be set to a filesystem folder that is
available to all masters. In single-master mode, a local filesystem path may be
used for simplicity. With
multiple masters distributed across different machines, the folder must
be in a distributed system where all masters can access it. The journal folder
should be in a filesystem that supports flush such as HDFS or NFS. It is not
recommended to put the journal in an object store like S3. With an object store, every
metadata operation requires a new object to be created, which is
prohibitively slow for most serious use cases.

UFS journal options can be configured using the configuration prefix:

`alluxio.master.journal.ufs.option.<some alluxio property>`

**Configuration examples:**

Use HDFS to store the journal:
```
alluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/alluxio_journal
alluxio.master.journal.ufs.option.alluxio.underfs.version=2.6
```

Use the local file system to store the journal:
```
alluxio.master.journal.folder=/opt/alluxio/journal
```

## Configuring Embedded Journal

### Required configuration

The configuration specified below should be applied to both Alluxio servers and Alluxio clients.

Set the addresses of all masters in the cluster. The default embedded journal port is `19200`.

```
alluxio.master.embedded.journal.addresses=master_hostname_1:19200,master_hostname_2:19200,master_hostname_3:19200
```

### Optional configuration

* `alluxio.master.embedded.journal.port`: The port masters use for embedded journal communication. Default: `19200`.
* `alluxio.master.rpc.port`: The port masters use for RPCs. Default: `19998`.
* `alluxio.master.rpc.addresses`: A list of comma-separated `host:port` RPC addresses where the client should look for masters
when using multiple masters without Zookeeper. This property is not used when Zookeeper is enabled, since Zookeeper already stores the master addresses.
If this is not set, clients will look for masters using the hostnames from `alluxio.master.embedded.journal.addresses`
and the master rpc port (Default:`19998`).

### Advanced configuration
* `alluxio.master.embedded.journal.transport.request.timeout.ms`: The duration after which embedded journal masters will timeout messages sent between each other.
 Lower values might cause leadership instability when network is slow. Default: `5s`.
* `alluxio.master.embedded.journal.appender.batch.size`: Size of a single internal knowledge batch sent from leader to secondary masters.
 Lower values could potentially slow down secondary masters' catching up with the leader. Higher values might be reasonable in the network environment so that packets
 don't get timed out. Default: `512KB`.
* `alluxio.master.embedded.journal.transport.max.inbound.message.size`: Maximum allowed size for a network message between embedded journal masters.
The configured value should allow for appending batches to all secondary masters. Default: `100MB`.

### Configuring Job service

It is usually best not to set any of these - by default the job master will use the same hostnames as the Alluxio master,
so it is enough to set only `alluxio.master.embedded.journal.addresses`. These properties only need to be set
when the job service is being run independent from the rest of the system or using a non-standard port.

* `alluxio.job.master.embedded.journal.port`: the port job masters use for embedded journal communications. Default: `20003`.
* `alluxio.job.master.embedded.journal.addresses`: a comma-separated list of journal addresses for all job masters in the cluster.
The format is `hostname1:port1,hostname2:port2,...`.
* `alluxio.job.master.rpc.addresses`: A list of comma-separated host:port RPC addresses where the client should look for job masters
when using multiple job masters without Zookeeper. This property is not used when Zookeeper is enabled,
since Zookeeper already stores the job master addresses. If this is not set, clients will look for job masters using the hostnames
from `alluxio.master.embedded.journal.addresses` and the job master rpc port.

## Formatting the journal

Formatting the journal deletes all of its content and restores it to a fresh state.
Before starting Alluxio for the first time, the journal must be formatted.

**Warning**: the following command permanently deletes all Alluxio metadata,
so be careful with this command or backup your journal first (see next section).

```console
$ ./bin/alluxio formatMasters
```

## Backing up the journal

### Manually backing up the journal

Alluxio supports taking journal backups so that Alluxio metadata can be restored
to a previous point in time. Generating a backup causes temporary service
unavailability while the backup is written.
(See [Backup delegation on HA cluster](#backup-delegation-on-ha-cluster) section for overcoming this limitation.)

To generate a backup, use the `fsadmin backup` CLI command.
```console
$ ./bin/alluxio fsadmin backup
```

By default, this will write a backup named
`alluxio-backup-YYYY-MM-DD-timestamp.gz` to the `/alluxio_backups` directory of
the root under storage system, e.g. `hdfs://cluster/alluxio_backups`. This default
backup directory can be configured by setting `alluxio.master.backup.directory`

```
alluxio.master.backup.directory=/alluxio/backups
```

See the [backup command documentation]({{ '/en/operation/Admin-CLI.html' | relativize_url }}#backup)
for additional backup options.

### Automatically backing up the journal

Alluxio supports automatically taking primary master metadata snapshots every day at a fixed time
so that Alluxio metadata can be restored to at most one day before.
This functionality is enabled by setting the following property in `${ALLUXIO_HOME}/conf/alluxio-site.properties`:

```
alluxio.master.daily.backup.enabled=true
```

The time to take daily snapshots is defined by `alluxio.master.daily.backup.time`. For example, if
a user specified `alluxio.master.daily.backup.time=05:30`, the Alluxio primary master will back up its metadata
to the `alluxio.master.backup.directory` of the root UFS every day at 5:30am UTC.
We recommend setting the backup time to an off-peak time to avoid interfering with other users of the system.

In the daily backup, the backup directory needs to be an absolute path within the root UFS.
For example, if `alluxio.master.backup.directory=/alluxio_backups`
and `alluxio.master.mount.table.root.ufs=hdfs://192.168.1.1:9000/alluxio/underfs`,
the default backup directory would be `hdfs://192.168.1.1:9000/alluxio_backups`.

The files to retain in the backup directory is limited by `alluxio.master.daily.backup.files.retained`.
Users can set this property to the number of backup files they want to keep in the backup directory.

### Backup delegation on HA cluster
Alluxio supports taking backup without causing service unavailability on a HA cluster configuration.
When enabled, Alluxio leading master delegates backups to backup masters in cluster.
After configuring backup delegation, both manual and scheduled backups will run in delegated mode.

Backup delegation can be configured with below properties:
- `alluxio.master.backup.delegation.enabled`: Whether to delegate backups to backup masters. Default: `false`.
- `alluxio.master.backup.heartbeat.interval`: Interval at which backup master that is taking the backup will update the leading master with current backup status. Default: `1sec`.

There are some advanced properties that controls the communication between Alluxio masters for coordinating the backup:
- `alluxio.master.backup.transport.timeout`: Communication timeout for messaging between masters for coordinating backup. Default: `5sec`.
- `alluxio.master.backup.connect.interval.min`: Minimum duration to sleep before retrying after unsuccessful handshake between backup master and leading master. Default: `1sec`.
- `alluxio.master.backup.connect.interval.max`: Maximum duration to sleep before retrying after unsuccessful handshake between backup master and leading master. Default: `10sec`.
- `alluxio.master.backup.abandon.timeout`: Specifies how long the leading master waits for a heart-beat before abandoning the backup. Default: `2min`.

Since it is uncertain which host will take the backup, it is suggested to use shared paths for taking backups with backup delegation.

Backup attempt will fail if delegation fails to find a backup master, thus favoring service availability.
For manual backups, you can pass `--allow-leader` option to allow leading master to take backup when there are no backup masters to delegate the backup.
This will causes temporary service unavailability while the backup is written on leading master.

### Restoring from a backup

To restore the Alluxio system from a journal backup, stop the system, format the
journal, then restart the system, passing the URI of the backup with the `-i`
(import) flag.

```console
$ ./bin/alluxio-stop.sh masters
$ ./bin/alluxio formatMasters
$ ./bin/alluxio-start.sh -i <backup_uri> masters
```

The `<backup_uri>` should be a full URI path that is available to all masters, e.g.
`hdfs://[namenodeserver]:[namenodeport]/alluxio_backups/alluxio-journal-YYYY-MM-DD-timestamp.gz`
If backups to the local disk of the leader master, copy the backup file to the same location in each master
and pass in the local backup file path.

If starting up masters individually, pass the `-i` argument to each one. The master which
becomes leader first will import the journal backup, and the rest will ignore the `-i`.

If the restore succeeds, you should see a log message along the lines of
```
INFO AlluxioMasterProcess - Restored 57 entries from backup
```
in the leading master logs.

### Changing masters

#### Embedded Journal Cluster
When internal leader election is used, Alluxio masters are determined with a quorum. Adding or removing
masters requires to keep this quorum in a consistent state.

##### Adding a new master
In order to prevent inconsistencies in the cluster configuration across masters, only a single master
should be added to an existing embedded journal cluster.

Below are the steps to add a new master to a live cluster:
* Prepare the new master.
    * New master should contain all existing masters in its embedded journal configuration, complete with its own address.
* Start new master.
    * This will introduce the new master to existing cluster.
    * New master will catch up with cluster's state in the background.
* Update existing masters' configuration with the new master address.
    * This is in order to make sure existing members will connect the new member directly upon restart.

Note: Adding to an already shut down cluster still requires adding only single master at a time.

Note: When adding a master to a single master cluster, you should shut down and update configuration for the existing master.
Then both masters could be started together.

See [here]({{ '/en/operation/Journal.html' | relativize_url }}) for configuring embedded journal.

##### Removing a master
Embedded journal cluster will take a notice when a member is not available anymore. Such masters will count
against failure tolerance of the cluster based on the initial member count. In order to resize the cluster
after a node failure an explicit action is required.

`Please note -domain parameter in below commands. This is because embedded journal based leader election is supported
for both regular masters and job service masters. You should supply correct value based on which cluster you intend to work on.`


1- Check current quorum state:

```console
$ ./bin/alluxio fsadmin journal quorum info -domain <MASTER | JOB_MASTER>
```

This will print out node status for all currently participating members of the embedded journal cluster. You should verify
that the removed master is shown as `UNAVAILABLE`.

2- Remove member from quorum:

`-address` option below should reflect the exact address that is returned by the `.. quorum info` command provided above.
```console
$ ./bin/alluxio fsadmin journal quorum remove -domain <MASTER | JOB_MASTER> -address <address>
```

3- Verify that removed member is no longer shown in the quorum info.

#### UFS Journal Cluster

To add a master to an HA Alluxio cluster, you can simply start a new Alluxio master process, with
the appropriate configuration. The configuration for the new master should be the same as other masters,
except that the parameter `alluxio.master.hostname=<MASTER_HOSTNAME>` should reflect the new hostname.
Once the new master is started, it will start interacting with ZooKeeper to participate in leader election.

Removing a master is as simple as stopping the master process. If the cluster is a single master cluster,
stopping the master will essentially shutdown the cluster, since the single master is down. If the
Alluxio cluster is an HA cluster, stopping the leading master will force ZooKeeper to elect a new leading master
and failover to that new leader. If a standby master is stopped, then the operation of the cluster is
unaffected. Keep in mind, Alluxio masters high availability depends on the availability on standby
masters. If there are not enough standby masters, the availability of the leading master will be affected.
It is recommended to have at least 3 masters for an HA Alluxio cluster.

## Advanced

### Managing the UFS journal size

When running with a single master, the journal folder size will grow indefinitely
as metadata operations are written to journal log files. To address this, production
deployments should run in HA mode with multiple Alluxio masters. The standby
masters will create checkpoints of the master state and clean up the logs that
were written prior to the checkpoints. For example, if 3 million Alluxio files were
created and then 2 million were deleted, the journal logs would contain 5 million
total entries. Then if a checkpoint is taken, the checkpoint will contain only the
metadata for the 1 million remaining files, and the original 5 million entries will be deleted.

By default, checkpoints are automatically taken every 2 million entries. This can be configured by
setting `alluxio.master.journal.checkpoint.period.entries` on the masters. Setting
the value lower will reduce the amount of disk space needed by the journal at the
cost of additional work for the standby masters.


#### Checkpointing on secondary master

If HA mode is not an option, it is possible to run a master on the same node as a
dedicated standby master. This second master exists only to write checkpoints, and
will not serve client requests if the leading master dies. In this setup, both
masters have similar memory requirements since they both need to hold all Alluxio
metadata in memory. To start a dedicated standby master for writing periodic checkpoints,
run

```console
$ ./bin/alluxio-start.sh secondary_master
```

#### Checkpointing on primary master

Checkpointing requires a pause in master metadata changes and causes temporary service
unavailability while the checkpoint is written. This operation may take hours depending on Alluxio namespace size.
Therefore, Alluxio primary master will not create checkpoints by default.

Restarting the current primary master to transfer the leadership to another running master periodically
can help avoiding primary master journal logs from growing unbounded when Alluxio is running in HA mode.

If HA mode is not an option, the following command can be used to manually trigger the checkpoint:

```console
$ ./bin/alluxio fsadmin journal checkpoint
```

Similar to the `backup` command, `checkpoint` command should
be used on an off-peak time to avoid interfering with other users of the system.

### Recovering from journal issues

The journal is integral to Alluxio's health. If the filesystem storing the journal
loses availability, no metadata operations can be performed on Alluxio. Similarly,
if the journal is accidentally deleted or its storage system becomes corrupted,
Alluxio must be reformatted to recover. To avoid the need for full reformatting,
we recommend taking regular journal backups at a time when the cluster is under low
load. Then if something happens to the journal, you can recover from one of the backups.

### Get human-readable journal

Alluxio journal is serialized and not human-readable. The following command
is given to read the Alluxio journal and write it to a directory in a human-readable format.

```console
$ ./bin/alluxio readJournal
```

Run `./bin/alluxio readJournal -help` for more detailed usage.
