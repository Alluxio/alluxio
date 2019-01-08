---
layout: global
title: Journal Management
nickname: Journal Management
group: Operations
priority: 0
---

* Table of Contents
{:toc}

Alluxio is fault-tolerant: force-killing the system will not lose metadata. To achieve
this, the master writes edit logs of all metadata changes. On startup,
a recovering master will read the edit logs to restore itself back to its previous state.
We use the term "journal" to refer to the system of edit logs used to support fault-tolerance.
The purpose of this documentation is to help Alluxio administrators understand and manage the Alluxio journal.

## UFS Journal vs Embedded Journal

The UFS journal simplifies certain aspects of Alluxio operation, but it relies on 
an external Zookeeper cluster for coordination, and relies on a UFS for persistent storage. 
To get reasonable performance, the UFS journal requires a UFS that supports fast streaming writes, 
such as HDFS or NFS.

The embedded journal does its own coordination and persistent storage, but has a few limitations.

First, `n` masters using the embedded journal can tolerate only `floor(n/2)` master failures, 
compared to `n-1` for UFS journal. For example, With `3` masters, UFS journal can tolerate `2` failures, 
while embedded journal can only tolerate `1`. However, UFS journal depends on Zookeeper, 
which similarly only supports `floor(#zookeeper_nodes / 2)` failures.

The other limitation is that embedded journal does not support dynamically changing master membership. 
With UFS journal, replacing a master on `host1` with a new master on `host2` is as simple as starting a new master on `host2`, 
then killing the master on `host1`. Changing the masters in an embedded journal cluster requires backing up the cluster, 
shutting it down, and then starting up again with the new masters using the backup.

If a fast UFS and Zookeeper cluster are readily available and stable, it is recommended to use the UFS journal. 
Otherwise, we recommend using the embedded journal.

## UFS Journal Configuration

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

**Configuration examples:**

Use HDFS to store the journal:
```
alluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/alluxio_journal
```

Use the local file system to store the journal:
```
alluxio.master.journal.folder=/opt/alluxio/journal
```

## Embedded Journal Configuration

### Required configuration

The configuration specified below should be applied to both Alluxio servers and Alluxio clients.

Set the journal type to "EMBEDDED":
```
alluxio.master.journal.type=EMBEDDED
```

Set the addresses of all masters in the cluster. The default embedded journal port is `19200`.
```
alluxio.master.embedded.journal.addresses=master_hostname_1:19200,master_hostname_2:19200,master_hostname_3:19200
```

### Optional configuration

* `alluxio.master.embedded.journal.port`: The port masters use for embedded journal communication. Default: `19200`.
* `alluxio.master.port`: The port masters use for RPCs. Default: `19998`.
* `alluxio.master.rpc.addresses`: A list of comma-separated `host:port` RPC addresses where the client 
should look for masters when using multiple masters without Zookeeper. This property is not used when Zookeeper is enabled, 
since Zookeeper already stores the master addresses. If this is not set, clients will look for masters using the hostnames 
from `alluxio.master.embedded.journal.addresses` and the master rpc port.

### Job service configuration

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

```bash
# This permanently deletes all Alluxio metadata, so be careful with this operation
$ bin/alluxio formatMaster
```

## Operations

### Manually backing up the journal

Alluxio supports taking journal backups so that Alluxio metadata can be restored
to a previous point in time. Generating a backup causes temporary service
unavailability while the backup is written.

To generate a backup, use the `fsadmin backup` CLI command.
```bash
$ bin/alluxio fsadmin backup
```

By default, this will write a backup named
`alluxio-journal-YYYY-MM-DD-timestamp.gz` to the "/alluxio_backups" directory of
the root under storage system, e.g. hdfs://cluster/alluxio_backups. This default
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

### Restoring from a backup

To restore the Alluxio system from a journal backup, stop the system, format the
journal, then restart the system, passing the URI of the backup with the `-i`
(import) flag.

```bash
$ bin/alluxio-stop.sh masters
$ bin/alluxio formatMaster
$ bin/alluxio-start.sh -i <backup_uri> masters
```

The `<backup_uri>` should be a full URI path that is available to all masters, e.g.
`hdfs://[namenodeserver]:[namenodeport]/alluxio_backups/alluxio-journal-YYYY-MM-DD-timestamp.gz`

If starting up masters individually, pass the `-i` argument to each one. The master which
becomes leader first will import the journal backup, and the rest will ignore the `-i`.

If the restore succeeds, you should see a log message along the lines of
```
INFO AlluxioMasterProcess - Restored 57 entries from backup
```
in the leading master logs.

### Changing masters

If the journal is stored in a shared storage system like HDFS, changing masters is easy.
As long as the new master sets `alluxio.master.journal.folder` the same as the old
master, it will start up in the same state that the old master left off.

If the journal is stored on the local filesystem, the journal folder needs to be
copied to the new master.

## Advanced

### Managing the journal size

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

If HA mode is not an option, it is possible to run a master on the same node as a
dedicated standby master. This second master exists only to write checkpoints, and
will not serve client requests if the leading master dies. In this setup, both
masters have similar memory requirements since they both need to hold all Alluxio
metadata in memory. To start a dedicated standby master for writing periodic checkpoints,
run

```bash
$ bin/alluxio-start.sh secondary_master
```

### Recovering from journal issues

The journal is integral to Alluxio's health. If the filesystem storing the journal
loses availability, no metadata operations can be performed on Alluxio. Similarly,
if the journal is accidentally deleted or its storage system becomes corrupted,
Alluxio must be reformatted to recover. To avoid the need for full reformatting,
we recommend taking regular journal backups at a time when the cluster is under low
load. Then if something happens to the journal, you can recover from one of the backups.
