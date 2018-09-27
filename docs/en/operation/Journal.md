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
The purpose of this doc is to help Alluxio administrators understand and manage the Alluxio journal.

## Configuration

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
the root under file system, e.g. hdfs://cluster/alluxio_backups. This default
backup directory can be configured by setting `alluxio.master.backup.directory`

```
alluxio.master.backup.directory=/alluxio/backups
```

See the [backup command documentation](Admin-CLI.html#backup) for additional backup options.

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
becomes primary first will import the journal backup, and the rest will ignore the `-i`.

If the restore succeeds, you should see a log message along the lines of
```
INFO AlluxioMasterProcess - Restored 57 entries from backup
```
in the primary master logs.

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
deployments should run in HA mode with multiple Alluxio masters. The non-primary
masters will create checkpoints of the master state and clean up the logs that
were written prior to the checkpoints. For example, if 3 million alluxio files were
created and then 2 million were deleted, the journal logs would contain 5 million
total entries. Then if a checkpoint is taken, the checkpoint will contain only the
metadata for the 1 million remaining files, and the original 5 million entries will be deleted.

By default, checkpoints are automatically taken every 2 million entries. This can be configured by
setting `alluxio.master.journal.checkpoint.period.entries` on the masters. Setting
the value lower will reduce the amount of disk space needed by the journal at the
cost of additional work for the secondary masters.

If HA mode is not an option, it is possible to run a master on the same node as a
dedicated secondary master. The second master exists only to write checkpoints, and
will not serve client requests if the primary master dies. In this setup, both
masters have similar memory requirements since they both need to hold all Alluxio
metadata in memory. To start a dedicated secondary master for writing periodic checkpoints,
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
