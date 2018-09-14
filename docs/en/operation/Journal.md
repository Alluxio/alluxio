---
layout: global
title: Journal Management
nickname: Journal Management
group: Operations
priority: 0
---

TODO(andrew): Add more information
- Talk about log files and checkpoints, how to manage the size
- Lead with what the journal is, and its fault tolerance guarantees
- Introduction should describe purpose of doc, which is administering the journal of an Alluxio cluster.

Outline:
# Configuration
# Formatting the Journal
# Operations
## Manually Backing up the Journal
## Restoring from a Backup
## Changing Masters
# Advanced
## Running Alluxio in High Availability (HA) Mode
## Space requirements (log files, checkpoints)

* Table of Contents
{:toc}

Alluxio maintains a journal to support persistence of metadata operations.
When a request modifies Alluxio state, e.g. creating or renaming files, the
Alluxio master will write a journal entry for the operation before returning
a successful response to the client. The journal entry is written to a
persistent storage such as disk or HDFS, so even if the Alluxio
master process is killed, state will be recovered on restart.

## Configuration

The most important configuration value to set for the journal is
`alluxio.master.journal.folder`. This must be set to a shared filesystem available
to all masters. In single-master mode, a local filesystem path is fine. With
multiple masters distributed across different machines, the shared folder should
be in a distributed system that supports flush such as HDFS or NFS. It is not
recommended to put the journal in an object store. With an object store, every
update to the journal requires a new object to be created, which is
prohibitively slow for most serious use cases.

**Configuration examples:**
Use HDFS to store the journal:
```
alluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/dir/alluxio_journal
```

Use the local file system to store the journal: 
```
alluxio.master.journal.folder=/opt/alluxio/journal
```

## Formatting

Before starting Alluxio for the first time, the journal must be formatted.

**WARNING: formatting the journal will delete all Alluxio metadata**
```bash
$ bin/alluxio formatMaster
```

## Backup

Alluxio supports taking journal backups so that Alluxio metadata can be restored
to a previous point in time. Generating a backup causes temporary service
unavailability while the backup is happening.

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

See the [backup command documentation](Admin-CLI.html#backup) for additional options for specifying
where to write a backup.

## Restore

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

If the restore succeeds, you should see a log message along the lines of
```
INFO AlluxioMasterProcess - Restored 57 entries from backup
```
in the primary master logs.
