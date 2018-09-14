---
layout: global
title: Admin Command Line Interface
group: Operations
priority: 0
---

* Table of Contents
{:toc}

Alluxio's admin command line interface provides admins with operations to manage the Alluxio filesystem.
You can invoke the following command line utility to get all the subcommands:

```bash
$ ./bin/alluxio fsadmin
Usage: alluxio fsadmin [generic options]
       [backup]
       [doctor]
       [report]
       [ufs --mode <noAccess/readOnly/readWrite> <ufsPath>]
       ...
```

## List of Operations

<table class="table table-striped">
  <tr><th>Operation</th><th>Syntax</th><th>Description</th></tr>
  {% for item in site.data.table.fsadmin-command %}
    <tr>
      <td>{{ item.operation }}</td>
      <td>{{ item.syntax }}</td>
      <td>{{ site.data.table.en.fsadmin-command[item.operation] }}</td>
    </tr>
  {% endfor %}
</table>

## Example Use Cases

### backup

The `backup` command creates a backup of Alluxio metadata.

Back up to the default backup folder (configured by `alluxio.master.backup.directory`)
```
$ ./bin/alluxio fsadmin backup
Successfully backed up journal to hdfs://mycluster/opt/alluxio/backups/alluxio-backup-2018-5-29-1527644810.gz
```
Back up to a specific directory in the under storage.
```
$ ./bin/alluxio fsadmin backup /alluxio/special_backups
Successfully backed up journal to hdfs://mycluster/opt/alluxio/backups/alluxio-backup-2018-5-29-1527644810.gz
```
Back up to a specific directory on the primary master's local filesystem.
```
$ ./bin/alluxio fsadmin backup /opt/alluxio/backups/ --local
Successfully backed up journal to file:///opt/alluxio/backups/alluxio-backup-2018-5-29-1527644810.gz on master Master2
```

### doctor

The `doctor` command shows Alluxio errors and warnings.

```bash
# shows server-side configuration errors and warnings
$ ./bin/alluxio fsadmin doctor configuration
```

### report

// TODO(lu) detailed description about each of the subcommands.
// such as the report capacity usage with filter tags.

The `report` command provides Alluxio running cluster information. 

```bash
# Report cluster summary
$ ./bin/alluxio fsadmin report
#
# Report worker capacity information
$ ./bin/alluxio fsadmin report capacity
#
# Report metrics information
$ ./bin/alluxio fsadmin report metrics
#
# Report under file system information
$ ./bin/alluxio fsadmin report ufs 
```

You can use `-h` to display helpful information about the command.

### ufs

The `ufs` command provides options to update attributes of a mounted under storage. The option `mode` can be used
to put an under storage in maintenance mode. Certain operations can be restricted at this moment.

For example, an under storage can enter `readOnly` mode to disallow write operations. Alluxio will not attempt any
write operations on the under storage.

```bash
$ ./bin/alluxio fsadmin ufs --mode readOnly hdfs://ns
```

The `fsadmin ufs` subcommand takes a UFS URI as an argument. The argument should be a root
UFS URI like `hdfs://<name-service>/`, and not `hdfs://<name-service>/<folder>`.
