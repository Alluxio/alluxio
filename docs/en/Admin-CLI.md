---
layout: global
title: Admin Command Line Interface
group: Features
priority: 0
---

* Table of Contents
{:toc}

Alluxio's admin command line interface provides admins with operations to manage the Alluxio filesystem.
You can invoke the following command line utility to get all the subcommands:

```bash
$ ./bin/alluxio fsadmin
Usage: alluxio fsadmin [generic options]
       [report]
       [ufs --mode <noAccess/readOnly/readWrite> <ufsPath>]
       ...
```

The `fsadmin ufs` subcommand that takes the UFS URI as argument, the argument should be the root
UFS URI like `hdfs://<name-service>/`, and not `hdfs://<name-service>/<folder>`.

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

### report

The `report` command provides Alluxio running cluster information. 

```bash
# Report cluster summary
$ ./bin/alluxio fsadmin report
#
# Report worker capacity information
$ ./bin/alluxio fsadmin report capacity
#
# Report runtime configuration information
$ ./bin/alluxio fsadmin report configuration
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
