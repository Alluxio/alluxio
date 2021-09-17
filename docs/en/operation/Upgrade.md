---
layout: global
title: Upgrading
nickname: Upgrading
group: Operations
priority: 15
---

In normal cases, users can directly shutdown the current Alluxio processes, change
the Alluxio binaries to a newer version, configure Alluxio clusters similar to before,
and start Alluxio processes with the existing journal folder/address for upgrading.
Alluxio can read the previous journal files and recover the Alluxio metadata automatically.

There are two cases where master journals are not backward compatible and additional steps are required to upgrade the Alluxio cluster:

- Upgrading from Alluxio 1.x to Alluxio 2.x
- Upgrading from Alluxio 2.3.x and below to Alluxio 2.4.0 and above when using [embedded journal]({{ '/en/operation/Journal.html' | relativize_url}}).

This document goes over how to upgrade Alluxio to a non-backward compatible version.
Even when upgrading to a backward-compatible version, it is still recommended to follow the steps below to create a backup before upgrading.

## Create a backup on the current version

alluxio-1.8.1 introduced a journal backup feature. 
Please note that the Alluxio binaries should not be changed before taking the backup.
With versions older than 1.8.1 master(s) running,
create a journal backup by running

```console
$ ./bin/alluxio fsadmin backup
Successfully backed up journal to ${BACKUP_PATH}
```

`${BACKUP_PATH}` will be determined by the date, and the configuration of your
journal. By default, the backup file will be saved to the `alluxio.master.backup.directory` of your cluster root UFS. 
One can also backup to the local filesystem of the current leading master node with `backup [local_address] --local` command.

## Upgrade and start from backup

Next, download and untar the newer version Alluxio. First format the cluster with

```console
$ ./bin/alluxio format
```

Then start the cluster with the `-i ${BACKUP_PATH}` argument, replacing
`${BACKUP_PATH}` with your specific backup path.

```console
$ ./bin/alluxio-start.sh -i ${BACKUP_PATH} all
```

Note that the `${BACKUP_PATH}` should be a full path like HDFS address that can be accessed by all the Alluxio masters. 
If backing up to a local filesystem path, remember to copy the backup file to the same location of all masters nodes, 
and then start all the masters with the local backup file path.

## Upgrade Clients and Servers

Alluxio 2.x makes significant changes in the RPC layer,
so pre-2.0.0 clients do not work with post-2.0.0 servers, and vice-versa.
Upgrade all applications to use the alluxio-2.x client.

## Additional Options

### Alluxio worker ramdisk cache persistence

If you have configured Alluxio workers with ramdisk caches, you may
persist and restore the contents of those caches using another storage
medium (eg., the host machine's local disk).

Use the `-c` flag with `alluxio-stop.sh` to specify a path for the workers to save
the contents of their ramdisks to (workers will save contents to their own host's filesystems):
```
$ ./bin/alluxio-stop.sh workers -c ${CACHE_PATH}
```
- **WARNING:** This will overwrite and replace any existing contents in the provided `${CACHE_PATH}`

Afterwards, use the `-c` flag with `alluxio-start.sh` to specify the directory containing
the workers' ramdisk caches.
```
$ ./bin/alluxio-start.sh workers NoMount -c ${CACHE_PATH}
```
- **WARNING:** This will overwrite and replace any existing contents in
the configured workers' ramdisk paths.
