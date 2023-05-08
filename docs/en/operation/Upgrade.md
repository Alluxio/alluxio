---
layout: global
title: Upgrading
nickname: Upgrading
group: Operations
priority: 15
---

* Table of Contents
{:toc}

## Basic upgrading procedure

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

After stopping your existing Alluxio cluster, download and untar the newer version of Alluxio.
Copy over the old configuration files from the `/conf` directory. Then format the cluster with

```console
$ ./bin/alluxio format
```
- **WARNING:** This will re-format the ramdisks on the Alluxio workers (i.e: wipe their contents).
If you wish to preserve the worker ramdisks please see
[Alluxio worker ramdisk cache persistence]({{ '/en/operation/Upgrade.html' | relativize_url}}#alluxio-worker-ramdisk-cache-persistence).

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

Please refer to the following steps：
1. Back up the metadata of the files in Alluxio. Please refer to the [documentation]({{ '/en/operation/Admin-CLI.html' | relativize_url }}#backup) on `backup` command.
2. Stop the Alluxio cluster
```console
$ ./bin/alluxio-stop.sh all
```
3. Update the Alluxio client jar path for all your applications. For example, `Yarn`, `Spark`, `Hive` and `Presto`. eg., In the "YARN (MR2 Included)" section of the Cloudera Manager, in the "Configuration" tab, search for the parameter "Gateway Client Environment Advanced Configuration Snippet (Safety Valve) for hadoop-env.sh". Then add the following line to the script:
```console
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```
   It should look like:
   ![locality]({{ '/img/screenshot_cdh_compute_hadoop_classpath.png' | relativize_url }})
4. Start the Alluxio cluster
```console
$ ./bin/alluxio-start.sh all
```
5. If you have updated the Alluxio client jar for an application, restart that application to use the new Alluxio client jar.

### Rolling upgrade/restart masters

When the cluster is running in high-availability mode (running multiple Alluxio masters), if the admin wants to restart all masters
in the cluster, it should be done in a rolling restart fashion to minimize service unavailable time.
The service should only be unavailable during primary master failover, once there is a primary master in HA,
restarting standby masters will not interrupt the service.

If the HA is on Embeddded Journal (using Raft), this is an example of how to perform rolling upgrade:
```shell
# First check all master nodes in the cluster
$ ./bin/alluxio fsadmin report
...
Raft journal addresses:
  master-0:19200
  master-1:19200
  master-2:19200
Master Address        State      Version       REVISION
master-0:19998        PRIMARY  alluxio-2.9.0  abcde
master-1:19998        STANDBY  alluxio-2.9.0  abcde
master-2:19998        STANDBY  alluxio-2.9.0  abcde

# Pick one standby master and restart it using the higher version
$ ssh master-1
$ bin/alluxio-start.sh master

# Wait for that master to join the quorum and observe it is using the higher verison
$ ./bin/alluxio fsadmin report
...
Raft journal addresses:
  master-0:19200
  master-1:19200
  master-2:19200
Master Address        State      Version       REVISION
master-0:19998        PRIMARY  alluxio-2.9.0  abcde
master-1:19998        STANDBY  alluxio-2.9.1  hijkl
master-2:19998        STANDBY  alluxio-2.9.0  abcde

# Do the same for the other standby master master2

# Manually failover the primary to one upgraded standby master, now master-0 becomes standby
$ ./bin/alluxio fsadmin journal quorum elect -address master-1:19200

# Restart master-0 with the higher version and wait for it to re-join the quorum
# Then you should observe all masters are on the higher version
$ ./bin/alluxio fsadmin report
...
Raft journal addresses:
  master-0:19200
  master-1:19200
  master-2:19200
Master Address        State      Version       REVISION
master-0:19998        STANDBY  alluxio-2.9.1  hijkl
master-1:19998        PRIMARY  alluxio-2.9.1  hijkl
master-2:19998        STANDBY  alluxio-2.9.1  hijkl

# Wait for all workers register with the new primary, and run tests to validate the service
$ bin/alluxio runTests
```

Similarly, if the HA is on UFS Journal (using ZooKeeper), the admin can restart masters one by one in the same order.
The only difference is there is no command to manually trigger a primary master failover. The admin can
directly kill the primary master process, after a brief timeout, one standby master will realize and become the new primary.

### Rolling upgrade/restart workers

If the admin wants to restart workers without interrupting ongoing service, there are now ways to rolling restart
all workers without failing ongoing I/O requests. Typically, we want to restart workers to apply configuration changes,
or to upgrade to a newer version.

A typical workflow of rolling upgrade workers looks as follows:
```shell
# First check all worker nodes in the cluster
$ ./bin/alluxio fsadmin report capacity
...
Worker Name        State            Last Heartbeat   Storage       MEM              Version          Revision
data-worker-1      ACTIVE           1                capacity      10.67GB          2.9.0            abcde
                                                     used          0B (0%)
data-worker-0      ACTIVE           2                capacity      10.67GB          2.9.0            abcde
                                                     used          0B (0%)
data-worker-2      ACTIVE           0                capacity      10.67GB          2.9.0            abcde
                                                     used          0B (0%)
...

# Pick a batch of workers to decommission, e.g. this batch is 2 workers
$ ./bin/alluxio fsadmin decommissionWorker -a data-worker-0,data-worker-1 -w 5m
Decommissioning worker data-worker-0:30000
Set worker data-worker-0:30000 decommissioned on master
Decommissioning worker data-worker-1:30000
Set worker data-worker-1:30000 decommissioned on master
Sent decommission messages to the master, 0 failed and 2 succeeded
Failed ones: []
Clients take alluxio.user.worker.list.refresh.interval=2min to be updated on the new worker list so this command will block for the same amount of time to ensure the update propagates to clients in the cluster.
Verifying the decommission has taken effect by listing all available workers on the master
Now on master the available workers are: [data-worker-2,data-worker-3,...]
Polling status from worker data-worker-0:30000
Polling status from worker data-worker-1:30000
...
There is no operation on worker data-worker-0:30000 for 20 times in a row. Worker is considered safe to stop.
Polling status from worker data-worker-1:30000
There is no operation on worker data-worker-1:30000 for 20 times in a row. Worker is considered safe to stop.
Waited 3 minutes for workers to be idle
All workers are successfully decommissioned and now idle. Safe to kill or restart this batch of workers now.

# Now you will be able to observe those workers' state have changed from ACTIVE to DECOMMISSIONED.
$ ./bin/alluxio fsadmin report capacity
...
Worker Name        State            Last Heartbeat   Storage       MEM              Version          Revision
data-worker-1      DECOMMISSIONED   1                capacity      10.67GB          2.9.0            abcde
                                                     used          0B (0%)
data-worker-0      DECOMMISSIONED   2                capacity      10.67GB          2.9.0            abcde
                                                     used          0B (0%)
data-worker-2      ACTIVE           0                capacity      10.67GB          2.9.0            abcde
                                                     used          0B (0%)

# Then you can restart the decommissioned workers. The workers will start normally and join the cluster.
$ ssh data-worker-0
$ ./bin/alluxio-start.sh worker
...

# Now you will be able to observe those workers become ACTIVE again and have a higher version
$ ./bin/alluxio fsadmin report capacity
...
Worker Name        State            Last Heartbeat   Storage       MEM              Version          Revision
data-worker-1      ACTIVE           1                capacity      10.67GB          2.9.1            hijkl
                                                     used          0B (0%)
data-worker-0      ACTIVE           2                capacity      10.67GB          2.9.1            hijkl
                                                     used          0B (0%)
data-worker-2      ACTIVE           0                capacity      10.67GB          2.9.0            abcde
                                                     used          0B (0%)

# You can run I/O tests against the upgraded workers to validate they are serving, before moving to upgrade the next batch
$ bin/alluxio runTests --workers data-worker-0,data-worker-1

# Keep performing the steps above until all workers are upgraded
```

See more details about the `decommissionWorker` command in
[documentation]({{ '/en/operation/Admin-CLI.html' | relativize_url }}#decommissionworker).

### Rolling restart/upgrade other components

Other components like the Job Master, Job Worker and Proxy do not support rolling upgrade at the moment.
The admin can manually restart them in batches.

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
