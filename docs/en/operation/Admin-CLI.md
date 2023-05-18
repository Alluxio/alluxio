---
layout: global
title: Admin Command Line Interface
nickname: Admin CLI
group: Operations
priority: 2
---

* Table of Contents
{:toc}

Alluxio's admin command line interface provides admins with operations to manage the Alluxio filesystem.
You can invoke the following command line utility to get all the subcommands:

```shell
$ ./bin/alluxio fsadmin
Usage: alluxio fsadmin [generic options]
	 [backup [directory] [--local] [--allow-leader]]
	 [doctor [category]]
	 [getBlockInfo [blockId]]
	 [journal [checkpoint] [quorum]]
	 [metrics [clear]]
	 [pathConf [add] [show] [list] [remove]]
	 [report [category] [category args]]
	 [statelock]
	 [ufs [--mode <noAccess/readOnly/readWrite>] <ufsPath>]
	 [updateConf key1=val1 [key2=val2 ...]]
	 [decommissionWorker [--addresses worker0,worker1] [--wait 5m] [--disable]]
	 [enableWorker [--addresses worker0,worker1]]
```

## Operations

### backup

The `backup` command backs up all Alluxio metadata to the backup directory configured on the leader master.

Back up to the default backup folder `/alluxio_backups` of the root under storage system. 
This default backup directory can be configured by setting `alluxio.master.backup.directory`. 
```shell
$ ./bin/alluxio fsadmin backup
Backup Host        : masters-1                          
Backup URI         : hdfs://masters-1:9000/alluxio_backups/alluxio-backup-2020-10-13-1602619110769.gz
Backup Entry Count : 4
```
Note that the user running the `backup` command need to have write permission to the backup folder of root under storage system.

Back up to a specific directory in the root under storage system.
```shell
$ ./bin/alluxio fsadmin backup /alluxio/special_backups
Backup Host        : masters-1                          
Backup URI         : hdfs://masters-1:9000/alluxio/special_backups/alluxio-backup-2020-10-13-1602619216699.gz
Backup Entry Count : 4
```

Back up to a specific directory on the leading master's local filesystem.
```shell
$ ./bin/alluxio fsadmin backup /opt/alluxio/backups/ --local
Backup Host        : AlluxioSandboxEJSC-masters-1                          
Backup URI         : file:///opt/alluxio/backups/alluxio-backup-2020-10-13-1602619298086.gz
Backup Entry Count : 4
```

Allow the leading master to take a backup if no standby masters are available.
```shell
$ ./bin/alluxio fsadmin backup --allow-leader
```

Force the leading master to take the backup even if standby masters are available.
```shell
$ bin/alluxio fsadmin backup --bypass-delegation
```

### journal
The `journal` command provides several sub-commands for journal management.

**quorum:** is used to query and manage embedded journal powered leader election.

```shell
# Get information on existing state of the `MASTER` or `JOB_MASTER` leader election quorum.
$ ./bin/alluxio fsadmin journal quorum info -domain <MASTER | JOB_MASTER>
```

```shell
# Remove a member from leader election quorum.
$ ./bin/alluxio fsadmin journal quorum remove -domain <MASTER | JOB_MASTER> -address <HOSTNAME:PORT>
```

```shell
# Elect a specific member of the quorum as the new leader.
$ ./bin/alluxio fsadmin journal quorum elect -address <HOSTNAME:PORT>
```

**checkpoint:** is used to create a checkpoint in the primary master journal system.

This command is mainly used for debugging and to avoid master journal logs from growing unbounded.

Checkpointing requires a pause in master metadata changes, so use this command sparingly to avoid 
interfering with other users of the system.

```shell
$ ./bin/alluxio fsadmin journal checkpoint
```

### doctor

The `doctor` command gives recommendations and warnings. It can diagnose inconsistent configurations
across different Alluxio nodes as well as alert the operator when worker storage volumes are missing.

```shell
# shows server-side configuration errors and warnings
$ ./bin/alluxio fsadmin doctor configuration
# shows worker storage health errors and warnings
$ ./bin/alluxio fsadmin doctor storage
```

### getBlockInfo

The `getBlockInfo` command provides the block information and file path of a block id.
It is primarily intended to assist power users in debugging their system.

```shell
$ ./bin/alluxio fsadmin getBlockInfo <block_id>
BlockInfo{id=16793993216, length=6, locations=[BlockLocation{workerId=8265394007253444396, address=WorkerNetAddress{host=local-mbp, rpcPort=29999, dataPort=29999, webPort=30000, domainSocketPath=, tieredIdentity=TieredIdentity(node=local-mbp, rack=null)}, tierAlias=MEM, mediumType=MEM}]}
This block belongs to file {id=16810770431, path=/test2}
```

### metrics

The `metrics` command provides operations for Alluxio metrics system.

The command `metrics clear`, will clear all metrics stored in the alluxio cluster.
This command is useful for collecting metrics for specific jobs and tests.
It should be used sparingly, since it will affect the current metrics reporting and can affect worker/client heartbeats
to the leading master.

If `--master` option is used, all the metrics stored in Alluxio leading master will be cleared.
If `--workers <WORKER_HOSTNAME_1>,<WORKER_HOSTNAME_2>` is used, metrics in specific workers will be cleared.

If you are clearing metrics on a large Alluxio cluster with many workers, you can use the `--parallelism <#>` option to
choose the `#` of workers to clear in parallel. For example, if your cluster has 200 workers, running with a
parallelism factor of 10 will clear execute the command on 10 workers at a time until all metrics are cleared.

```shell
# Clear metrics of the whole alluxio cluster including leading master and workers
$ ./bin/alluxio fsadmin metrics clear
# Clear metrics of alluxio leading master
$ ./bin/alluxio fsadmin metrics clear --master
# Clear metrics of specific workers
$ ./bin/alluxio fsadmin metrics clear --workers <WORKER_HOSTNAME_1>,<WORKER_HOSTNAME_2>
# Clear metrics of an alluxio cluster with many workers in parallel
$ ./bin/alluxio fsadmin metrics clear --parallelism 10
```

### pathConf

The `pathConf` command manages [path defaults]({{ '/en/operation/Configuration.html' | relativize_url }}#path-defaults).

#### list

`pathConf list` lists paths configured with path defaults.

```shell
$ ./bin/alluxio fsadmin pathConf list

/a
/b
```
The above command shows that there are path defaults set for paths with prefix `/a` and `/b`.

#### show

`pathConf show` shows path defaults for a specific path.

It has two modes:
1. without option `--all`, only show path defaults set for the specific path;
2. with option `--all`, show path defaults set for all paths that are prefixes of the specified path.

For example, suppose path defaults `property1=value1` is set for `/a`,
and `property2=value2` is set for `/a/b`.

Then without `--all`, only properties for `/a/b` are shown:
```shell
$ ./bin/alluxio fsadmin pathConf show /a/b

property2=value2
```

With `--all`, since `/a` is a prefix of `/a/b`, properties for both `/a` and `/a/b` are shown:
```shell
$ ./bin/alluxio fsadmin pathConf show --all /a/b

property1=value1
property2=value2
```

#### add

`pathConf add` adds or updates path defaults. Only properties with scope equal to or broader than the
client scope can be set as path defaults.

```shell
$ ./bin/alluxio fsadmin pathConf add --property property1=value1 --property property2=value2 /tmp
```

The above command adds two properties as path defaults for paths with prefix `/tmp`.

```shell
$ ./bin/alluxio fsadmin pathConf add --property property1=value2 /tmp
```
The above command updates the value of `property1` from `value1` to `value2` for path defaults of `/tmp`.

#### remove

`pathConf remove` removes path-specific configurations for a path.

```shell
$ ./bin/alluxio fsadmin pathConf remove [-R/--recursive] [--keys property1,property2] PATH
```

**Options:**

- `--keys`: specifies which configuration properties are to be removed for PATH
  (and all child paths if `-R` is specified). If omitted, all properties will be removed.
- `-R` or `--recursive`: recursively removes configuration properties from PATH and all its
  children.

**Examples:**

```shell
$ ./bin/alluxio fsadmin pathConf remove --keys property1,property2 /tmp
```

The above command removes configuration with key `property1` and `property2` from path
defaults for `/tmp`.

```shell
$ ./bin/alluxio fsadmin pathConf remove -R --keys property1,property2 /tmp
```

The above command removes configuration with key `property1` and `property2` from path
defaults for `/tmp` and all its children.

The difference between the two commands is that if a child path `/tmp/a` has an overridden
configuration for `property1` and the removal is recursive, then `property1` will be 
removed for `/tmp/a`, too.

```shell
$ ./bin/alluxio fsadmin pathConf remove -R /
```

This command removes all path-specific configurations for all paths on any level in Alluxio.

### report

The `report` command provides Alluxio running cluster information.

If no argument is passed in, `report` will report the leading master, worker number, and capacity information.

```shell
$ ./bin/alluxio fsadmin report
Alluxio cluster summary:
    Master Address: localhost:19998
    Zookeeper Enabled: false
    Live Workers: 1
    Lost Workers: 0
    Total Capacity: 10.45GB
    Used Capacity: 0B
    (only a subset of the results is shown)
```

`report capacity` will report Alluxio cluster capacity information for different subsets of workers:
* `-live` Live workers
* `-lost` Lost workers
* `-decommissioned` Decommissioned workers
* `-workers <worker_names>` Specified workers, host names or ip addresses separated by `,`.

```shell
# Capacity information of all workers
$ ./bin/alluxio fsadmin report capacity
# Capacity information of live workers
$ ./bin/alluxio fsadmin report capacity -live
# Capacity information of specified workers
$ ./bin/alluxio fsadmin report capacity -workers AlluxioWorker1,127.0.0.1
```

A typical output looks like below:
```shell
$ ./bin/alluxio fsadmin report capacity
Capacity information for all workers:
    Total Capacity: 10.67GB
        Tier: MEM  Size: 10.67GB
    Used Capacity: 0B
        Tier: MEM  Size: 0B
    Used Percentage: 0%
    Free Percentage: 100%

Worker Name        State            Last Heartbeat   Storage       MEM              Version          Revision
data-worker-1      ACTIVE           166              capacity      10.67GB          2.9.0            ffcb706497bf47e43d5b2efc90f664c7a3e7014e
                                                     used          0B (0%)
```

`report metrics` will report the metrics stored in the leading master which includes
leading master process metrics and aggregated cluster metrics.

```shell
$ ./bin/alluxio fsadmin report metrics
```

`report ufs` will report all the mounted under storage system information of Alluxio cluster.

```shell
$ ./bin/alluxio fsadmin report ufs
Alluxio under storage system information:
hdfs://localhost:9000/ on / (hdfs, capacity=-1B, used=-1B, not read-only, not shared, properties={})
```

`report jobservice` will report a summary of the job service.

```shell
$ bin/alluxio fsadmin report jobservice
Master Address                State    Start Time       Version                          Revision
MigrationTest-master-1:20001  PRIMARY  20230425-110043  2.9.0                            ac6a0616
MigrationTest-master-2:20001  STANDBY  20230425-110044  2.9.0                            ac6a0616
MigrationTest-master-3:20001  STANDBY  20230425-110050  2.9.0                            ac6a0616

Job Worker               Version       Revision Task Pool Size Unfinished Tasks Active Tasks Load Avg
MigrationTest-workers-2  2.9.0         ac6a0616 10             1303             10           1.08, 0.64, 0.27
MigrationTest-workers-3  2.9.0         ac6a0616 10             1766             10           1.02, 0.48, 0.21
MigrationTest-workers-1  2.9.0         ac6a0616 10             1808             10           0.73, 0.5, 0.23

Status: CREATED   Count: 4877
Status: CANCELED  Count: 0
Status: FAILED    Count: 1
Status: RUNNING   Count: 0
Status: COMPLETED Count: 8124

10 Most Recently Modified Jobs:
Timestamp: 10-28-2020 22:02:34:001       Id: 1603922371976       Name: Persist             Status: COMPLETED
Timestamp: 10-28-2020 22:02:34:001       Id: 1603922371982       Name: Persist             Status: COMPLETED
(only a subset of the results is shown)

10 Most Recently Failed Jobs:
Timestamp: 10-24-2019 17:15:22:946       Id: 1603922372008       Name: Persist             Status: FAILED

10 Longest Running Jobs:
```

`report proxy` will report a summary of the proxy instances in the cluster.

```shell
$ ./bin/alluxio fsadmin report proxy
1 Proxy instances in the cluster, 1 serving and 0 lost

Address                          State    Start Time       Last Heartbeat Time  Version        Revision
Alluxio-Proxy-Node-1:39997       ACTIVE   20230421-165608  20230421-170201      2.9.0          c697105199e29a480cf6251494d367cf325123a0
```

### statelock

The `statelock` command provides information about the waiters and holders of the alluxio statelock.
This command can help diagnose any long running requests issued by users or the Alluxio system.

### ufs

The `ufs` command provides options to update attributes of a mounted under storage. The option `--mode` can be used
to put an under storage in maintenance mode. Certain operations can be restricted at this moment.

For example, an under storage can enter `readOnly` mode to disallow write operations. Alluxio will not attempt any
write operations on the under storage.

```shell
$ ./bin/alluxio fsadmin ufs --mode readOnly hdfs://ns
```

The `fsadmin ufs` subcommand takes a UFS URI as an argument. The argument should be a root
UFS URI like `hdfs://<name-service>/`, and not `hdfs://<name-service>/<folder>`.

### updateConf

The `updateConf` command provides a way to update config for current running services if `alluxio.conf.dynamic.update.enabled` is set to true.
The request is sent to Alluxio master directly,
but the configuration change is also propagated to other services such as worker, fuse, and proxy.

```shell
$ ./bin/alluxio fsadmin updateConf key1=val1 key2=val2
Updated 2 configs
```

Till Alluxio 2.9.0, Alluxio supports updating the configurations on the running service as follows:

```
alluxio.master.unsafe.direct.persist.object.enabled
alluxio.master.worker.timeout
alluxio.master.audit.logging.enabled
alluxio.master.ufs.managed.blocking.enabled
alluxio.master.metastore.inode.inherit.owner.and.group
```

### decommissionWorker

The `decommissionWorker` command can be used to take the target workers off-line from the cluster, 
so Alluxio clients and proxy instances stop using those workers, and therefore they can be killed or restarted gracefully.
Note that this command will NOT kill worker processes. This command will NOT remove the cache on the workers.
This command can be typically used to scale down the cluster without interrupting user I/O workflow.

```shell
$ ./bin/alluxio fsadmin decommissionWorker --addresses data-worker-0,data-worker-1 [--wait 5m] [--disable]
```
The arguments are explained as follows:

`--addresses/-a` is a required argument, followed by a list of comma-separated worker addresses. Each worker address is `<host>:<web port>`.
Unlike many other commands which specify the RPC port, we use the web port here because the command will monitor the worker's workload
exposed at the web port. If the port is not specified, the value in `alluxio.worker.web.port` will be used. Note that `alluxio.worker.web.port`
will be resolved from the node where this command is run.

`--wait/-w` is an optional argument. This argument defines how long this command waits for the workers to become idle.
This command returns either when all workers are idle, or when this wait time is up. The default value is `5m`.

`--disable/-d` is an optional argument. If this is specified, the decommissioned workers will not be able to
register to the master. In other words, a disabled worker cannot join the cluster and will not be chosen for I/O requests.
This is useful when the admin wants to remove the workers from the cluster. When those disabled workers register,
the master will reject them but will not kill the worker processes. This is often used in pair with the `enableWorker` command.

The command will perform the following actions:

1. For each worker in the batch, send a decommission command to the primary Alluxio master so 
   the master marks those workers as decommissioned and will not serve operations. The ongoing I/O on those workers
   will NOT be interrupted.
2. It takes a small interval for all other Alluxio components (like clients and Proxy instances)
   to know those workers should not be used, so this command waits for the interval time defined by `alluxio.user.worker.list.refresh.interval`
   on the node where this command is executed. Before a client/proxy realizes the workers are decommissioned,
   they may submit more I/O requests to those workers, and those requests should execute normally.
3. Get the active worker list from the master after waiting, and verify the target workers are not active anymore.
4. Wait for the workers to become idle. This command will constantly check the idleness status on each worker.
   A worker is considered "idle" if it is not actively serving RPCs(including control RPCs and data I/O).
5. Either all workers have become idle, or the specified timeout expires, this command will return.

A worker is considered "idle" if it is not actively serving RPCs(including control RPCs and data I/O).
The `decommissionWorker` command stops all clients from using those workers, and waits for all ongoing requests to complete.
The command waits for those all clients to stop using those workers, and waits for those workers to become idle,
so when this command returns success(exit code 0), it is safe for the admin to kill/restart those worker processes.

The primary Alluxio master maintains a list of available Alluxio workers, and all Alluxio components(including Proxy, Job Master/Job Worker and Client)
will regularly refresh this list with the primary master. The refresh interval is defined by `alluxio.user.worker.list.refresh.interval`.
So after those workers are taken off the available list, after another refresh interval has elapsed,
and after all ongoing requests have been served, those workers should not receive any more requests.
Therefore, no matter when the admin restarts/kills those worker processes, that should not fail any requests.
However, there are a few exceptions. See the next section for more details.

**Limitations**
 
This has some limitations. In some cases, the `decommissionWorker` command may return code 0 (success)
but when the worker process is killed, some user I/O requests may fail.

When the `decommissionWorker` command waits for a worker to become idle, it only respects ongoing I/O requests on the worker.
If the Alluxio client is reading/writing the worker with short circuit, the client directly reads/writes cache files
in worker storage and maintains a gRPC stream with the worker simply for locking that block. An open(and idle) stream
will NOT be respected by the `decommissionWorker` command so it may consider the worker is idle.
However, when the admin kills the worker and deletes the cache storage, the client request will fail
either because the cache blocks are gone or because the gRPC stream is broken. So if you are using short circuit,
wait a few more minutes before killing the worker process and deleting cached blocks. The clients will
know the workers are decommissioned (should not be read/written) and stop using those workers by short circuit.

The `decommissionWorker` command does NOT consider cache blocks on the target workers. That means if
decommissioning some workers will bring ALL replicas of certain blocks offline, and those blocks only exist in cache,
then clients CAN NOT read those blocks. The user has to restart the workload after those workers are restarted.

**Exit Codes**

This command is idempotent and can be retried, but the admin is advised to manually check if there's an error. 

The return codes have different meanings:

1. `0(OK)`: All workers are successfully decommissioned and now idle. Safe to kill or restart this batch of workers now.
2. `1(DECOMMISSION_FAILED)`: Failed to decommission all workers. The admin should double check the worker addresses and the primary master status.
3. `2(LOST_MASTER_CONNECTION)`: Lost connection to the primary master while this command is running. This suggests the configured master address is wrong or the primary master failed over.
4. `3(WORKERS_NOT_IDLE)`: Some workers were still not idle after the wait. Either the wait time is too short or those workers failed to mark decommissioned. The admin should manually intervene and check those workers.
5. `10(LOST_SOME_WORKERS)`: Workers are decommissioned but some or all workers lost contact while this command is running. If a worker is not serving then it is safe to kill or restart. But the admin is advised to double check the status of those workers.

### enableWorker

The `enableWorker` command is the reverse operation of `decommissionWorker -d`. The `decommissionWorker -d` command will
decommission workers and not disable them from re-registering to the cluster. The `enableWorker` command will
enable those workers to re-register to the cluster and serve again.

```shell
# Decommission 2 workers and disable them from joining the cluster again even if they restart
$ ./bin/alluxio fsadmin decommissionWorker --addresses data-worker-0,data-worker-1 --disable

# data-worker-0 and data-worker-1 will not be able to register to the master after they restart
$ ./bin/alluxio-start.sh workers # This should show an error status

# The admin regrets and wants to bring one of them back to the cluster
$ ./bin/alluxio fsadmin enableWorker --addresses data-worker-1

# If data-worker-1 is restarted, it is able to register to the cluster and serve normally again
$ ./bin/alluxio-start.sh workers
```
