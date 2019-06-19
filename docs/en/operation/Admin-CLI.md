---
layout: global
title: Admin Command Line Interface
nickname: Admin CLI
group: Operations
priority: 0
---

* Table of Contents
{:toc}

Alluxio's admin command line interface provides admins with operations to manage the Alluxio filesystem.
You can invoke the following command line utility to get all the subcommands:

```bash
./bin/alluxio fsadmin
Usage: alluxio fsadmin [generic options]
       [backup]
       [checkpoint]
       [doctor [category]]
       [report]
       [ufs --mode <noAccess/readOnly/readWrite> <ufsPath>]
       ...
```

## Operations

### backup

The `backup` command creates a backup of Alluxio metadata. By default, this is the '$ALLUXIO_HOME/alluxio_backups'
on the primary Alluxio Master.

Back up to the default backup folder (this default can be configured by setting `alluxio.master.backup.directory`)
```
./bin/alluxio fsadmin backup
Successfully backed up journal to /opt/alluxio/alluxio_backups/alluxio-backup-2019-06-13-1560446387260.gz
```
Back up to a specific directory in the under storage.
```
./bin/alluxio fsadmin backup /alluxio/special_backups
Successfully backed up journal to hdfs://mycluster/alluxio/special_backups/alluxio-backup-2018-5-29-1527644810.gz
```
Back up to a specific directory on the leading master's local filesystem.
```
./bin/alluxio fsadmin backup /opt/alluxio/backups/ --local
Successfully backed up journal to /opt/alluxio/backups/alluxio-backup-2018-5-29-1527644810.gz on master Master2
```

### checkpoint 

The `checkpoint` command creates a checkpoint in the primary master journal system.

This command is mainly used for debugging and to avoid master journal logs from growing unbounded.
Checkpointing requires a pause in master metadata changes, so use this command sparingly to avoid 
interfering with other users of the system.

```
./bin/alluxio fsadmin checkpoint
```

### doctor

The `doctor` command gives recommendations and warnings. It can diagnose inconsistent configurations
across different Alluxio nodes as well as alert the operator when worker storage volumes are missing.

```bash
# shows server-side configuration errors and warnings
./bin/alluxio fsadmin doctor configuration
```

### getBlockInfo

The `getBlockInfo` command provides the block information and file path of a block id.
It is primarily intended to assist power users in debugging their system.

```bash
./bin/alluxio fsadmin getBlockInfo <block_id>
BlockInfo{id=16793993216, length=6, locations=[BlockLocation{workerId=8265394007253444396, address=WorkerNetAddress{host=local-mbp, rpcPort=29999, dataPort=29999, webPort=30000, domainSocketPath=, tieredIdentity=TieredIdentity(node=local-mbp, rack=null)}, tierAlias=MEM, mediumType=MEM}]}
This block belongs to file {id=16810770431, path=/test2}
```

### report

The `report` command provides Alluxio running cluster information.

If no argument is passed in, `report` will report the leading master, worker number, and capacity information.

```bash
./bin/alluxio fsadmin report
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
* `-workers <worker_names>` Specified workers, host names or ip addresses separated by `,`.

```bash
# Capacity information of all workers
./bin/alluxio fsadmin report capacity
# Capacity information of live workers
./bin/alluxio fsadmin report capacity -live
# Capacity information of specified workers
./bin/alluxio fsadmin report capacity -workers AlluxioWorker1,127.0.0.1
```

`report metrics` will report the metrics information of Alluxio cluster.

```bash
./bin/alluxio fsadmin report metrics
```

`report ufs` will report all the mounted under storage system information of Alluxio cluster.

```bash
./bin/alluxio fsadmin report ufs
Alluxio under storage system information:
hdfs://localhost:9000/ on / (hdfs, capacity=-1B, used=-1B, not read-only, not shared, properties={})
```

### ufs

The `ufs` command provides options to update attributes of a mounted under storage. The option `mode` can be used
to put an under storage in maintenance mode. Certain operations can be restricted at this moment.

For example, an under storage can enter `readOnly` mode to disallow write operations. Alluxio will not attempt any
write operations on the under storage.

```bash
./bin/alluxio fsadmin ufs --mode readOnly hdfs://ns
```

The `fsadmin ufs` subcommand takes a UFS URI as an argument. The argument should be a root
UFS URI like `hdfs://<name-service>/`, and not `hdfs://<name-service>/<folder>`.

### pathConf

The `pathConf` command manages [path defaults]({{ '/en/basic/Configuration-Settings.html' | relativize_url }}#path-defaults).

#### list

`pathConf list` lists paths configured with path defaults.

```bash
./bin/alluxio fsadmin pathConf list

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
```bash
./bin/alluxio fsadmin pathConf show /a/b

property2=value2
```

With `--all`, since `/a` is a prefix of `/a/b`, properties for both `/a` and `/a/b` are shown:
```bash
./bin/alluxio fsadmin pathConf show --all /a/b

property1=value1
property2=value2
```

#### add

`pathConf add` adds or updates path defaults, only properties with scope equal to or broader than the
client scope can be set as path defaults.

```bash
./bin/alluxio fsadmin pathConf add --property property1=value1 --property property2=value2 /tmp
```

The above command adds two properties as path defaults for paths with prefix `/tmp`.

```bash
./bin/alluxio fsadmin pathConf add --property property1=value2 /tmp
```
The above command updates the value of `property1` from `value1` to `value2` for path defaults of `/tmp`.

#### remove

`pathConf remove` removes properties from path defaults for a path.

```bash
./bin/alluxio fsadmin pathConf remove --keys property1,property2 /tmp
```

The above command removes properties with key `property1` and `property2` from path
defaults for paths with prefix `/tmp`.
