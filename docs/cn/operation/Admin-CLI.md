---
layout: global
title: 管理员命令行接口
group: Operations
priority: 2
---

* 目录
{:toc}

Alluxio的管理员命令行接口为管理员提供了管理Alluxio文件系统的操作。
您可以调用以下命令行来获取所有子命令：

```console
$ ./bin/alluxio fsadmin
Usage: alluxio fsadmin [generic options]
       [report]
       [ufs --mode <noAccess/readOnly/readWrite> <ufsPath>]
       ...
```

以UFS URI作为参数的`fsadmin ufs`子命令，参数应该是像`hdfs://<name-service>/`这样的根UFS URI，而不是`hdfs://<name-service>/<folder>`。

## 操作列表

<table class="table table-striped">
  <tr><th>操作</th><th>语法</th><th>描述</th></tr>
  {% for item in site.data.table.fsadmin-command %}
    <tr>
      <td>{{ item.operation }}</td>
      <td>{{ item.syntax }}</td>
      <td>{{ site.data.table.en.fsadmin-command[item.operation] }}</td>
    </tr>
  {% endfor %}
</table>

## 示例用例

### backup(备份)

`backup`命令创建Alluxio元数据的备份

备份到默认备份文件夹（由`alluxio.master.backup.directory`）配置
```
./bin/alluxio fsadmin backup
Successfully backed up journal to hdfs://mycluster/opt/alluxio/backups/alluxio-backup-2018-5-29-1527644810.gz
```
备份到下存储中的特定目录。
```
./bin/alluxio fsadmin backup /alluxio/special_backups
Successfully backed up journal to hdfs://mycluster/opt/alluxio/backups/alluxio-backup-2018-5-29-1527644810.gz
```
备份到主主机的本地文件系统的特定目录。
```
./bin/alluxio fsadmin backup /opt/alluxio/backups/ --local
Successfully backed up journal to file:///opt/alluxio/backups/alluxio-backup-2018-5-29-1527644810.gz on master Master2
```
### doctor

`doctor`命令显示Alluxio错误和警告。

```console
# shows server-side configuration errors and warnings
$ ./bin/alluxio fsadmin doctor configuration
```

### report

`report`命令提供了Alluxio运行中的集群信息。

```console
# Report cluster summary
$ ./bin/alluxio fsadmin report

# Report worker capacity information
$ ./bin/alluxio fsadmin report capacity

# Report runtime configuration information 
$ ./bin/alluxio fsadmin report configuration

# Report metrics information
$ ./bin/alluxio fsadmin report metrics

# Report under file system information
$ ./bin/alluxio fsadmin report ufs
```

使用 `-h` 选项来获得更多信息。
 
### ufs
 
`ufs`命令提供了选项来更新挂载的底层存储的属性。`mode`选项可用于将底层存储设置为维护模式。目前某些操作可能会受到限制。
 
例如，一个底层存储可以设为`readOnly`模式来禁止写入操作。 Alluxio将不会对底层存储尝试任何写入操作。
 
```console
$ ./bin/alluxio fsadmin ufs --mode readOnly hdfs://ns
```

`fsadmin ufs`命令接受一个UFS URI作为参数。该参数需要是一个
UFS URI的根，类似`hdfs://<name-service>/`，而非`hdfs://<name-service>/<folder>`。
