---
layout: global
title: 管理员命令行接口
group: Features
priority: 0
---
 
* 目录
{:toc}
 
Alluxio的管理员命令行接口为管理员提供了管理Alluxio文件系统的操作。
您可以调用以下命令行来获取所有子命令：
 
```bash
$ ./bin/alluxio fsadmin
Usage: alluxio fsadmin [generic options]
       [report]
       [ufs --mode <noAccess/readOnly/readWrite> <ufsPath>]
       ...
```
 
以UFS URI作为参数的`fsadmin ufs`子命令，参数应该是像`hdfs://<name-service>/`这样的根UFS URI，而不是`hdfs://<name-service>/<folder>`。
 
##操作列表
 
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
 
##示例用例

### report

`report`命令提供了Alluxio运行中的集群信息。

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

使用 `-h` 选项来获得更多信息。
 
### ufs
 
`ufs`命令提供了选项来更新挂载的底层存储的属性。`mode`选项可用于将底层存储设置为维护模式。目前某些操作可能会受到限制。
 
例如，一个底层存储可以设为`readOnly`模式来禁止写入操作。 Alluxio将不会对底层存储尝试任何写入操作。
 
```bash
$ ./bin/alluxio fsadmin ufs --mode readOnly hdfs://ns
```
