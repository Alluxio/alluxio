---
layout: global
title: 世系关系（Lineage）客户端API（内测版）
nickname: 世系关系（Lineage）API
group: Features
priority: 2
---

* 内容列表
{:toc}

Alluxio可以利用*世系关系（Lineage）*达到很高的读写吞吐量，又不影响容错性，丢失的输出数据会通过重新运行生成该数据的作业来进行恢复。

依赖世系关系，应用程序将输出写到内存中，并且Alluxio会周期性地将输出结果以异步的方式备份（checkpoint）到底层文件系统。如果失败，Alluxio会启动*重新计算作业*来恢复丢失的文件。世系关系的前提假设作业是确定的，所以重新计算的输出和之前的结果相同。如果该提前假设不能满足，那么由上层的应用来处理分歧数据。

# 使世系关系生效

默认情况下，世系关系是不生效的。你可以通过在配置文件中将`alluxio.user.lineage.enabled`属性设置为`true`来使其生效。

# 世系关系API

Alluxio提供了Java版本的API来操作和访问世系关系信息。

### 获取世系关系客户端实例

要想用Java代码获取一个Alluxio世系关系客户端实例，可以使用：

{% include Lineage-API/get-lineage-client.md %}

### 创建世系关系记录

世系关系可以通过调用`AlluxioLineage#createLineage(List<AlluxioURI>, List<AlluxioURI>, Job)`来创建。一条记录包含（1）一组输入文件的URI，（2）一组输出文件的URI，和（3）一个*作业*。作业是是对一个运行在Alluxio上的项目的描述，其可以利用给定的输入文件重新计算输出文件。*注意：在当前的测试版中，仅支持内置的`CommandLineJob`，其只需要接收运行在终端上的命令行字符串。用户需要提供必要的配置和运行环境来保证命令可以在Alluxio Client和Master上运行（在重新计算期间）。*

例如：
{% include Lineage-API/config-lineage.md %}

`createLineage`函数返回的是新建的世系关系记录id。在创建一条世系关系记录之前，要确保所有的输入文件要么已被持久化，要么可以通过另一条世系关系记录来指定。

### 指定操作选项

对于所有的`AlluxioLineage`操作，可以指定额外的`options`域,其允许用户指定非默认的操作设置。

### 删除一条世系关系

世系关系记录可以通过调用`AlluxioLineage#deleteLineage`来删除。删除函数要接受一个记录id。

{% include Lineage-API/delete-lineage.md %}

默认情况下，如果其他世系关系依赖该世系关系记录的输出文件，那么这条记录不允许被删除。可以选择设置cascade删除标志来删除所有下游的世系关系。例如：

{% include Lineage-API/delete-cascade.md %}

# 配置世系关系相关参数

下表列出了与Alluxio世系关系相关的配置参数。

<table class="table table-striped">
<tr><th>参数</th><th>默认值</th><th>介绍</th></tr>
</tr>
{% for record in site.data.table.LineageParameter %}
<tr>
  <td>{{record.parameter}}</td>
  <td>{{record.defaultvalue}}</td>
  <td>{{site.data.table.cn.LineageParameter[record.parameter]}}</td>
</tr>
{% endfor %}
</table>
