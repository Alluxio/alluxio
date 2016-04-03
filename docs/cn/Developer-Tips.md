---
layout: global
title: 开发者指南
nickname: 开发者指南
group: Resources
---

* Table of Contents
{:toc}

该页面是一些面向Alluxio开发者的技巧和手册。

### 更改Thrift RPC的定义

Alluxio使用Thrift来完成客户端与服务端的RPC通信。`.thrift`文件定义在`common/src/thrift/`目录下，其一方面用于自动生成客户端调用RPC的Java代码，另一方面用于实现服务端的RPC。要想更改一个Thrift定义，你首先必须要[安装Thrift的编译器](https://thrift.apache.org/docs/install/)。如果你的机器上有brew，你可以通过运行下面的命令来完成。

{% include Developer-Tips/install-thrift.md %}

然后重新生成Java代码，运行

{% include Developer-Tips/thriftGen.md %}

### 更改协议缓冲区消息

Alluxio使用协议缓冲区来读写日志消息。`.proto`文件被定义在`servers/src/proto/journal/`目录下，其用于为协议缓冲区消息自动生成Java定义。要需要修改这些消息，首先要[读取更新的消息类型](https://developers.google.com/protocol-buffers/docs/proto#updating)从而保证你的修改不会破坏向后兼容性。然后就是[安装protoc](https://github.com/google/protobuf#protocol-buffers---googles-data-interchange-format)。如果你的机器上有brew，你可以通过运行下面的命令来完成。

{% include Developer-Tips/install-protobuf.md %}

然后重新生成Java代码，运行

{% include Developer-Tips/protoGen.md %}

### bin/alluxio目录下的命令列表

开发者所用到的大多数命令都在`bin/alluxio`目录下。下面的表格有对每条命令及其参数的说明。

<table class="table table-striped">
<tr><th>命令</th><th>参数</th><th>介绍</th></tr>
{% for dscp in site.data.table.Developer-Tips %}
<tr>
  <td>{{dscp.command}}</td>
  <td>{{dscp.args}}</td>
  <td>{{site.data.table.cn.Developer-Tips.[dscp.command]}}</td>
</tr>
{% endfor %}
</table>

此外，这些命令的执行有不同的先决条件。`format`，`formatWorker`，`journalCrashTest`，`readJournal`，`version`和`validateConf`命令的先决条件是你已经构建了Alluxio（见[构建Alluxio主分支](Building-Alluxio-Master-Branch.html)其介绍了如何手动构建Alluxio)。而`fs`，`loadufs`，`runTest`和`runTests`命令的先决条件是你已经运行了Alluxio系统。
