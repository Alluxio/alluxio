---
布局: 全局
标题: 环境变量列表
---

Alluxio支持通过环境变量定义一些常用的配置。

<table class="table table-striped">
<tr><th>Environment Variable</th><th>Description</th></tr>
{% for env_var in site.data.table.en.env_vars %}
  <tr>
    <td markdown="span">`{{env_var.name}}`</td>
    <td markdown="span">{{env_var.description}}</td>
  </tr>
{% endfor %}
</table>

在以下示例中，将进行如下设置：
- 一个运行在localhost的Alluxio Master节点
- 将根挂载点设置为HDFS集群，该集群namenode同样运行在localhost上
- 设置虚拟机的最大堆(heap)空间为30g
- 在7001端口启用Java远程调试

```shell
$ export ALLUXIO_MASTER_HOSTNAME="localhost"
$ export ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="hdfs://localhost:9000"
$ export ALLUXIO_MASTER_JAVA_OPTS="-Xmx30g"
$ export ALLUXIO_MASTER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=7001"
```

用户可以通过shell或在`conf/alluxio-env.sh`中设置这些变量。如果该文件尚不存在，可以从`${ALLUXIO_HOME}/conf`下的模板文件复制：

```shell
$ cp conf/alluxio-env.sh.template conf/alluxio-env.sh
```
