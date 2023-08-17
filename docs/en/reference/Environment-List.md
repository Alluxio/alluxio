---
layout: global
title: List of Environment Variables
---

Alluxio supports defining a few frequently used configuration settings through environment
variables.

<table class="table table-striped">
<tr><th>Environment Variable</th><th>Description</th></tr>
{% for env_var in site.data.table.en.env_vars %}
  <tr>
    <td markdown="span">`{{env_var.name}}`</td>
    <td markdown="span">{{env_var.description}}</td>
  </tr>
{% endfor %}
</table>

The following example will set up:
- an Alluxio master at `localhost`
- the root mount point as an HDFS cluster with a namenode also running at `localhost`
- defines the maximum heap space of the VM to be 30g
- enable Java remote debugging at port 7001

```shell
$ export ALLUXIO_MASTER_HOSTNAME="localhost"
$ export ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="hdfs://localhost:9000"
$ export ALLUXIO_MASTER_JAVA_OPTS="-Xmx30g"
$ export ALLUXIO_MASTER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=7001"
```

Users can either set these variables through the shell or in `conf/alluxio-env.sh`.
If this file does not exist yet, it can be copied from the template file under `${ALLUXIO_HOME}/conf`:

```shell
$ cp conf/alluxio-env.sh.template conf/alluxio-env.sh
```