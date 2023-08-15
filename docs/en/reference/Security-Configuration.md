---
layout: global
title: Security Configuration
---

An Alluxio cluster can be configured by setting the values of Alluxio
configuration properties within
`${ALLUXIO_HOME}/conf/alluxio-site.properties`. If this file does not exist, it can be copied from the template file under `${ALLUXIO_HOME}/conf`:

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Make sure that this file is distributed to `${ALLUXIO_HOME}/conf` on every Alluxio master
and worker before starting the cluster.
Restarting Alluxio processes is the safest way to ensure any configuration updates are applied.

The security configuration specifies information regarding the security features, such as
authentication and file permission. Settings for authentication take effect for master, worker, and
user. Settings for file permission only take effect for master.
<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.generated.security-configuration %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.propertyName }}"></a> `{{ item.propertyName }}`</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.generated.en.security-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>