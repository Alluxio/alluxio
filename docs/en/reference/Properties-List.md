---
layout: global
title: List of Configuration Properties
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

All Alluxio configuration settings fall into one of the five categories:
[Common](#common-configuration) (shared by Master and Worker),
[Master specific](#master-configuration), [Worker specific](#worker-configuration),
[User specific](#user-configuration), and
[Security specific](#security-configuration) (shared by Master, Worker, and User).
- properties prefixed with `alluxio.master` affect the Alluxio master processes
- properties prefixed with `alluxio.worker` affect the Alluxio worker processes
- properties prefixed with `alluxio.user` affect Alluxio client operations (e.g. compute applications)

## Common Configuration

The common configuration contains constants shared by different components.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.generated.common-configuration %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.propertyName }}"></a> `{{ item.propertyName }}`</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.generated.en.common-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Master Configuration

The master configuration specifies information regarding the master node, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.generated.master-configuration %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.propertyName }}"></a> `{{ item.propertyName }}`</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.generated.en.master-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Worker Configuration

The worker configuration specifies information regarding the worker nodes, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.generated.worker-configuration %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.propertyName }}"></a> `{{ item.propertyName }}`</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.generated.en.worker-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## User Configuration

The user configuration specifies values regarding file system access.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.generated.user-configuration %}
  <tr>
    <td markdown="span"><a class="anchor" name="{{ item.propertyName }}"></a> `{{ item.propertyName }}`</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.generated.en.user-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Security Configuration

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
