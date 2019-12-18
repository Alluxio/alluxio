---
layout: global
title: List of Configuration Properties
group: Reference
priority: 0
---

* Table of Contents
{:toc}

All Alluxio configuration settings fall into one of the six categories:
[Common](#common-configuration) (shared by Master and Worker),
[Master specific](#master-configuration), [Worker specific](#worker-configuration),
[User specific](#user-configuration), [Cluster specific](#resource-manager-configuration) (used for running
Alluxio with cluster managers like Mesos and YARN), and
[Security specific](#security-configuration) (shared by Master, Worker, and User).

## Common Configuration

The common configuration contains constants shared by different components.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.table.common-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.common-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Master Configuration

The master configuration specifies information regarding the master node, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.table.master-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.master-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Worker Configuration

The worker configuration specifies information regarding the worker nodes, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.table.worker-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.worker-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## User Configuration

The user configuration specifies values regarding file system access.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.table.user-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.user-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Resource Manager Configuration

When running Alluxio with resource managers like Mesos and YARN, Alluxio has additional configuration options.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.table.cluster-management-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.cluster-management-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Security Configuration

The security configuration specifies information regarding the security features, such as
authentication and file permission. Settings for authentication take effect for master, worker, and
user. Settings for file permission only take effect for master. See
[Security]({{ '/en/operation/Security.html' | relativize_url }})
for more information about security features.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.table.security-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.security-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>
