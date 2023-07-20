---
layout: global
title: List of Configuration Properties
---


All Alluxio configuration settings fall into one of the five categories:
[Common](#common-configuration) (shared by Master and Worker),
[Master specific](#master-configuration), [Worker specific](#worker-configuration),
[User specific](#user-configuration), and
[Security specific](#security-configuration) (shared by Master, Worker, and User).

## Common Configuration

The common configuration contains constants shared by different components.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>
{% for item in site.data.generated.common-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
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
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
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
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
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
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
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
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.generated.en.security-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>
