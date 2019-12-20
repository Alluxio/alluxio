---
layout: global
title: 配置项列表
group: Reference
priority: 0
---

* 内容列表
{:toc}

所有Alluxio配置属性都属于以下六类之一：
[共有配置项](#common-configuration)（由Master和Worker共享），
[Master配置项](#master-configuration)，[Worker配置项](#worker-configuration)，
[用户配置项](#user-configuration)，[集群管理配置项](#cluster-management)（用于在诸如Mesos和YARN的集群管理器上运行Alluxio）
以及[安全性配置项](#security-configuration)（由Master，Worker和用户共享）。

## 共有配置项 {#common-configuration}

共有配置项包含了不同组件共享的常量。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>描述</th></tr>
{% for item in site.data.table.common-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.common-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Master配置项 {#master-configuration}

Master配置项指定master节点的信息，例如地址和端口号。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>描述</th></tr>
{% for item in site.data.table.master-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.master-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Worker配置项 {#worker-configuration}

Worker配置项指定worker节点的信息，例如地址和端口号。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>描述</th></tr>
{% for item in site.data.table.worker-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.worker-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## 用户配置项 {#user-configuration}

用户配置项指定了文件系统访问的相关信息。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>描述</th></tr>
{% for item in site.data.table.user-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.user-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## 集群管理配置项 {#cluster-management-configuration}

如果使用诸如Mesos和YARN的集群管理器运行Alluxio，还有额外的配置项。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>描述</th></tr>
{% for item in site.data.table.cluster-management-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.cluster-management-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## 安全性配置项 {#security-configuration}

安全性配置项指定了安全性相关的信息，如安全认证和文件权限。
安全认证相关的配置同时适用于master、worker和用户。
文件权限相关的配置只对master起作用。
更多安全性相关的信息详见[系统安全文档]({{ '/cn/operation/Security.html' | relativize_url }})。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>描述</th></tr>
{% for item in site.data.table.security-configuration %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.security-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>
