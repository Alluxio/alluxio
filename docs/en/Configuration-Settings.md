---
layout: global
title: Configuration Settings
group: Features
priority: 1
---

* Table of Contents
{:toc}

There are two types of configuration parameters for Alluxio:

1. [Configuration properties](#configuration-properties) are used to configure the runtime settings
of Alluxio system, and
2. [System environment properties](#system-environment-properties) control the Java VM options to
run Alluxio as well as some basic very setting.

# Configuration properties

On startup, Alluxio loads the default (and optionally a site specific) configuration properties file
to set the configuration properties.

1. The default values of configuration properties of Alluxio are defined in
`alluxio-default.properties`. This file can be found in Alluxio source tree and is typically
distributed with Alluxio binaries. We do not recommend beginner users to edit this file directly.

2. Each site deployment and application client can also override the default property values via
`alluxio-site.properties` file. Note that, this file **must be in the classpath** of the Java VM in
which Alluxio is running. The easiest way is to put the site properties file in directory
`$ALLUXIO_HOME/conf`.

All Alluxio configuration properties fall into one of the six categories:
[Common](#common-configuration) (shared by Master and Worker),
[Master specific](#master-configuration), [Worker specific](#worker-configuration),
[User specific](#user-configuration), [Cluster specific](#cluster-management) (used for running
Alluxio with cluster managers like Mesos and YARN), and
[Security specific](#security-configuration) (shared by Master, Worker, and User).

## Common Configuration

The common configuration contains constants shared by different components.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.common-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.common-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Master Configuration

The master configuration specifies information regarding the master node, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.master-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.master-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Worker Configuration

The worker configuration specifies information regarding the worker nodes, such as the address and
the port number.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.worker-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.worker-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>


## User Configuration

The user configuration specifies values regarding file system access.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.user-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.user-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Cluster Management

When running Alluxio with cluster managers like Mesos and YARN, Alluxio has additional
configuration options.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.cluster-management %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.cluster-management.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Security Configuration

The security configuration specifies information regarding the security features,
such as authentication and file permission. Properties for authentication take effect for master,
worker, and user. Properties for file permission only take effect for master.
See [Security](Security.html) for more information about security features.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.security-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.security-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Configure multihomed networks

Alluxio configuration provides a way to take advantage of multi-homed networks. If you have more
than one NICs and you want your Alluxio master to listen on all NICs, you can specify
`alluxio.master.bind.host` to be `0.0.0.0`. As a result, Alluxio clients can reach the master node
from connecting to any of its NIC. This is also the same case for other properties suffixed with
`bind.host`.

# System environment properties

To run Alluxio, it also requires some system environment variables being set which by default are
configured in file `conf/alluxio-env.sh`. If this file does not exist yet, you can create one from a
template we provided in the source code using:

{% include Common-Commands/copy-alluxio-env.md %}

There are a few frequently used Alluxio configuration properties that can be set via environment
variables. One can either set these variables through shell or modify their default values specified
in `conf/alluxio-env.sh`.

* `$ALLUXIO_MASTER_ADDRESS`: Alluxio master address, default to localhost.
* `$ALLUXIO_UNDERFS_ADDRESS`: under storage system address, default to
`${ALLUXIO_HOME}/underFSStorage` which is a local file system.
* `$ALLUXIO_JAVA_OPTS`: Java VM options for both Master and Worker.
* `$ALLUXIO_MASTER_JAVA_OPTS`: additional Java VM options for Master configuration.
* `$ALLUXIO_WORKER_JAVA_OPTS`: additional Java VM options for Worker configuration. Note that, by
default `ALLUXIO_JAVA_OPTS` is included in both `ALLUXIO_MASTER_JAVA_OPTS` and
`ALLUXIO_WORKER_JAVA_OPTS`.

For example, if you would like to connect Alluxio to HDFS running at localhost and enable Java
remote debugging at port 7001, you can do so using:

{% include Configuration-Settings/more-conf.md %}
