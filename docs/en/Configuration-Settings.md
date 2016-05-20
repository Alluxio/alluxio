---
layout: global
title: Configuration Settings
group: Features
priority: 1
---

* Table of Contents
{:toc}

This page explains the configuration system of Alluxio and also provides recommendation on how customize the configuration
for Alluxio users in different contexts.

# Configuration in Alluxio

Alluxio runtime respects two sources of configuration settings:

1. [Environment variables](#Environment-variables). This is a convenient way for beginner users or shell scripts to set
a few basic properties when running Alluxio.
2. [Configuration properties](#configuration-properties). This provides a way to customize any
[supported Alluxio configure properties](#appendix).

## Environment variables

There are a few basic but very frequently used Alluxio configuration properties that can be set via the
following environment variables:

<table class="table table-striped">
<tr><th>Environment Variable</th><th>Meaning</th></tr>
<tr><td>`$ALLUXIO_MASTER_HOSTNAME`</td><td>hostname of Alluxio master, default to localhost.</td></tr>
<tr><td>`$ALLUXIO_MASTER_ADDRESS`</td><td>Same as `ALLUXIO_MASTER_HOSTNAME`, deprecated since version 1.1 and
will be remove in version 2.0.</td></tr>
<tr><td>`$ALLUXIO_UNDERFS_ADDRESS`</td><td>under storage system address, default to
`${ALLUXIO_HOME}/underFSStorage` which is a local file system.</td></tr>
<tr><td>`$ALLUXIO_RAM_FOLDER`</td><td>The directory where a worker stores in-memory data, default to /mnt/ramdisk.</td></tr>
<tr><td>`$ALLUXIO_JAVA_OPTS`</td><td>Java VM options for both Master and Worker.</td></tr>
<tr><td>`$ALLUXIO_MASTER_JAVA_OPTS`</td><td>additional Java VM options for Master configuration.</td></tr>
<tr><td>`$ALLUXIO_WORKER_JAVA_OPTS`</td><td>additional Java VM options for Worker configuration. Note that, by
default `ALLUXIO_JAVA_OPTS` is included in both `ALLUXIO_MASTER_JAVA_OPTS` and
`ALLUXIO_WORKER_JAVA_OPTS`.</td></tr>
</table>

For example, if you would like to setup a Alluxio master at `localhost` that talks to a HDFS also running at `localhost`, and enable Java
remote debugging at port 7001, you can do so using:

{% include Configuration-Settings/more-conf.md %}

One can either set these variables through shell or set them in `conf/alluxio-env.sh`. If this file does not exist yet,
you can create one from a template we provided in the source code using:

{% include Common-Commands/copy-alluxio-env.md %}

Alternatively, Alluxio can help users bootstrap the `conf/alluxio-env.sh` file by running

{% include Common-Commands/bootstrap-conf.md %}


## Configuration properties

In addition to these environment variables that provide very basic setting, Alluxio also provides a
more general approach for users to customize all supported configuration properties via property files.
On startup, Alluxio optionally loads configuration properties file to set the configuration properties:

1. For each Alluxio site deployment, both servers or application clients can override the default property values via
`alluxio-site.properties` file.

2. Alluxio master and workers will load `alluxio-server.properties` file; Alluxio clients (e.g., Jobs reading from or writing to
Alluxio) will load `alluxio-client.properties` file.

Note that, these property files are searched in `${HOME}/.alluxio/`, `/etc/conf/` and the classpath of the Java VM in
which Alluxio is running in order. The easiest way is to copy the site properties template in directory
`$ALLUXIO_HOME/conf` and edit it to fit your configuration tuning needs.

```bash
$ cp $ALLUXIO_HOME/conf/alluxio-site.properties.template ~/.alluxio/alluxio-site.properties
```


# Appendix
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
