---
layout: global
title: Configuration Settings
group: Features
priority: 1
---

* Table of Contents
{:toc}

There are two types of configuration parameters for Tachyon:

1. [Configuration properties](#configuration-properties) are used to configure the runtime settings
of Tachyon system, and
2. [System environment properties](#system-environment-properties) control the Java VM options to
run Tachyon as well as some basic very setting.

# Configuration properties

On startup, Tachyon loads the default (and optionally a site specific) configuration properties file
to set the configuration properties.

1. The default values of configuration properties of Tachyon are defined in
`tachyon-default.properties`. This file can be found in Tachyon source tree and is typically
distributed with Tachyon binaries. We do not recommend beginner users to edit this file directly.

2. Each site deployment and application client can also override the default property values via
`tachyon-site.properties` file. Note that, this file **must be in the classpath** of the Java VM in
which Tachyon is running. The easiest way is to put the site properties file in directory
`$TACHYON_HOME/conf`.

All Tachyon configuration properties fall into one of the six categories:
[Common](#common-configuration) (shared by Master and Worker),
[Master specific](#master-configuration), [Worker specific](#worker-configuration),
[User specific](#user-configuration), [Cluster specific](#cluster-management) (used for running
Tachyon with cluster managers like Mesos and YARN), and
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

When running Tachyon with cluster managers like Mesos and YARN, Tachyon has additional
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
<tr>
  <td>tachyon.security.authentication.type</td>
  <td>NOSASL</td>
  <td>The authentication mode. Currently three modes are supported: NOSASL, SIMPLE,
  CUSTOM. The default value NOSASL indicates that authentication is not enabled.</td>
</tr>
<tr>
  <td>tachyon.security.authentication.socket.timeout.ms</td>
  <td>60000</td>
  <td>The maximum amount of time (in milliseconds) for a user to create a Thrift socket which
  will connect to the master.</td>
</tr>
<tr>
  <td>tachyon.security.authentication.custom.provider.class</td>
  <td></td>
  <td>The class to provide customized authentication implementation, when
  tachyon.security.authentication.type is set to CUSTOM. It must implement the
  interface 'tachyon.security.authentication.AuthenticationProvider'.</td>
</tr>
<tr>
  <td>tachyon.security.login.username</td>
  <td></td>
  <td>When tachyon.security.authentication.type is set to SIMPLE or CUSTOM, user application uses
  this property to indicate the user requesting Tachyon service. If it is not set explicitly,
  the OS login user will be used.</td>
</tr>
<tr>
  <td>tachyon.security.authorization.permission.enabled</td>
  <td>false</td>
  <td>Whether to enable access control based on file permission.</td>
</tr>
<tr>
  <td>tachyon.security.authorization.permission.umask</td>
  <td>022</td>
  <td>The umask of creating file and directory. The initial creation permission is 777, and
  the difference between directory and file is 111. So for default umask value 022,
  the created directory has permission 755 and file has permission 644.</td>
</tr>
<tr>
  <td>tachyon.security.authorization.permission.supergroup</td>
  <td>supergroup</td>
  <td>The super group of Tachyon file system. All users in this group have super permission.</td>
</tr>
<tr>
  <td>tachyon.security.group.mapping.class</td>
  <td>tachyon.security.group.provider.&#8203;ShellBasedUnixGroupsMapping</td>
  <td>The class to provide user-to-groups mapping service. Master could get the various group
  memberships of a given user.  It must implement the interface
  'tachyon.security.group.GroupMappingService'. The default implementation execute the 'groups'
  shell command to fetch the group memberships of a given user.</td>
</tr>
</table>

## Configure multihomed networks

Tachyon configuration provides a way to take advantage of multi-homed networks. If you have more
than one NICs and you want your Tachyon master to listen on all NICs, you can specify
`tachyon.master.bind.host` to be `0.0.0.0`. As a result, Tachyon clients can reach the master node
from connecting to any of its NIC. This is also the same case for other properties suffixed with
`bind.host`.

# System environment properties

To run Tachyon, it also requires some system environment variables being set which by default are
configured in file `conf/tachyon-env.sh`. If this file does not exist yet, you can create one from a
template we provided in the source code using:

{% include Configuration-Settings/copy-tachyon-env.md %}

There are a few frequently used Tachyon configuration properties that can be set via environment
variables. One can either set these variables through shell or modify their default values specified
in `conf/tachyon-env.sh`.

* `$TACHYON_MASTER_ADDRESS`: Tachyon master address, default to localhost.
* `$TACHYON_UNDERFS_ADDRESS`: under storage system address, default to
`${TACHYON_HOME}/underFSStorage` which is a local file system.
* `$TACHYON_JAVA_OPTS`: Java VM options for both Master and Worker.
* `$TACHYON_MASTER_JAVA_OPTS`: additional Java VM options for Master configuration.
* `$TACHYON_WORKER_JAVA_OPTS`: additional Java VM options for Worker configuration. Note that, by
default `TACHYON_JAVA_OPTS` is included in both `TACHYON_MASTER_JAVA_OPTS` and
`TACHYON_WORKER_JAVA_OPTS`.

For example, if you would like to connect Tachyon to HDFS running at localhost and enable Java
remote debugging at port 7001, you can do so using:

{% include Configuration-Settings/more-conf.md %}
