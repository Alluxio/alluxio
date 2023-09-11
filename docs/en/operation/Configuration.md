---
layout: global
title: Configuration Settings
---


An Alluxio cluster can be configured by setting the values of Alluxio
[configuration properties]({{ '/en/reference/Properties-List.html' | relativize_url }}) within
`${ALLUXIO_HOME}/conf/alluxio-site.properties`.

## Configure an Alluxio Cluster

### Cluster Defaults

When different client applications (Alluxio Shell CLI, Spark jobs, MapReduce jobs)
or Alluxio workers connect to an Alluxio master, they will initialize their own Alluxio
configuration properties with the default values supplied by the masters based on the master-side
`${ALLUXIO_HOME}/conf/alluxio-site.properties` files.
As a result, cluster admins can set default client-side settings (e.g., `alluxio.user.*`), or
network transport settings (e.g., `alluxio.security.authentication.type`) in
`${ALLUXIO_HOME}/conf/alluxio-site.properties` on all the masters, which will be distributed and
become cluster-wide default values when clients and workers connect.

For example, the property `alluxio.user.file.writetype.default` defaults to `ASYNC_THROUGH`, which
first writes to Alluxio and then asynchronously writes to the UFS.
In an Alluxio cluster where data persistence is preferred and all jobs need to write to both the UFS
and Alluxio, the administrator can add `alluxio.user.file.writetype.default=CACHE_THROUGH` in each
master's `alluxio-site.properties` file.
After restarting the cluster, all jobs will automatically set `alluxio.user.file.writetype.default`
to `CACHE_THROUGH`.

Clients can ignore or overwrite the cluster-wide default values by following the approaches
described in [Configure Applications](#configure-applications) to overwrite the same properties.

## Configuration Sources

Alluxio properties can be configured from multiple sources.
A property's final value is determined by the following priority list, from highest priority to lowest:

1. [JVM system properties (i.e., `-Dproperty=key`)](http://docs.oracle.com/javase/jndi/tutorial/beyond/env/source.html#SYS)
2. [Environment variables]({{ '/en/reference/Environment-List.html' | relativize_url}})
3. [Property files]({{ '/en/reference/Properties-List.html' | relativize_url }}):
When an Alluxio cluster starts, each server process including master and worker searches for
`alluxio-site.properties` within the following directories in the given order, stopping when a match is found:
`${CLASSPATH}`, `${HOME}/.alluxio/`, `/etc/alluxio/`, and `${ALLUXIO_HOME}/conf`
4. [Path default values](#path-defaults)
5. [Cluster default values](#cluster-defaults):
An Alluxio client may initialize its configuration based on the cluster-wide default configuration served by the masters.

If no user-specified configuration is found for a property, Alluxio will fall back to
its [default property value]({{ '/en/reference/Properties-List.html' | relativize_url }}).

To check the value of a specific configuration property and the source of its value,
users can run the following command:

```shell
$ ./bin/alluxio conf get alluxio.worker.rpc.port
29998
$ ./bin/alluxio conf get --source alluxio.worker.rpc.port
DEFAULT
```

To list all of the configuration properties with sources:

```shell
$ ./bin/alluxio conf get --source
alluxio.conf.dir=/Users/bob/alluxio/conf (SYSTEM_PROPERTY)
alluxio.debug=false (DEFAULT)
...
```

Users can also specify the `--master` option to list all
of the cluster-wide configuration properties served by the masters.
Note that with the `--master` option, `bin/alluxio conf get` will query the
master which requires the master process to be running.
Otherwise, without `--master` option, this command only checks the local configuration.

```shell
$ ./bin/alluxio conf get --master --source
alluxio.conf.dir=/Users/bob/alluxio/conf (SYSTEM_PROPERTY)
alluxio.debug=false (DEFAULT)
...
```

## Java 11 Configuration

Alluxio also supports Java 11.
To run Alluxio on Java 11, configure the `JAVA_HOME` environment variable to point to a Java 11
installation directory.
If you only want to use Java 11 for Alluxio, you can set the `JAVA_HOME` environment variable in
the `alluxio-env.sh` file.
Setting the `JAVA_HOME` in `alluxio-env.sh` will not affect the Java version which may be used
by other applications running in the same environment.

## Server Configuration Checker

The server-side configuration checker helps discover configuration errors and warnings.
Suspected configuration errors are reported through the web UI, `info doctor` CLI, and master logs.

The web UI shows the result of the server configuration check.

![webUi]({{ '/img/screenshot_configuration_checker_webui.png' | relativize_url }})

Users can also run the `info doctor` command to get the same results.

```shell
$ ./bin/alluxio info doctor configuration
```

Configuration warnings can also be found in the master logs.

![masterLogs]({{ '/img/screenshot_configuration_checker_masterlogs.png' | relativize_url }})
