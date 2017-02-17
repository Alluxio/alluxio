---
layout: global
title: Running Alluxio on Mesos
nickname: Alluxio on Mesos
group: Deploying Alluxio
priority: 4
---

* Table of Contents
{:toc}

Alluxio can be deployed through Mesos. This allows Mesos to manage the resources used by Alluxio. For the Alluxio
master this is just the cpu and memory needed by the JVM process. For the worker it is the same, but with the addition
of the memory needed by the ramdisk.

## Mesos version

Alluxio is compatible with Mesos 0.23.0 and later.

## Mesos requirements

By default, the Alluxio Master needs ports 19998 and 19999, and the Alluxio Worker needs ports 29998, 29999, and 30000.
For Mesos to run Alluxio, you must either make these ports available to Mesos frameworks, or change the Alluxio ports.

#### Making ports available

When you launch the Mesos slave, you can specify the port resources for it to manage.

```bash
$ /usr/local/sbin/mesos-slave --resources='ports:[19998-19999,29998-30000]'
```

#### Changing Alluxio ports

Alternately, you may specify the Alluxio ports in your `alluxio-site.properties` file like so:

```properties
alluxio.master.port=31398
alluxio.master.web.port=31399

alluxio.worker.port=31498
alluxio.worker.data.port=31499
alluxio.worker.web.port=31500
```

## Deploying Alluxio on Mesos

To deploy Alluxio on Mesos, we need to make the Alluxio distribution available to Mesos. There are two ways to do this:

1. Install Alluxio on all Mesos nodes.
2. Point Mesos to an Alluxio tarball.

#### Setting configuration properties
See the [Configuration Settings](Configuration-Settings.html) documentation for instructions on how to set configuration properties.

#### Deploy with Alluxio already installed on all Mesos nodes

1. Install Alluxio on all Mesos nodes. The remaining steps should be performed from an Alluxio installation
2. Set the configuration property `alluxio.integration.mesos.alluxio.jar.url` to `LOCAL`
3. Set the configuration property `alluxio.home` to the path where Alluxio is installed on the Mesos nodes
4. Launch the Alluxio Mesos framework

{% include Running-Alluxio-on-Mesos/alluxio-mesos.md %}

#### Deploy with Alluxio tarball url

From anywhere with Alluxio installed:

1. Set the configuration property `alluxio.integration.mesos.alluxio.jar.url` to point to an Alluxio tarball
2. Launch the Alluxio Mesos framework

{% include Running-Alluxio-on-Mesos/alluxio-mesos.md %}

Note that the tarball should be compiled with `-Pmesos`. Released Alluxio tarballs from version 1.3.0 onwards are compiled this way.

#### Java

By default, the Alluxio Mesos framework will download the Java 7 jdk and use it to run Alluxio. If you would prefer
to use whatever version of java is available on the Mesos executor, set the configuration property

```properties
alluxio.integration.mesos.jdk.url=LOCAL
```

#### Configuring Alluxio Masters and Workers

When Alluxio is deployed on Mesos, it propagates all Alluxio configuration to the launched masters and workers.
This means you can configure the launched Alluxio cluster by setting configuration properties in `conf/alluxio-site.properties`.

#### Log files

The `./integration/mesos/bin/alluxio-mesos-start.sh` script will launch an `AlluxioFramework` Java process which will log to `alluxio/logs/framework.out`.

Alluxio masters and workers launched on Mesos will write their Alluxio logs to `mesos_container/logs/`. There
may also be useful information in the `mesos_container/stderr` file.
