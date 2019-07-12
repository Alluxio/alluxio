---
layout: global
title: Deploy Alluxio on Mesos
nickname: Mesos
group: Deploying Alluxio
priority: 4
---

* Table of Contents
{:toc}

**Note: This legacy scheduler is DEPRECATED and is no longer maintained. Please expect a new scheduler in Alluxio 2.0.**

Alluxio can be deployed through [Mesos](http://mesos.apache.org/getting-started/). This allows Mesos to manage the resources used by Alluxio. For the Alluxio
master, the managed resources are the CPU and memory needed by the JVM process. For the worker, the additional resources
include the ramdisk memory (if configuring memory as a storage tier) and other optional tiered storage such as SSD or HDD.

## Mesos version

Alluxio is compatible with Mesos **0.23.0 and later**.

## Mesos requirements

By default, the Alluxio Master needs ports *19998* and *19999*, and the Alluxio Worker needs ports *29998*, *29999*, and *30000*.
For Mesos to run Alluxio, you must either make these ports available to the Mesos framework, or change the Alluxio ports.

### Making ports available

When you launch the Mesos slave, you can specify the port resources for it to manage.

```bash
/usr/local/sbin/mesos-slave --resources='ports:[19998-19999,29998-30000]'
```

### Changing Alluxio ports

Alternately, you may specify the Alluxio ports in `alluxio-site.properties`:

```properties
alluxio.master.rpc.port=31398
alluxio.master.web.port=31399

alluxio.worker.rpc.port=31498
alluxio.worker.web.port=31500
```

## Deploying Alluxio on Mesos

To deploy Alluxio on Mesos, we need to make the Alluxio distribution available to Mesos. There are two ways to do this:

1. Install Alluxio on all Mesos nodes.
2. Point Mesos to an Alluxio tarball.

### Setting configuration properties
See the [Configuration Settings]({{ '/en/basic/Configuration-Settings.html' | relativize_url }})
documentation for instructions on how to set configuration properties.

### Deploy with Alluxio already installed on all Mesos nodes

1. Install Alluxio on all Mesos nodes. The remaining steps should be performed from an Alluxio installation
2. Set the configuration property `alluxio.integration.mesos.alluxio.jar.url` to `LOCAL`
3. Set the configuration property `alluxio.home` to the path where Alluxio is installed on the Mesos nodes
4. Launch the Alluxio Mesos framework

```bash
./integration/mesos/bin/alluxio-mesos-start.sh mesosMaster:5050 // address of Mesos master
```

### Deploy with Alluxio tarball URL

From anywhere with Alluxio installed:

1. Set the configuration property `alluxio.integration.mesos.alluxio.jar.url` to point to an Alluxio tarball
2. Launch the Alluxio Mesos framework

```bash
./integration/mesos/bin/alluxio-mesos-start.sh mesosMaster:5050 // address of Mesos master
```

- Note that the tarball should be compiled with `-Pmesos`. Released Alluxio tarballs from version 1.3.0 onwards are compiled this way.
- Mesos library path is assumed to be at `/usr/lib`. If it is not, you can update `MESOS_LIBRARY_PATH` in `alluxio-mesos-start.sh` with the correct path.

Verify that the Alluxio Mesos framework is working by starting a Mesos master and confirming that it is detected in `alluxio/logs/framework.out`.

- Example from `alluxio/logs/framework.out`  
*I0612 18:08:31.251133  3047 sched.cpp:336] New master detected at master@127.0.0.1:5050*

### Java

By default, the Alluxio Mesos framework will use whatever version of Java is available on the Mesos executor. Download a
Java 8 jdk and use it to run Alluxio by setting the configuration property

```properties
alluxio.integration.mesos.jdk.url=JDK_URL
```

### Configuring Alluxio Masters and Workers

When Alluxio is deployed on Mesos, it propagates all Alluxio configuration to the launched masters and workers.
This means you can configure the launched Alluxio cluster by setting configuration properties in `conf/alluxio-site.properties`.

### Log files

The `./integration/mesos/bin/alluxio-mesos-start.sh` script will launch an `AlluxioFramework` Java process which will log to `alluxio/logs/framework.out`.

Alluxio masters and workers launched on Mesos will write their Alluxio logs to `mesos_container/logs/`. There
may also be useful information in the `mesos_container/stderr` file.
