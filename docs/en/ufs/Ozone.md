---
layout: global
title: Ozone
nickname: Ozone
group: Storage Integrations
priority: 10
---


This guide describes how to configure [Ozone](https://hadoop.apache.org/ozone) as Alluxio's under storage system. 
Ozone is a scalable, redundant, and distributed object store for Hadoop. Apart from scaling to billions of objects of varying sizes, 
Ozone can function effectively in containerized environments such as Kubernetes and YARN.

## Prerequisites

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines. You can either [download the precompiled binaries directly]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }})
with the correct Hadoop version (recommended), or 
[compile the binaries from Alluxio source code]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}) (for advanced users).

In preparation for using Ozone with Alluxio, follow the [Ozone On Premise Installation](https://hadoop.apache.org/ozone/docs/1.0.0/start/onprem.html)
to install a Ozone cluster, and follow the [Volume Commands](https://hadoop.apache.org/ozone/docs/1.0.0/shell/volumecommands.html) and 
[Bucket Commands](https://hadoop.apache.org/ozone/docs/1.0.0/shell/bucketcommands.html) to create volume and bucket for Ozone cluster.

## Basic Setup

To configure Alluxio to use Ozone as under storage, you will need to modify the configuration file 
`conf/alluxio-site.properties`. If the file does not exist, create the configuration file from the template.

```
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Edit `conf/alluxio-site.properties` file to set the under storage address to the Ozone bucket and 
the Ozone directory you want to mount to Alluxio. For example, the under storage address can be `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/` if
you want to mount the whole bucket to Alluxio, or `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/alluxio/data` if only the directory `/alluxio/data`
inside the ozone bucket `<OZONE_BUCKET>` of `<OZONE_VOLUME>` is mapped to Alluxio.

```
alluxio.master.mount.table.root.ufs=o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/
``` 

## Example: Running Alluxio Locally with Ozone

Start the Alluxio servers:

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This will start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Use the HDFS shell or Ozone shell to Visit your Ozone directory `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/<OZONE_DIRECTORY>`
to verify the files and directories created by Alluxio exist. For this test, you should see files named like
`<OZONE_BUCKET>.<OZONE_VOLUME>/<OZONE_DIRECTORY>/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

Stop Alluxio by running:

```console
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Mount Ozone 

An Ozone location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{ '/en/operation/User-CLI.html' | relativize_url }}#mount) can be used for this purpose.
For example, the following command mounts a directory inside an Ozone bucket into Alluxio directory
`/ozone`:

```console
$ ./bin/alluxio fs mount \
  --option alluxio.underfs.hdfs.configuration=<DIR>/ozone-site.xml:<DIR>/core-site.xml \
  /ozone o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/
```

Possible `core-site.xml` and `ozone-site.xml`
- `core-site.xml`

```xml
<configuration>
  <property>
    <name>fs.o3fs.impl</name>
    <value>org.apache.hadoop.fs.ozone.BasicOzoneFileSystem</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.o3fs.impl</name>
    <value>org.apache.hadoop.fs.ozone.BasicOzFs</value>
  </property>
</configuration>
```

- `ozone-site.xml`

```xml
<configuration>
  <property>
    <name>ozone.om.address</name>
    <value>localhost</value>
  </property>
  <property>
    <name>scm.container.client.max.size</name>
    <value>256</value>
  </property>
  <property>
    <name>scm.container.client.idle.threshold</name>
    <value>10s</value>
  </property>
  <property>
    <name>hdds.ratis.raft.client.rpc.request.timeout</name>
    <value>60s</value>
  </property>
  <property>
    <name>hdds.ratis.raft.client.async.outstanding-requests.max</name>
    <value>32</value>
  </property>
  <property>
    <name>hdds.ratis.raft.client.rpc.watch.request.timeout</name>
    <value>180s</value>
  </property>
</configuration>
```

Make sure the related config file is on all servers nodes running Alluxio.

### Supported Ozone Versions

Currently, the only tested Ozone version with Alluxio is `1.0.0`.

## Contributed by the Alluxio Community

Ozone UFS integration is contributed and maintained by the Alluxio community.
The source code is located [here](https://github.com/Alluxio/alluxio/tree/master/underfs/ozone).
Feel free submit pull requests to improve the integration and update 
the documentation [here](https://github.com/Alluxio/alluxio/edit/master/docs/en/ufs/Ozone.md) 
if any information is missing or out of date.
