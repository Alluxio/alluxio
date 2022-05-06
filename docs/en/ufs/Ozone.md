---
layout: global
title: Ozone
nickname: Ozone
group: Storage Integrations
priority: 10
---


This guide describes how to configure [Ozone](https://ozone.apache.org/) as Alluxio's under storage system. 
Ozone is a scalable, redundant, and distributed object store for Hadoop. Apart from scaling to billions of objects of varying sizes, 
Ozone can function effectively in containerized environments such as Kubernetes and YARN.

## Prerequisites

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines. You can either [download the precompiled binaries directly]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }})
with the correct Hadoop version (recommended), or 
[compile the binaries from Alluxio source code]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}) (for advanced users).

In preparation for using Ozone with Alluxio, follow the [Ozone On Premise Installation](https://ozone.apache.org/docs/1.2.1/start/onprem.html)
to install a Ozone cluster, and follow the [Cli Commands](https://ozone.apache.org/docs/1.2.1/interface/cli.html) to create volume and bucket for Ozone cluster.

## Basic Setup

To configure Alluxio to use Ozone as under storage, you will need to modify the configuration file 
`conf/alluxio-site.properties`. If the file does not exist, create the configuration file from the template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Edit `conf/alluxio-site.properties` file to set the under storage address to the Ozone bucket and the Ozone directory you want to mount to Alluxio. 
For example, the under storage address can be `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/` if you want to mount the whole bucket to Alluxio, 
or `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/alluxio/data` if only the directory `/alluxio/data` inside the ozone bucket `<OZONE_BUCKET>` of `<OZONE_VOLUME>` is mapped to Alluxio.

set the property `alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration` in `conf/alluxio-site.properties` to point to your `ozone-site.xml`. Make sure this configuration is set on all servers running Alluxio.

```properties
alluxio.master.mount.table.root.ufs=o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/
alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/ozone-site.xml
``` 

## Ozone HA Mode

To make Alluxio mount Ozone in HA mode, you should configure Alluxio's server so that it can find the OzoneManager. Please note that once set up, your application using the Alluxio client does not require any special configuration.
In HA mode `alluxio.master.mount.table.root.ufs` needs to specify `<OM_SERVICE_IDS>`
such as:
```properties
alluxio.master.mount.table.root.ufs=o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>.<OM_SERVICE_IDS>/
alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/ozone-site.xml
``` 

`<OM_SERVICE_IDS>` can be found in `ozone-site.xml`.
In the following example `ozone-site.xml` file, `<OM_SERVICE_IDS>` is `omservice1`:
```xml
<property>
    <name>ozone.om.service.ids</name>
    <value>omservice1</value>
</property>
```

### Mount Ozone with Specific Versions

Users can mount an Ozone cluster of a specific version as an under storage into Alluxio namespace.
Before mounting Ozone with a specific version, make sure you have built a client with that specific version of Ozone.
You can check the existence of this client by going to the `lib` directory under the Alluxio directory.

When mounting the under storage at the Alluxio root with a specific Ozone version, one can add the following line to the site properties file (`conf/alluxio-site.properties`).

```properties
alluxio.master.mount.table.root.option.alluxio.underfs.version=<OZONE_VERSION>
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
  --option alluxio.underfs.hdfs.configuration=<DIR>/ozone-site.xml \
  /ozone o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/
```

If you need to mount an Ozone cluster of a specific version, you can specify it through the command line option `alluxio.underfs.version=<OZONE_VERSION>`.
```console
$ ./bin/alluxio fs mount \
  --option alluxio.underfs.hdfs.configuration=<DIR>/ozone-site.xml \
  --option alluxio.underfs.version=<OZONE_VERSION> \
  /ozone o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/
```

Possible `ozone-site.xml`

- `ozone-site.xml`

```xml
<configuration>
  <property>
    <name>ozone.om.address</name>
    <value>localhost</value>
  </property>
</configuration>
```

Make sure the related config file is on all servers nodes running Alluxio.

### Supported Ozone Versions

Currently, the only tested Ozone version with Alluxio is `1.0.0`, `1.1.0`, `1.2.1`.

## Contributed by the Alluxio Community

Ozone UFS integration is contributed and maintained by the Alluxio community.
The source code is located [here](https://github.com/Alluxio/alluxio/tree/master/underfs/ozone).
Feel free submit pull requests to improve the integration and update 
the documentation [here](https://github.com/Alluxio/alluxio/edit/master/docs/en/ufs/Ozone.md) 
if any information is missing or out of date.
