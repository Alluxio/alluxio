---
layout: global
title: Ozone
---


This guide describes how to configure [Ozone](https://ozone.apache.org/) as Alluxio's under storage system. 

Ozone is a scalable, redundant, and distributed object store for Hadoop. Apart from scaling to billions of objects of varying sizes, 
Ozone can function effectively in containerized environments such as Kubernetes and YARN.

## Prerequisites

In preparation for using Ozone with Alluxio, follow the [Ozone On Premise Installation](https://ozone.apache.org/docs/1.2.1/start/onprem.html)
to install a Ozone cluster, and follow the [Cli Commands](https://ozone.apache.org/docs/1.2.1/interface/cli.html) to create volume and bucket for Ozone cluster.

## Basic Setup

To configure Alluxio to use Ozone as under storage, you will need to modify the configuration file 
`conf/alluxio-site.properties`. If the file does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Edit `conf/alluxio-site.properties` file to set the under storage address to the Ozone bucket and the Ozone directory you want to mount to Alluxio.
Ozone supports two different schemas `o3fs` and `ofs`
### o3fs
For example, the under storage address can be `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/` if you want to mount the whole bucket to Alluxio, 
or `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/alluxio/data` if only the directory `/alluxio/data` inside the ozone bucket `<OZONE_BUCKET>` of `<OZONE_VOLUME>` is mapped to Alluxio.

set the property `alluxio.underfs.hdfs.configuration` in `conf/alluxio-site.properties` to point to your `ozone-site.xml`. Make sure this configuration is set on all servers running Alluxio.

```properties
alluxio.dora.client.ufs.root=o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/
alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/ozone-site.xml
``` 

### ofs
For example, the under storage address can be `ofs://<OZONE_MANAGER>/<OZONE_VOLUME>/<OZONE_BUCKET>/` if you want to mount the whole bucket to Alluxio,
or `ofs://<OZONE_MANAGER>/<OZONE_VOLUME>/<OZONE_BUCKET>/alluxio/data` if only the directory `/alluxio/data` inside the ozone bucket `<OZONE_BUCKET>` of `<OZONE_VOLUME>` is mapped to Alluxio.

```properties
alluxio.dora.client.ufs.root=ofs://<OZONE_MANAGER>/<OZONE_VOLUME>/<OZONE_BUCKET>/
``` 

## Ozone HA Mode
### o3fs
To make Alluxio mount Ozone in HA mode, you should configure Alluxio's server so that it can find the OzoneManager. Please note that once set up, your application using the Alluxio client does not require any special configuration.
In HA mode `alluxio.dora.client.ufs.root` needs to specify `<OM_SERVICE_IDS>`
such as:

```properties
alluxio.dora.client.ufs.root=o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>.<OM_SERVICE_IDS>/
alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/ozone-site.xml
``` 

### ofs
```properties
alluxio.dora.client.ufs.root=ofs://<OZONE_MANAGER>/<OZONE_VOLUME>/<OZONE_BUCKET>/
alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/ozone-site.xml
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
alluxio.underfs.version=<OZONE_VERSION>
```

## Example: Running Alluxio Locally with Ozone

Start the Alluxio servers:

```shell
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This will start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```shell
$ ./bin/alluxio runTests
```

Use the HDFS shell or Ozone shell to Visit your Ozone directory `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/<OZONE_DIRECTORY>`
to verify the files and directories created by Alluxio exist. For this test, you should see files named like
`<OZONE_BUCKET>.<OZONE_VOLUME>/<OZONE_DIRECTORY>/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

Stop Alluxio by running:

```shell
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Supported Ozone Versions

Currently, the only tested Ozone version with Alluxio is `1.0.0`, `1.1.0`, `1.2.1`.

## Contributed by the Alluxio Community

Ozone UFS integration is contributed and maintained by the Alluxio community.
The source code is located [here](https://github.com/Alluxio/alluxio/tree/master/underfs/ozone).
Feel free submit pull requests to improve the integration and update 
the documentation [here](https://github.com/Alluxio/alluxio/edit/master/docs/en/ufs/Ozone.md) 
if any information is missing or out of date.
