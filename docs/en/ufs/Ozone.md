---
layout: global
title: Ozone
---


This guide describes how to configure [Ozone](https://ozone.apache.org/){:target="_blank"} as Alluxio's under storage system. 

Ozone is a scalable, redundant, and distributed object store for Hadoop. Apart from scaling to billions of objects of varying sizes, 
Ozone can function effectively in containerized environments such as Kubernetes and YARN.

Ozone supports two different schemas. The biggest difference between `o3fs` and `ofs` is that `o3fs` suports operations only at a **single bucket**, while `ofs` supports operations across all volumes and buckets and provides a full view of all the volume/buckets. For more information, please read its documentation:
- [o3fs](https://ozone.apache.org/docs/1.0.0/interface/o3fs.html){:target="_blank"}
- [ofs](https://ozone.apache.org/docs/1.0.0/interface/ofs.html){:target="_blank"}

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using Ozone with Alluxio:

{% navtabs Prerequisites %}
{% navtab o3fs %}

<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<OZONE_VOLUME>`</td>
        <td markdown="span">[Create a new volume](https://ozone.apache.org/docs/1.2.1/interface/o3fs.html){:target="_blank"} or use an existing volume</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OZONE_BUCKET>`</td>
        <td markdown="span">[Create a new bucket](https://ozone.apache.org/docs/1.2.1/interface/o3fs.html){:target="_blank"} or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OZONE_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OM_SERVICE_IDS>`</td>
        <td markdown="span">To select between the available HA clusters, a logical named called a serviceID is required for each of the cluseters. Read [more](https://ozone.apache.org/docs/1.0.0/feature/ha.html){:target="_blank"}</td>
    </tr>
</table>

{% endnavtab %}
{% navtab ofs %}

<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<OZONE_MANAGER>`</td>
        <td markdown="span">The namespace manager for Ozone. See [Ozone Manager](https://ozone.apache.org/docs/1.2.1/concept/ozonemanager.html){:target="_blank"}</td>
    </tr><tr>
        <td markdown="span" style="width:30%">`<OZONE_VOLUME>`</td>
        <td markdown="span">[Create a new volume](https://ozone.apache.org/docs/1.2.1/interface/ofs.html){:target="_blank"} or use an existing volume</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OZONE_BUCKET>`</td>
        <td markdown="span">[Create a new bucket](https://ozone.apache.org/docs/1.2.1/interface/ofs.html){:target="_blank"} or use an existing bucket</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<OZONE_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the bucket, either by creating a new directory or using an existing one</td>
    </tr>
</table>

{% endnavtab %}
{% endnavtabs %}

Follow the [Ozone On Premise Installation](https://ozone.apache.org/docs/1.2.1/start/onprem.html){:target="_blank"} to install a Ozone cluster.

## Basic Setup

To use Ozone as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify an Ozone bucket and directory as the underfs address by modifying `conf/alluxio-site.properties`.

{% navtabs Setup %}
{% navtab o3fs %}

For example, the under storage address can be `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/` if you want to mount the whole bucket to Alluxio, 
or `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/alluxio/data` if only the directory `/alluxio/data` inside the ozone bucket `<OZONE_BUCKET>` of `<OZONE_VOLUME>` is mapped to Alluxio.

```properties
alluxio.dora.client.ufs.root=o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/
```

Set the property `alluxio.underfs.hdfs.configuration` in `conf/alluxio-site.properties` to point to your `ozone-site.xml`. Make sure this configuration is set on all servers running Alluxio.

```properties
alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/ozone-site.xml
``` 

{% endnavtab %}
{% navtab ofs %}

For example, the under storage address can be `ofs://<OZONE_MANAGER>/<OZONE_VOLUME>/<OZONE_BUCKET>/` if you want to mount the whole bucket to Alluxio,
or `ofs://<OZONE_MANAGER>/<OZONE_VOLUME>/<OZONE_BUCKET>/alluxio/data` if only the directory `/alluxio/data` inside the ozone bucket `<OZONE_BUCKET>` of `<OZONE_VOLUME>` is mapped to Alluxio.

```properties
alluxio.dora.client.ufs.root=ofs://<OZONE_MANAGER>/<OZONE_VOLUME>/<OZONE_BUCKET>/
``` 
{% endnavtab %}
{% endnavtabs %}

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

## Running Alluxio Locally with Ozone

Once you have configured Alluxio to Ozone, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

Use the HDFS shell or Ozone shell to visit your Ozone directory `o3fs://<OZONE_BUCKET>.<OZONE_VOLUME>/<OZONE_DIRECTORY>`
to verify the files and directories created by Alluxio exist. For this test, you should see files named like

```shell
<OZONE_BUCKET>.<OZONE_VOLUME>/<OZONE_DIRECTORY>/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE
```

## Advanced Setup

### Supported Ozone Versions

Currently, the only tested Ozone version with Alluxio is `1.0.0`, `1.1.0`, `1.2.1`.

## Contributed by the Alluxio Community

Ozone UFS integration is contributed and maintained by the Alluxio community.
The source code is located [here](https://github.com/Alluxio/alluxio/tree/master/underfs/ozone){:target="_blank"}.
Feel free submit pull requests to improve the integration and update 
the documentation [here](https://github.com/Alluxio/alluxio/edit/master/docs/en/ufs/Ozone.md){:target="_blank"} 
if any information is missing or out of date.
