---
layout: global
title: Other Under File Storages
nickname: Other UFSes
group: Under Stores
priority: 10
---

* Table of Contents
{:toc}

## Ceph

This guide describes how to configure Alluxio with Ceph as the under storage system. Alluxio supports
two different clients APIs to connect to [Ceph Object Storage](http://ceph.com/ceph-storage/object-storage/)
using [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/):
- [S3A](http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) (preferred)
- [Swift](http://docs.openstack.org/developer/swift/)

### Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %}), or
[download the binaries locally]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-Locally.md %}).

### Configuring Alluxio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

#### Option 1: S3A Interface (preferred)

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.address=s3a://<bucket>/<folder>
aws.accessKeyId=<access-key>
aws.secretKey=<secret-key>
alluxio.underfs.s3.endpoint=http://<rgw-hostname>:<rgw-port>
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3a.inherit_acl=<inherit-acl>
```

If using a Ceph release such as hammer (or older) specify `alluxio.underfs.s3a.signer.algorithm=S3SignerType`
to use v2 S3 signatures. To use GET Bucket (List Objects) Version 1 specify
`alluxio.underfs.s3a.list.objects.v1=true`.

#### Option 2: Swift Interface
Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.address=swift://<container>/<folder>
fs.swift.user=<swift-user>
fs.swift.tenant=<swift-tenant>
fs.swift.password=<swift-user-password>
fs.swift.auth.url=<swift-auth-url>
fs.swift.use.public.url=<swift-use-public>
fs.swift.auth.method=<swift-auth-model>
```
Replace `<container>/<folder>` with an existing Swift container location. Possible values of `<swift-use-public>` are
`true`, `false`. Specify `<swift-auth-model>` as `swiftauth` if using native Ceph RGW authentication and `<swift-auth-url>`
as `http://<rgw-hostname>:<rgw-port>/auth/1.0`.

### Running Alluxio Locally with Ceph

After everything is configured, you can start up Alluxio locally to see that everything works.

```bash
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ ./bin/alluxio runTests
```

After this succeeds, you can visit your bucket/container to verify the files and directories created
by Alluxio exist.

If using the S3A connector, you should see files named like:
```
<bucket>/<folder>/default_tests_files/Basic_CACHE_THROUGH
```

If using the Swift connector, you should see files named like:
```
<container>/<folder>/default_tests_files/Basic_CACHE_THROUGH
```

To stop Alluxio, run:

```bash
$ ./bin/alluxio-stop.sh local
```

### Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying Ceph Object Storage.
Depending on the interace used, refer to [S3A Access Control]({{ site.baseurl }}{% link en/ufs/Ufs-S3.md %}#s3-access-control)
or [Swift Access Control]({{ site.baseurl }}{% link en/ufs/Ufs-Other.md %}#swift-access-control) for more information.

## GlusterFS

This guide describes how to configure Alluxio with [GlusterFS](http://www.gluster.org/) as the under
storage system.

### Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %}), or
[download the binaries locally]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-Locally.md %}).

### Configuring Alluxio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Assuming the GlusterFS bricks are co-located with Alluxio nodes, the GlusterFS volume is mounted at
`/mnt/gluster`, the following configuration parameters need to be added to
`conf/alluxio-site.properties`:

{% include Configuring-Alluxio-with-GlusterFS/underfs-address.md %}

### Running Alluxio Locally with GlusterFS

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit your GlusterFS volume to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-GlusterFS/glusterfs-file.md %}

To stop Alluxio, you can run:

{% include Common-Commands/stop-alluxio.md %}

## MapR-FS

This guide describes how to configure Alluxio with [MapR-FS](https://www.mapr.com/products/mapr-fs)
as the under storage system.

### Compiling Alluxio with MapR Version

Alluxio must be [compiled]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %}) with the correct MapR distribution
to integrate with MapR-FS. Here are some values of `hadoop.version` for different MapR
distributions:

<table class="table table-striped">
<tr><th>MapR Version</th><th>hadoop.version</th></tr>
<tr>
  <td>5.2</td>
  <td>2.7.0-mapr-1607</td>
</tr>
<tr>
  <td>5.1</td>
  <td>2.7.0-mapr-1602</td>
</tr>
<tr>
  <td>5.0</td>
  <td>2.7.0-mapr-1506</td>
</tr>
<tr>
  <td>4.1</td>
  <td>2.5.1-mapr-1503</td>
</tr>
<tr>
  <td>4.0.2</td>
  <td>2.5.1-mapr-1501</td>
</tr>
<tr>
  <td>4.0.1</td>
  <td>2.4.1-mapr-1408</td>
</tr>
</table>

### Configuring Alluxio for MapR-FS

Once you have compiled Alluxio with the appropriate `hadoop.version` for your MapR distribution, you
may have to configure Alluxio to recognize the MapR-FS scheme and URIs. Alluxio uses the HDFS client
to access MapR-FS, and by default is already configured to do so. However, if the configuration has
been changed, you can enable the HDFS client to access MapR-FS URIs by adding the URI prefix
`maprfs:///` to the configuration variable `alluxio.underfs.hdfs.prefixes` like below:

```properties
alluxio.underfs.hdfs.prefixes=hdfs://,maprfs:///
```

This configuration parameter should be set for all the Alluxio servers (masters, workers). Please
read how to [configure Alluxio]({{ site.baseurl }}{% link en/advanced/Configuration-Settings.md %}). For Alluxio processes, this parameter
can be set in the property file `alluxio-site.properties`. For more information, please read about
[configuration of Alluxio with property files]({{ site.baseurl }}{% link en/advanced/Configuration-Settings.md %}#property-files).

### Configuring Alluxio to use MapR-FS as Under File System

There are various ways to configure Alluxio to use MapR-FS as the Under File System. If you want to
mount MapR-FS to the root of Alluxio, add the following to `conf/alluxio-site.properties`:

```properties
alluxio.underfs.address=maprfs:///<path in MapR-FS>/
```

You can also mount a directory in MapR-FS to a directory in the Alluxio namespace.

```bash
$ ${ALLUXIO_HOME}/bin/alluxio fs mount /<path in Alluxio>/ maprfs:///<path in MapR-FS>/
```

### Running Alluxio Locally with MapR-FS

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

After this succeeds, you can visit MapR-FS web UI to verify the files and directories created by
Alluxio exist. For this test, you should see files named like:
`/default_tests_files/BASIC_CACHE_CACHE_THROUGH`

You can stop Alluxio any time by running:

{% include Common-Commands/stop-alluxio.md %}

## Minio

This guide describes how to configure Alluxio with [Minio](https://minio.io/) as the
under storage system. Alluxio natively provides the `s3a://` scheme (recommended for better performance). You can
use this scheme to connect Alluxio with Minio server.

### Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %}), or
[download the binaries locally]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-Locally.md %}).

### Setup Minio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Minio is an object storage server built for cloud applications and DevOps. Minio provides an open source alternative to AWS S3.

Launch a Minio server instance using the steps mentioned [here](http://docs.minio.io/docs/minio-quickstart-guide). Then create a
bucket (or use an existing bucket). Once the server is launched, keep a note of Minio server endpoint, accessKey
and secretKey.

You should also note the directory you want to use in that bucket, either by creating
a new directory in the bucket, or using an existing one. For the purposes of this guide, the Minio bucket name is called
`MINIO_BUCKET`, and the directory in that bucket is called `MINIO_DIRECTORY`.

### Configuring Alluxio

You need to configure Alluxio to use Minio as its under storage system by modifying
`conf/alluxio-site.properties`. The first modification is to specify an **existing** Minio
bucket and directory as the under storage system.

All the fields to be modified in `conf/alluxio-site.properties` file are listed here:

```properties
alluxio.underfs.address=s3a://<MINIO_BUCKET>/<MINIO_DIRECTORY>
alluxio.underfs.s3.endpoint=http://<MINIO_ENDPOINT>/
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3a.inherit_acl=false
aws.accessKeyId=<MINIO_ACCESS_KEY_ID>
aws.secretKey=<MINIO_SECRET_KEY_ID>
```

For these parameters, replace `<MINIO_ENDPOINT>` with the hostname and port of your Minio service, e.g.,
`http://localhost:9000`. If the port value is left unset, it defaults to port 80 for `http` and 443 for `https`.

## NFS

This guide describes the instructions to configure [NFS](http://nfs.sourceforge.net) as Alluxio's under
storage system.

### Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %}), or
[download the binaries locally]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-Locally.md %}).

### Configuring Alluxio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Assuming the NFS clients are co-located with Alluxio nodes, all the NFS shares are mounted at
directory `/mnt/nfs`, the following environment variable assignment needs to be added to
`conf/alluxio-site.properties`:

```
alluxio.master.hostname=localhost
alluxio.underfs.address=/mnt/nfs
```

### Running Alluxio with NFS

Simply run the following command to start Alluxio filesystem.

{% include Configuring-Alluxio-with-NFS/start-alluxio.md %}

To verify that Alluxio is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder.

Next, you can run a simple example program:

{% include Configuring-Alluxio-with-NFS/runTests.md %}

After this succeeds, you can visit your NFS volume to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

{% include Configuring-Alluxio-with-NFS/nfs-file.md %}

You can stop Alluxio any time by running:

{% include Configuring-Alluxio-with-NFS/stop-alluxio.md %}

## OBS

This guide describes how to configure Alluxio with
[Huawei OBS](http://www.huaweicloud.com/en-us/product/obs.html) as the under storage system. Object Storage
Service (OBS) is a massive, secure and highly reliable cloud storage service provided by Huawei Cloud.

### Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines. You can either
[compile Alluxio]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %}), or
[download the binaries locally]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-Locally.md %}).

OBS under storage is implemented as an under storage extension. A precompiled OBS under storage jar can be downloaded from [here](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/obs/target).

Then execute the following command on master to install the extension to all masters and workers defined in `conf/masters` and `conf/workers`:

```bash
$ bin/alluxio extensions install /PATH/TO/DOWNLOADED/OBS/jar
```

See [this doc]({{ site.baseurl }}{% link en/ufs/Ufs-Extensions.md %}) for more details on Alluxio extension management.

A bucket and directory in OBS should exist before mounting OBS to Alluxio, create them if they do not exist.
Suppose the bucket is named `OBS_BUCKET` and the directory is named `OBS_DIRECTORY`.
Please refer to [this doc](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0046535383.html) for more
information on creating a bucket in OBS.

The OBS endpoint specifying the region of your bucket should also be set in Alluxio configurations.
Suppose the endpoint is named `OBS_ENDPOINT`.
Please refer to [this doc](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0075679174.html) for more
information on different regions and endpoints in OBS.

### Mounting OBS

Alluxio unifies access to different storage systems through the
[unified namespace]({{ site.baseurl }}{% link en/advanced/Namespace-Management.md %}) feature. An OBS location can be
either mounted at the root of the Alluxio namespace or at a nested directory.

#### Root Mount

You need to configure Alluxio to use OBS as its under storage system. The first modification is to
specify an existing OBS bucket and folder as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.underfs.address=obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

Next you need to specify the Huawei Cloud credentials for OBS access. In `conf/alluxio-site.properties`,
add:

```
fs.obs.accessKey=<OBS_ACCESS_KEY>
fs.obs.secretKey=<OBS_SECRET_KEY>
fs.oss.endpoint=<OBS_ENDPOINT>
```

Here `fs.obs.accessKey` is the Access Key string and `fs.obs.secretKey` is the Secret Key
string, please refer to [help on managing access keys](http://support.huaweicloud.com/en-us/usermanual-ca/en-us_topic_0046606340.html).
`fs.obs.endpoint` is the endpoint of this bucket, please refer to [this doc](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0075679174.html).

After these changes, Alluxio should be configured to work with OBS as its under storage system,
and you can try to run alluxio locally with OBS.

#### Nested Mount
An OBS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{ site.baseurl }}{% link en/advanced/Command-Line-Interface.md %}#mount) can be used for this purpose.
For example, the following command mounts a folder inside an OBS bucket into Alluxio directory
`/obs`:

```bash
$ ./bin/alluxio fs mount --option fs.obs.accessKey=<OBS_ACCESS_KEY> \
  --option fs.obs.secretKey=<OBS_SECRET_KEY> \
  --option fs.obs.endpoint=<OBS_ENDPOINT> \
  /obs obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

### Running Alluxio Locally with OBS

After everything is configured, you can start up Alluxio locally to see that everything works.

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ bin/alluxio runTests
```

After this succeeds, you can visit your OBS folder `obs://<OBS_BUCKET>/<OBS_DIRECTORY>` to verify the files
and directories created by Alluxio exist.

To stop Alluxio, you can run:

```bash
$ bin/alluxio-stop.sh local
```

## OSS

This guide describes how to configure Alluxio with
[Aliyun OSS](https://intl.aliyun.com/product/oss) as the under storage system. Object Storage
Service (OSS) is a massive, secure and highly reliable cloud storage service provided by Aliyun.

### Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines.You can either
[compile Alluxio]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %}), or
[download the binaries locally]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-Locally.md %}).

Also, in preparation for using OSS with alluxio, create a bucket or use an existing bucket. You
should also note the directory you want to use in that bucket, either by creating a new
directory in the bucket, or using an existing one. For the purposes of this guide, the OSS bucket
name is called `OSS_BUCKET`, and the directory in that bucket is called `OSS_DIRECTORY`. Also, for
using the OSS Service, you should provide an OSS endpoint to specify which range your bucket is
on. The endpoint here is called `OSS_ENDPOINT`, and to learn more about the endpoints for special
range you can see [here](https://intl.aliyun.com/help/en/doc-detail/31834.htm). For more
information about OSS Bucket, please see [here](https://intl.aliyun.com/help/doc-detail/31885.htm)

### Mounting OSS

Alluxio unifies access to different storage systems through the
[unified namespace]({{ site.baseurl }}{% link en/advanced/Namespace-Management.md %}) feature. An OSS location can be
either mounted at the root of the Alluxio namespace or at a nested directory.

#### Root Mount

You need to configure Alluxio to use OSS as its under storage system. The first modification is to
specify an existing OSS bucket and directory as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.underfs.address=oss://<OSS_BUCKET>/<OSS_DIRECTORY>/
```

Next you need to specify the Aliyun credentials for OSS access. In `conf/alluxio-site.properties`,
add:

```
fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID>
fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET>
fs.oss.endpoint=<OSS_ENDPOINT>
```

Here `fs.oss.accessKeyId` is the Access Key Id string and `fs.oss.accessKeySecret` is the Access
Key Secret string, which are managed in [AccessKeys](https://ak-console.aliyun.com) in Aliyun UI.
`fs.oss.endpoint` is the endpoint of this bucket, which can be found in the Bucket overview with
possible values like "oss-us-west-1.aliyuncs.com", "oss-cn-shanghai.aliyuncs.com"
([OSS Internet Endpoint](https://intl.aliyun.com/help/doc-detail/31837.htm)).

After these changes, Alluxio should be configured to work with OSS as its under storage system,
and you can try to run alluxio locally with OSS.

#### Nested Mount
An OSS location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command]({{ site.baseurl }}{% link en/advanced/Command-Line-Interface.md %}#mount) can be used for this purpose.
For example, the following command mounts a directory inside an OSS bucket into Alluxio directory
`/oss`:

```bash
$ ./bin/alluxio fs mount --option fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID> \
  --option fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET> \
  --option fs.oss.endpoint=<OSS_ENDPOINT> \
  /oss oss://<OSS_BUCKET>/<OSS_DIRECTORY>/
```

### Running Alluxio Locally with OSS

After everything is configured, you can start up Alluxio locally to see that everything works.

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ bin/alluxio runTests
```

After this succeeds, you can visit your OSS directory `oss://<OSS_BUCKET>/<OSS_DIRECTORY>` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like
`OSS_BUCKET/OSS_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

To stop Alluxio, you can run:

```bash
$ bin/alluxio-stop.sh local
```

## Swift

This guide describes how to configure Alluxio with an under storage system supporting the
[Swift API](http://docs.openstack.org/developer/swift/).

### Initial Setup

First, the Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ site.baseurl }}{% link en/contributor/Building-Alluxio-From-Source.md %}), or
[download the binaries locally]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-Locally.md %}).

### Configuring Alluxio

You need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.address=swift://<container>/<folder>
fs.swift.user=<swift-user>
fs.swift.tenant=<swift-tenant>
fs.swift.password=<swift-user-password>
fs.swift.auth.url=<swift-auth-url>
fs.swift.use.public.url=<swift-use-public>
fs.swift.auth.method=<swift-auth-model>
```

Replace `<container>/<folder>` with an existing Swift container location. Possible values of `<swift-use-public>`
are `true`, `false`. Possible values of `<swift-auth-model>` are `keystonev3`, `keystone`, `tempauth`, `swiftauth`. 

When using either keystone authentication, the following parameter can optionally be set:

```properties
fs.swift.region=<swift-preferred-region>
```

On the successful authentication, Keystone will return two access URLs: public and private. If
Alluxio is used inside company network and Swift is located on the same network it is adviced to set
value of `<swift-use-public>`  to `false`.

### Options for Swift Object Storage

Using the Swift module makes [Ceph Object Storage](https://ceph.com/ceph-storage/object-storage/) and [IBM SoftLayer](http://www.softlayer.com/object-storage) Object Storage as under storage options for Alluxio. To use Ceph, the [Rados Gateway](http://docs.ceph.com/docs/master/radosgw/) module must be deployed.

### Running Alluxio Locally with Swift

After configuration, you can start an Alluxio cluster:

```bash
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ ./bin/alluxio runTests
```

After this succeeds, you can visit your Swift container to verify the files and directories created
by Alluxio exist. For this test, you should see files named like:

```bash
<container>/<folder>/default_tests_files/Basic_CACHE_THROUGH
```

To stop Alluxio, you can run:

```bash
$ ./bin/alluxio-stop.sh local
```

### Running functional tests

```bash
$ mvn test -DtestSwiftContainerKey=swift://<container>
```

### Swift Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying object storage.

The Swift credentials specified in Alluxio (`fs.swift.user`, `fs.swift.tenant` and
`fs.swift.password`) represents a Swift user. Swift service backend checks the user permission to
the container. If the given Swift user does not have the right access permission to the specified
container, a permission denied error will be thrown. When Alluxio security is enabled, Alluxio loads
the container ACL to Alluxio permission on the first time when the metadata is loaded to Alluxio
namespace.

#### Mount point sharing

If you want to share the Swift mount point with other users in Alluxio namespace, you can enable
`alluxio.underfs.object.store.mount.shared.publicly`.

#### Permission change

In addition, chown/chgrp/chmod to Alluxio directories and files do NOT propagate to the underlying
Swift containers nor objects.
