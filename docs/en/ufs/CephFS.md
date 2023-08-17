---
layout: global
title: CephFS
---

This guide describes how to configure Alluxio with [CephFS](https://ceph.io/en/discover/technology/#file){:target="_blank"} as the under storage system. 

The Ceph File System (CephFS) is a POSIX-compliant file system built on top of Ceph’s distributed object store, RADOS. CephFS endeavors to provide a state-of-the-art, multi-use, highly available, and performant file store for a variety of applications, including traditional use-cases like shared home directories, HPC scratch space, and distributed workflow shared storage.

Alluxio supports two different implementations of under storage system for CephFS. Fore more information, please read its documentation:
- [cephfs](https://docs.ceph.com/en/latest/cephfs/api/libcephfs-java/){:target="_blank"}
- [cephfs-hadoop](https://docs.ceph.com/en/nautilus/cephfs/hadoop/){:target="_blank"}

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using CephFS with Alluxio:
<table class="table table-striped">
  <tr>
    <td markdown="span" style="width:30%">`<CEPHFS_CONF_FILE>`</td>
    <td markdown="span">Local path to Ceph configuration file ceph.conf</td>
  </tr>
  <tr>
    <td markdown="span" style="width:30%">`<CEPHFS_NAME>`</td>
    <td markdown="span">Ceph URI that is used to identify dameon instances in the ceph.conf</td>
  </tr>
  <tr>
    <td markdown="span" style="width:30%">`<CEPHFS_DIRECTORY>`</td>
    <td markdown="span">The directory you want to use, either by creating a new directory or using an existing one</td>
  </tr>
  <tr>
    <td markdown="span" style="width:30%">`<CEPHFS_AUTH_ID>`</td>
    <td markdown="span">Ceph user id</td>
  </tr>
  <tr>
    <td markdown="span" style="width:30%">`<CEPHFS_KEYRING_FILE>`</td>
    <td markdown="span">Ceph keyring file that stores one or more Ceph authentication keys</td>
  </tr>
</table>

### Install Dependencies
Follow [Ceph packages install](https://docs.ceph.com/en/latest/install/get-packages/){:target="_blank"} to install below packages:

```
cephfs-java
libcephfs_jni
libcephfs2
```

### Make symbolic links

```shell
$ ln -s /usr/lib64/libcephfs_jni.so.1.0.0 /usr/lib64/libcephfs_jni.so
$ ln -s /usr/lib64/libcephfs.so.2.0.0 /usr/lib64/libcephfs.so
$ java_path=`which java | xargs readlink | sed 's#/bin/java##g'`
$ ln -s /usr/share/java/libcephfs.jar $java_path/jre/lib/ext/libcephfs.jar
```

### Download CephFS Hadoop jar

```shell
$ curl -o $java_path/jre/lib/ext/hadoop-cephfs.jar -s https://download.ceph.com/tarballs/hadoop-cephfs.jar
```

## Basic Setup

To use CephFS as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
$ cp conf/core-site.xml.template conf/core-site.xml
```

{% navtabs Setup %}
{% navtab cephfs %}

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.cephfs.conf.file=<CEPHFS_CONF_FILE>
alluxio.underfs.cephfs.mds.namespace=<CEPHFS_NAME>
alluxio.underfs.cephfs.mount.point=<CEPHFS_DIRECTORY>
alluxio.underfs.cephfs.auth.id=<CEPHFS_AUTH_ID>
alluxio.underfs.cephfs.auth.keyring=<CEPHFS_KEYRING_FILE>
```

{% endnavtab %}
{% navtab cephfs-hadoop %}

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.hdfs.configuration=${ALLUXIO_HOME}/conf/core-site.xml
```

Modify `conf/core-site.xml` to include:

```xml
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>ceph://mon1,mon2,mon3/</value>
  </property>
  <property>
    <name>fs.defaultFS</name>
    <value>ceph://mon1,mon2,mon3/</value>
  </property>
  <property>
    <name>ceph.data.pools</name>
    <value>${data-pools}</value>
  </property>
  <property>
    <name>ceph.auth.id</name>
    <value>${client-id}</value>
  </property>
  <property>
    <name>ceph.conf.options</name>
    <value>client_mount_gid=${gid},client_mount_uid=${uid},client_mds_namespace=${ceph-fs-name}</value>
  </property>
  <property>
    <name>ceph.root.dir</name>
    <value>${ceph-fs-dir}</value>
  </property>
  <property>
    <name>ceph.mon.address</name>
    <value>mon1,mon2,mon3</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.ceph.impl</name>
    <value>org.apache.hadoop.fs.ceph.CephFs</value>
  </property>
  <property>
    <name>fs.ceph.impl</name>
    <value>org.apache.hadoop.fs.ceph.CephFileSystem</value>
  </property>
  <property>
    <name>ceph.auth.keyring</name>
    <value>${client-keyring-file}</value>
  </property>
</configuration>
```

{% endnavtab %}
{% endnavtabs %}

## Running Alluxio Locally with CephFS

Once you have configured Alluxio to CephFS, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

{% navtabs Test %}
{% navtab cephfs %}

Issue the following command to use the ufs cephfs:

```shell
$ ./bin/alluxio fs mkdir /mnt/cephfs
$ ./bin/alluxio fs mount /mnt/cephfs cephfs://mon1\;mon2\;mon3/
```

Run a simple example program:

```shell
$ ./bin/alluxio runTests --path cephfs://mon1\;mon2\;mon3/
```

Visit your cephfs to verify the files and directories created by Alluxio exist. You should see files named like:

```
${cephfs-dir}/default_tests_files/Basic_CACHE_THROUGH
```
In cephfs, you can visit cephfs with ceph-fuse or mount by POSIX APIs. [Mounting CephFS](https://docs.ceph.com/en/latest/cephfs/#mounting-cephfs){:target="_blank"}

In Alluxio, you can visit the nested directory in the Alluxio. Alluxio's [Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

```
/mnt/cephfs/default_tests_files/Basic_CACHE_THROUGH
```

{% endnavtab %}
{% navtab cephfs-hadoop %}

Issue the following command to use the ufs cephfs-hadoop:

```shell
$ ./bin/alluxio fs mkdir /mnt/cephfs-hadoop
$ ./bin/alluxio fs mount /mnt/cephfs-hadoop cephfs://mon1\;mon2\;mon3/
```

Run a simple example program:

```shell
$ ./bin/alluxio runTests --path cephfs://mon1\;mon2\;mon3/
```

Visit your cephfs-hadoop to verify the files and directories created by Alluxio exist. You should see files named like:

```
${cephfs-hadoop-dir}/default_tests_files/Basic_CACHE_THROUGH
```
In cephfs, you can visit cephfs with ceph-fuse or mount by POSIX APIs. [Mounting CephFS](https://docs.ceph.com/en/latest/cephfs/#mounting-cephfs){:target="_blank"}

In Alluxio, you can visit the nested directory in the Alluxio. Alluxio's [Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

```
/mnt/cephfs-hadoop/default_tests_files/Basic_CACHE_THROUGH
```

{% endnavtab %}
{% endnavtabs %}

