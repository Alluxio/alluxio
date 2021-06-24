---
layout: global
title: CephFS
nickname: CephFS
group: Storage Integrations
priority: 11
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with CephFS as the under storage system. Alluxio supports
two different implementations of under storage system for [CephFS](https://docs.ceph.com/en/latest/cephfs/):
- [cephfs](https://docs.ceph.com/en/latest/cephfs/api/libcephfs-java/)
- [cephfs-hadoop](https://docs.ceph.com/en/nautilus/cephfs/hadoop/)

## Prerequisites

### Deploy Alluxio binary package
The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

### Install Dependences
According to [ceph packages install](https://docs.ceph.com/en/latest/install/get-packages/) to install below packages:

```
cephfs-java
libcephfs_jni
libcephfs2
```

### Make symbolic links

```console
$ ln -s /usr/lib64/libcephfs_jni.so.1.0.0 /usr/lib64/libcephfs_jni.so
$ ln -s /usr/lib64/libcephfs.so.2.0.0 /usr/lib64/libcephfs.so
$ java_path=`which java | xargs readlink | sed 's#/bin/java##g'`
$ ln -s /usr/share/java/libcephfs.jar $java_path/jre/lib/ext/libcephfs.jar
```

### Download CephFS Hadoop jar

```console
$ curl -o $java_path/jre/lib/ext/hadoop-cephfs.jar -s https://download.ceph.com/tarballs/hadoop-cephfs.jar
```

## Basic Setup

Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties` and `conf/core-site.xml`. If them do not exist, 
create the configuration files from the templates

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
$ cp conf/core-site.xml.template conf/core-site.xml
```

{% navtabs Setup %}
{% navtab Option 1: cephfs %}

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.cephfs.conf.file=<ceph-conf-file>
alluxio.underfs.cephfs.mds.namespace=<ceph-fs-name>
alluxio.underfs.cephfs.mount.point=<ceph-fs-dir>
alluxio.underfs.cephfs.auth.id=<client-id>
alluxio.underfs.cephfs.auth.keyring=<client-keyring-file>
```

{% endnavtab %}
{% navtab Option 2: cephfs-hadoop %}

Modify `conf/alluxio-site.properties` to include:

```properties
alluxio.underfs.hdfs.configuration=${ALLUXIO_HOME}/conf/core-site.xml
```

Modify `conf/core-site.xml` to include:

```properties
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

Start up Alluxio locally to see that everything works.

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

{% navtabs Test %}
{% navtab Option 1: cephfs %}

An CephFS location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's [Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

Issue the following command to use the ufs cephfs:

```
$ ./bin/alluxio fs mkdir /mnt/cephfs
$ ./bin/alluxio fs mount /mnt/cephfs cephfs://mon1\;mon2\;mon3/
```

Run a simple example program:

```console
$ ./bin/alluxio runTests --path cephfs://mon1\;mon2\;mon3/
```

Visit your cephfs to verify the files and directories created by Alluxio exist.

You should see files named like:
In cephfs, you can visit cephfs with ceph-fuse or mount by POSIX APIs. [Mounting CephFS](https://docs.ceph.com/en/latest/cephfs/#mounting-cephfs)

```
${ceph-fs-dir}/default_tests_files/Basic_CACHE_THROUGH
```
In Alluxio, you can visit the nested directory in the Alluxio. Alluxio's [Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

```
/mnt/cephfs/default_tests_files/Basic_CACHE_THROUGH
```

{% endnavtab %}
{% navtab Option 2: cephfs-hadoop %}

An CephFS location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's [Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

Issue the following command to use the ufs cephfs:

```console
$ ./bin/alluxio fs mkdir /mnt/cephfs-hadoop
$ ./bin/alluxio fs mount /mnt/cephfs-hadoop ceph://mon1\;mon2\;mon3/
```

Run a simple example program:

```console
./bin/alluxio runTests --path cephfs://mon1\;mon2\;mon3/
```

Visit your cephfs to verify the files and directories created by Alluxio exist.

You should see files named like:
In cephfs, you can visit cephfs with ceph-fuse or mount by POSIX APIs. [Mounting CephFS](https://docs.ceph.com/en/latest/cephfs/#mounting-cephfs)

```
${ceph-fs-dir}/default_tests_files/Basic_CACHE_THROUGH
```
In Alluxio, you can visit the nested directory in the Alluxio. Alluxio's [Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

```
/mnt/cephfs-hadoop/default_tests_files/Basic_CACHE_THROUGH
```

{% endnavtab %}
{% endnavtabs %}

