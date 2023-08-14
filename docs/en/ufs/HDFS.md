---
layout: global
title: HDFS
---


This guide describes the instructions to configure [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html){:target="_blank"} as Alluxio's under storage system. 

HDFS, or Hadoop Distributed File System, is the primary distributed storage used by Hadoop applications, providing reliable and scalable storage for big data processing in Hadoop ecosystems.

For more information about HDFS, please read its [documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html){:target="_blank"}.

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using HDFS with Alluxio:
<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<HDFS_NAMENODE>`</td>
        <td markdown="span">The IP address of the NameNode that processes client connections to the cluster. NameNode is the master node in the Apache Hadoop HDFS Architecture that maintains and manages the blocks present on the DataNodes (slave nodes).</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<HDFS_PORT>`</td>
        <td markdown="span">The port at which the NameNode accepts client connections.</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<HADOOP_VERSION>`</td>
        <td markdown="span"></td>
    </tr>
</table>

## Basic Setup

To configure Alluxio to use HDFS as under storage, you will need to modify the configuration
file `conf/alluxio-site.properties`.
If the file does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify the HDFS namenode and the HDFS port as the underfs address by modifying `conf/alluxio-site.properties`. 

For example, the under storage address can be `hdfs://localhost:8020` if you are running the HDFS namenode locally with default port and
mapping HDFS root directory to Alluxio, or `hdfs://localhost:8020/alluxio/data` if only the HDFS
directory `/alluxio/data` is mapped to Alluxio.

```properties
alluxio.dora.client.ufs.root=hdfs://<HDFS_NAMENODE>:<HDFS_PORT>
```

To find out where HDFS is running, use `hdfs getconf -confKey fs.defaultFS` to get the default hostname
and port HDFS is listening on.

Additionally, you may need to specify the following property to be your HDFS version.
See [mounting HDFS with specific versions]({{ '/en/ufs/HDFS.html' | relativize_url }}#mount-hdfs-with-specific-versions).
```properties
alluxio.underfs.version=<HADOOP_VERSION>
```

## Running Alluxio Locally with HDFS

Once you have configured Alluxio to HDFS, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

## Advanced Setup

### Specify HDFS Configuration Location

When HDFS has non-default configurations, you need to configure Alluxio servers to
access HDFS with the proper configuration file.
Note that once this is set, your applications using
Alluxio client do not need any special configuration.

There are two possible approaches:

- Copy or make symbolic links from `hdfs-site.xml` and
`core-site.xml` from your Hadoop installation into `${ALLUXIO_HOME}/conf`. Make sure
this is set up on all servers running Alluxio.

- Alternatively, you can
set the property `alluxio.underfs.hdfs.configuration` in
`conf/alluxio-site.properties` to point to your `hdfs-site.xml` and `core-site.xml`.
Make sure this configuration is set on all servers running Alluxio.

```properties
alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/core-site.xml:/path/to/hdfs/conf/hdfs-site.xml
```

### HDFS Namenode HA Mode

To configure Alluxio to work with HDFS namenodes in HA mode, first configure Alluxio servers to [access HDFS with the proper configuration files](#specify-hdfs-configuration-location).

In addition, set the under storage address to `hdfs://nameservice/` (`nameservice` is 
the [HDFS nameservice](https://hadoop.apache.org/docs/r3.3.1/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html#Configuration_details){:target="_blank"}
already configured in `hdfs-site.xml`). To mount an HDFS subdirectory to Alluxio instead
of the whole HDFS namespace, change the under storage address to something like
`hdfs://nameservice/alluxio/data`.

```properties
alluxio.dora.client.ufs.root=hdfs://nameservice/
```

### User/Permission Mapping

To ensure that the permission information of files/directories including user, group and mode in
HDFS is consistent with Alluxio (e.g., a file created by user Foo in Alluxio is persisted to
HDFS also with owner as user Foo), the user to start Alluxio master and worker processes
**is required** to be either:

1. [HDFS super user](http://hadoop.apache.org/docs/r3.3.1/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#The_Super-User){:target="_blank"}.
Namely, use the same user that starts HDFS namenode process to also start Alluxio master and
worker processes.

2. A member of [HDFS superuser group](http://hadoop.apache.org/docs/r3.3.1/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#Configuration_Parameters){:target="_blank"}.
Edit HDFS configuration file `hdfs-site.xml` and check the value of configuration property
`dfs.permissions.superusergroup`. If this property is set with a group (e.g., "hdfs"), add the
user to start Alluxio process (e.g., "alluxio") to this group ("hdfs"); if this property is not
set, add a group to this property where your Alluxio running user is a member of this newly added
group.

The user set above is only the identity that starts Alluxio master and worker
processes. Once Alluxio servers are started, it is **unnecessary** to run your Alluxio client
applications using this user.

### Connect to Secure HDFS

If your HDFS cluster is Kerberized, first configure Alluxio servers to [access HDFS with the proper configuration files](#specify-hdfs-configuration-location).

In addition, security configuration is needed for Alluxio to be able to
communicate with the HDFS cluster. Set the following Alluxio properties in `alluxio-site.properties`:

```properties
alluxio.master.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.master.principal=hdfs/<_HOST>@<REALM>
alluxio.worker.keytab.file=<YOUR_HDFS_KEYTAB_FILE_PATH>
alluxio.worker.principal=hdfs/<_HOST>@<REALM>
```

If connecting to secure HDFS, run `kinit` on all Alluxio nodes.
Use the principal `hdfs` and the keytab that you configured earlier in `alluxio-site.properties`.
A known limitation is that the Kerberos TGT may expire after
the max renewal lifetime. You can work around this by renewing the TGT periodically. Otherwise you
may see `No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)`
when starting Alluxio services. Another option is to set `alluxio.hadoop.security.kerberos.keytab.login.autorenewal=true`
so the TGT is automatically refreshed.

The user can also use `alluxio.hadoop.security.krb5.conf` to specify the krb5.conf file location
and use `alluxio.hadoop.security.authentication` to specify authentication method.

#### Custom Kerberos Realm/KDC

By default, Alluxio will use machine-level Kerberos configuration to determine the Kerberos realm
and KDC. You can override these defaults by setting the JVM properties
`java.security.krb5.realm` and `java.security.krb5.kdc`.

To set these, set `ALLUXIO_JAVA_OPTS` in `conf/alluxio-env.sh`.

```sh
ALLUXIO_JAVA_OPTS+=" -Djava.security.krb5.realm=<YOUR_KERBEROS_REALM> -Djava.security.krb5.kdc=<YOUR_KERBEROS_KDC_ADDRESS>"
```

### Mount HDFS with Specific Versions

There are multiple ways for a user to mount an HDFS cluster with a specified version as an under storage into Alluxio namespace.
Before mounting HDFS with a specific version, make sure you have built a client with that specific version of HDFS.
You can check the existence of this client by going to the `lib` directory under the Alluxio directory.

If you have built Alluxio from source, you can build additional client jar files by running `mvn` command under the `underfs` directory in the Alluxio source tree. 
For example, issuing the following command would build the client jar for the 2.8.0 version.

```shell
$ mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests \
    -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true \
    -Pufs-hadoop-2 -Dufs.hadoop.version=2.8.0
```

#### Using Site Properties

When mounting the under storage of Alluxio root directory with a specific HDFS version, one can add the
following line to the site properties file (`conf/alluxio-site.properties`)

```properties
alluxio.dora.client.ufs.root=hdfs://namenode1:8020
alluxio.underfs.version=2.2
```

#### Supported HDFS Versions

Alluxio supports the following versions of HDFS as a valid argument of mount option `alluxio.underfs.version`:

- Apache Hadoop: 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 3.0, 3.1, 3.2, 3.3

Note: Apache Hadoop 1.0 and 1.2 are still supported, but not included in the default download.
To build this module yourself, build the shaded hadoop client and then the UFS module.

### Use Hadoop Native Library

Hadoop comes with a native library that provides better performance and additional features compared to its Java implementation.
For example, when the native library is used, the HDFS client can use native checksum function which is more efficient than the default Java implementation.
To use the Hadoop native library with Alluxio HDFS under filesystem, first install the native library on Alluxio nodes by following the
instructions on [this page](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/NativeLibraries.html){:target="_blank"}.
Once the hadoop native library is installed on the machine, update Alluxio startup Java parameters in `conf/alluxio-env.sh` by adding the following line:

```sh
ALLUXIO_JAVA_OPTS+=" -Djava.library.path=<local_path_containing_hadoop_native_library> "
```

Make sure to restart Alluxio services for the change to take effect.
