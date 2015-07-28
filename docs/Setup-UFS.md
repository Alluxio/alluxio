---
layout: global
title: Configure Under Storage System
nickname: Configure Under Storage System
group: More
---

### toc
* [HDFS](#hdfs)
* [Amazon S3](#amazon-s3)
* [GlusterFS](#glusterfs)
    * [Prerequisites](#prerequisites)
    * [Mount GlusterFS](#mount-a-glusterfs-filesystem)
    * [Configure Tachyon](#configure-tachyon-to-use-glusterfs-filesystem)
    * [Format](#format-the-filesystem)
    * [Verify](#verify-that-glusterfs-is-ready-to-use)
* [Swift & IBM SoftLayer](#openstack-swift-and-ibm-softlayer-object-store-alpha)
    * [Configuration](#configuration)
    * [Local file system usage](#local-file-system-usage)
    * [Accessing IBM SoftLayer](#accessing-ibm-softlayer-object-store)
* [Add other UFS](#add-other-underlayer-storage-system)

Tachyon can run with different underlayer storage systems. This guide describes how to accomplish
the following:

-   Configure Tachyon with different supported under file systems.
-   Add new under file systems.

# Setup supported underlayer storage systems

## HDFS

The additional prerequisite for this part is [Hadoop HDFS](http://www.michael-noll.com/tutorials
/running-hadoop-on-ubuntu-linux-multi-node-cluster/). By default, Tachyon is set to use HDFS version
1.0.4. You can use another Hadoop version by changing the hadoop.version tag in pom.xml in Tachyon
and recompiling it. You can also set the hadoop version when compiling with maven:

    $ mvn -Dhadoop.version=2.2.0 clean package

Edit `tachyon-env.sh` file. Setup `TACHYON_UNDERFS_ADDRESS=hdfs://HDFS_HOSTNAME:HDFS_PORT`. You may
also need to setup `JAVA_HOME` in the same file.

## Amazon S3

Edit `tachyon-env.sh` file. Setup `TACHYON_UNDERFS_ADDRESS=s3n://S3_BUCKET/s3_directory` and the
necessary credentials such as `fs.s3n.awsAccessKeyId` and `fs.s3n.awsSecretAccessKey` under
`TACHYON_JAVA_OPTS`. You may also need to setup `JAVA_HOME` in the same file. Make sure that you set
the underfs address to a directory and bucket that already exist (you can create new buckets and
directories from the S3 web interface).

Tachyon comes with a native S3 client and the option to use the S3 client provided by hadoop. To
enable the native S3 client, include the `underfs-s3` module and exclude the `underfs-hdfs` module.
Alternatively, modify `tachyon.underfs.hadoop.prefixes` to exclude `s3n://`. To use the hadoop
provided client, include the `underfs-hdfs` module and exclude the `underfs-s3` module.

Some users have run into additional issues getting Tachyon working with S3. The `hadoop-client`
package requires the `jets3t` package to use S3, but for some reason doesn't pull it in as a
depedency. One way to fix this is to repackage Tachyon, adding jets3t as a dependency. For example,
the following should work with hadoop version 2.3.0, although depending on your version of Hadoop,
you may need an older version of jets3t:

    <dependency>
      <groupId>net.java.dev.jets3t</groupId>
      <artifactId>jets3t</artifactId>
      <version>0.9.0</version>
      <exclusions>
        <exclusion>
          <groupId>commons-codec</groupId>
          <artifactId>commons-codec</artifactId>
          <!-- <version>1.3</version> -->
        </exclusion>
      </exclusions>
    </dependency>

If you are using a Tachyon client that is running separately from the Tachyon master/workers (in a
seperate JVM), then you need to make sure that your AWS credentials are provided to the Tachyon
client JVM processes as well. The easiest way to do this is to add them add them as command line
options when starting your client JVM process. For example:

```
$ java -Xmx3g -Dfs.s3n.awsAccessKeyId=MY_ACCESS_KEY -Dfs.s3n.awsSecretAccessKey=MY_SECRET_KEY -cp my_jar_path.jar com.MyClass myArgs
```

## GlusterFS

### Prerequisites

You need to install [GlusterFS](http://www.gluster.org) on your cluster and create a GlusterFS
volume [GlusterFS usage](http://www.gluster.org/community/documentation/index.php/QuickStart).

Compile Tachyon with GlusterFS: `mvn clean install -Dtest.profile=glusterfs -Dhadoop.version=2.3.0
-Dtachyon.underfs.glusterfs.mounts=/vol -Dtachyon.underfs.glusterfs.volumes=testvol `, where /vol is
a valid GlusterFS mount point.

### Mount a GlusterFS filesystem

Assume the GlusterFS bricks are co-located with Tachyon nodes, the GlusterFS volume name is `gsvol`,
and the mount point is `/vol`.

On each Tachyon node, edit `/etc/fstab` and add `localhost:gsvol /vol glusterfs`. Then mount the
GlusterFS filesystem:

    $ mount -a -t glusterfs

### Configure Tachyon to use GlusterFS filesystem

Next, config Tachyon in `tachyon` folder:

    $ cd /root/tachyon/conf
    $ cp tachyon-glusterfs-env.sh.template tachyon-env.sh

Add the following lines to the `tachyon-env.sh` file:

    TACHYON_UNDERFS_GLUSTER_VOLUMES=tachyon_vol
    TACHYON_UNDERFS_GLUSTER_MOUNTS=/vol

Sync the configuration to all nodes.

### Format the filesystem

    $ cd /root/tachyon/
    $ ./bin/tachyon format

### Verify that GlusterFS is ready to use

    $ cd /vol
    $ ls

You should find that a tachyon folder has been created.


## OpenStack Swift and IBM SoftLayer object store (Alpha)

Swift module depends on Hadoop version 2.3.0 or later and can be configured via
`hadoop-openstack.version` in the main pom.xml. It is strongly advised to compile Tachyon with
the same Hadoop version as `hadoop-openstack.version`. For example, to compile Tachyon with Swift
and Hadoop 2.4.0 use.

    $ mvn -Dhadoop.version=2.4.0 clean package

### Configuration

After the built is successful, there is need to configure `core-sites.xml` required by the Swift
driver. The configuration template included in the `/conf/core-sites.xml.template` and it contains
three example sections: local Swift based on Keystone authentication model, local Swift based on
temp authentication model, and SoftLayer public object store. The general structure of the parameters
are `fs.swift.service.<PROFILE>.<PARAMETER>` where `<PROFILE>` is any name that will be later used
as a part of the Swift URL. For example, if `<PROFILE>` is “swift1” then the Swift URL would be

    swift://<SWIFT CONTAINER>.swift1/

Edit `core-sites.xml` and update `fs.swift.service.<PROFILE>.auth.url`. For Swift based on temp auth
or SoftLayer, update:

    fs.swift.service.<PROFILE>.apikey, fs.swift.service.<PROFILE>.username

For Keystone update:

    fs.swift.service.<PROFILE>.region, fs.swift.service.<PROFILE>.tenant,
    fs.swift.service.<PROFILE>.password, fs.swift.service.<PROFILE>.username

We also need to configure `TACHYON_UNDERFS_ADDRESS` so that Tachyon will use Swift file system.
For example

    TACHYON_UNDERFS_ADDRESS=swift://testcont.swift1

Where `testcont` is the existing Swift container and `swift1` is a profile in `core-sites.xml`.
By default, Tachyon uses `/conf` directory to load `core-sites.xml`. To specify a different location,
please configure `tachyon.underfs.hadoop.configuration` accordingly.

### Local file system usage

By default, Tachyon uses local file system to store certain temporary files. In deployments where
local file system does not exist, both `tachyon.master.journal.folder` and
`tachyon.master.temporary.folder` have to be configured with `swift://` name space.

### Accessing IBM SoftLayer object store

Using the Swift module also makes the IBM SoftLayer object store an option as an under storage system for
Tachyon. To access SoftLayer there is an additional preliminary step. Up to date, hadoop-openstack
implements Keystone authentication model, which is not suitable for SoftLayer object store. There is
a pending [patch](https://issues.apache.org/jira/browse/HADOOP-10420) to extend hadoop-openstack
project with additional type of authentication, which is also good for accessing SoftLayer object
store.  As a temporary solution we would like to explain how to apply this patch on Hadoop and then
build Tachyon with enablement to access SoftLayer object store. Hadoop can be any version from 2.4.0
and up.

Hadoop patch for SoftLayer object store. We demonstrate version 2.4.0

1.	Download hadoop-2.4.0-src.tar.gz and extract it under hadoop-2.4.0 folder
2.	Download https://issues.apache.org/jira/secure/attachment/12662347/HADOOP-10420-007.patch and save it under hadoop-2.4.0/ folder.
3.	Check that changes are visible and can be applied, by executing: git apply --stat HADOOP-10420-007.patch or git apply --check HADOOP-10420-007.patch from /hadoop-2.4.0 folder.
4.	Apply the path git apply HADOOP-10420-007.patch
5.	Navigate to /hadoop-2.4.0/hadoop-tools/hadoop-openstack folder and execute: `mvn -DskipTests install`
6.	Build Tachyon as described above.

## Add other underlayer storage system

Besides the above under file system, Tachyon can run on top of other under file system. In order
to do so, a user should create a new underfs submodule with a
[UnderFileSystem](https://github.com/amplab/tachyon/blob/master/common/src/main/java/tachyon/underfs/UnderFileSystem.java)
interface and a corresponding
[UnderFileSystemFactory](https://github.com/amplab/tachyon/blob/master/common/src/main/java/tachyon/underfs/UnderFileSystemFactory.java).
Finally, a META-INF/services file should be included so the implementation will be registered. The
[HDFS Submodule](https://github.com/amplab/tachyon/tree/master/underfs/hdfs) and
[S3 Submodule](https://github.com/amplab/tachyon/tree/master/underfs/s3) are two examples.
