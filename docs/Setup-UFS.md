---
layout: global
title: Under File Systems
---

Tachyon can run with different under file systems. This guide describes how to accomplish the
following:

-   Configure Tachyon with different supported under file systems.
-   Add new under file systems.

# Setup supported under file systems

## HDFS

The additional prerequisite for this part is [Hadoop HDFS](http://www.michael-noll.com/tutorials
/running-hadoop-on-ubuntu-linux-multi-node-cluster/). By default, Tachyon is set to use HDFS version
1.0.4. You can use another Hadoop version by changing the hadoop.version tag in pom.xml in Tachyon
and recompiling it. You can also set the hadoop version when compiling with maven:

    $ mvn -Dhadoop.version=2.2.0 clean package

Edit `tachyon-env.sh` file. Setup `TACHYON_UNDERFS_ADDRESS=hdfs://HDFS_HOSTNAME:HDFS_IP`. You may
also need to setup `JAVA_HOME` in the same file.

## Amazon S3

Edit `tachyon-env.sh` file. Setup `TACHYON_UNDERFS_ADDRESS=s3n://S3_BUCKET/s3_directory` and the
necessary credentials such as `fs.s3n.awsAccessKeyId` and `fs.s3n.awsSecretAccessKey` under
`TACHYON_JAVA_OPTS`. You may also need to setup `JAVA_HOME` in the same file. Make sure that you set
the underfs address to a directory and bucket that already exist (you can create new buckets and
directories from the S3 web interface).

Some users have run into additional issues getting Tachyon working with S3. The `hadoop-client`
package requires the `jets3t` package to use S3, but for some reason doesn't pull it in as a
depedency. One way to fix this is to repackage Tachyon, adding jets3t as a dependency. For example,
the following should work with hadoop version 2.3.0, although depending on your version of Hadoop,
you may need an older version of jets3t:

```xml
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
```

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

# Add other under file system

Besides the above under file system, Tachyon can run on top of other under file system. In order
to do so, a user should implement this
[UnderFileSystem](https://github.com/amplab/tachyon/blob/master/core/src/main/java/tachyon/UnderFileSystem.java)
interface. Here are the implementations of
[HDFS](https://github.com/amplab/tachyon/blob/master/core/src/main/java/tachyon/UnderFileSystemHdfs.java)
and [Local File System](https://github.com/amplab/tachyon/blob/master/core/src/main/java/tachyon/UnderFileSystemSingleLocal.java).
