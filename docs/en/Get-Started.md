---
layout: global
title: Quick Start Guide
---

This quick start guide goes over how to run Alluxio on a local machine.
The guide will cover the following tasks:

* Download and configure Alluxio
* Validate the Alluxio environment
* Start Alluxio locally
* Perform basic tasks via Alluxio Shell
* **[Bonus]** Mount a public Amazon S3 bucket in Alluxio
* Stop Alluxio

**[Bonus]** This guide contains optional tasks that use credentials from an
[AWS account with an access key id and secret access key](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html).
The optional sections will be labeled with **[Bonus]**.

**Note**: This guide is designed to start an Alluxio system with minimal setup on a single machine.
If you are trying to speedup SQL analytics, you can try the
[Presto Alluxio Getting Started](https://www.alluxio.io/alluxio-presto-sandbox-docker/) tutorial.

## Prerequisites

* MacOS or Linux
* [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* Enable remote login: see [instructions for MacOS users](http://osxdaily.com/2011/09/30/remote-login-ssh-server-mac-os-x/)
* **[Bonus]** AWS account and keys

## Downloading Alluxio

Download Alluxio from [this page](https://alluxio.io/downloads/). Select the
desired release followed by the distribution built for default Hadoop.
Unpack the downloaded file with the following commands.

```shell
$ tar -xzf alluxio-{{site.ALLUXIO_VERSION_STRING}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_VERSION_STRING}}
```

This creates a directory `alluxio-{{site.ALLUXIO_VERSION_STRING}}` with all of the Alluxio
source files and Java binaries. Through this tutorial, the path of this directory will be referred
to as `${ALLUXIO_HOME}`.

## Configuring Alluxio

In the `${ALLUXIO_HOME}/conf` directory, create the `conf/alluxio-env.sh` configuration
file by copying the template file.

```shell
$ cp conf/alluxio-env.sh.template conf/alluxio-env.sh
```

In `conf/alluxio-env.sh`, adds configuration for `JAVA_HOME`. For example:

```shell
$ echo "JAVA_HOME=/path/to/java/home" >> conf/alluxio-env.sh
```

In the `${ALLUXIO_HOME}/conf` directory, create the `conf/alluxio-site.properties` configuration
file by copying the template file.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Set `alluxio.master.hostname` in `conf/alluxio-site.properties` to `localhost`.

```shell
$ echo "alluxio.master.hostname=localhost" >> conf/alluxio-site.properties
```

Set additional parameters in `conf/alluxio-site.properties`
```shell
$ echo "alluxio.dora.client.read.location.policy.enabled=true" >> conf/alluxio-site.properties
$ echo "alluxio.user.short.circuit.enabled=false" >> conf/alluxio-site.properties
$ echo "alluxio.master.worker.register.lease.enabled=false" >> conf/alluxio-site.properties
$ echo "alluxio.worker.block.store.type=PAGE" >> conf/alluxio-site.properties
$ echo "alluxio.worker.page.store.type=LOCAL" >> conf/alluxio-site.properties
$ echo "alluxio.worker.page.store.sizes=1GB" >> conf/alluxio-site.properties
$ echo "alluxio.worker.page.store.page.size=1MB" >> conf/alluxio-site.properties
```
Set the page store directories to an existing directory which the current user has read/write permissions to.
The following uses `/mnt/ramdisk` as an example. 
```shell
$ echo "alluxio.worker.page.store.dirs=/mnt/ramdisk" >> conf/alluxio-site.properties
```
The [paging cache storage guide]({{ '/en/core-services/Data-Caching.html' | relativize_url }}#paging-worker-storage) has more information about how to configure page block store.

Configure Alluxio ufs:
```shell
$ echo "alluxio.dora.client.ufs.root=/tmp" >> conf/alluxio-site.properties
```

`<UFS_URI>` should be a full ufs uri. This can be set to a local folder (e.g. default value `/tmp`)
in a single node deployment  or a full ufs uri (e.g.`hdfs://namenode:port/path/` or `s3://bucket/path`).

### [Bonus] Configuration for AWS

To configure Alluxio to interact with Amazon S3, add AWS access information to the Alluxio configuration in `conf/alluxio-site.properties`.

```shell
$ echo "alluxio.dora.client.ufs.root=s3://<BUCKET_NAME>/<DIR>" >> conf/alluxio-site.properties
$ echo "s3a.accessKeyId=<AWS_ACCESS_KEY_ID>" >> conf/alluxio-site.properties
$ echo "s3a.secretKey=<AWS_SECRET_ACCESS_KEY>" >> conf/alluxio-site.properties
```

Replace **`s3://<BUCKET_NAME>/<DIR>`**, **`<AWS_ACCESS_KEY_ID>`** and **`<AWS_SECRET_ACCESS_KEY>`** with
a valid AWS S3 address, AWS access key ID and AWS secret access key respectively.

For more information, please refer to the [S3 configuration docs]({{ '/en/ufs/S3.html' | relativize_url }}.

### [Bonus] Configuration for HDFS

To configure Alluxio to interact with HDFS, provide the path to HDFS configuration files available locally on each node in `conf/alluxio-site.properties`.

```shell
$ echo "alluxio.dora.client.ufs.root=hdfs://nameservice/<DIR>" >> conf/alluxio-site.properties
$ echo "alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/core-site.xml:/path/to/hdfs/conf/hdfs-site.xml" >> conf/alluxio-site.properties
```

Replace the url and configuration with the actual value.

For more information, please refer to the [HDFS configuration docs]({{ '/en/ufs/HDFS.html' | relativize_url }}.

## Starting Alluxio

Alluxio needs to be formatted before starting the process. The following command formats
the Alluxio journal and worker storage directories.

```shell
$ ./bin/alluxio format
```

Start the Alluxio services

```shell
$ ./bin/alluxio-start.sh master
$ ./bin/alluxio-start.sh worker SudoMount
```

Congratulations! Alluxio is now up and running!

## Using the Alluxio Shell

The [Alluxio shell]({{ '/en/operation/User-CLI.html' | relativize_url }}) provides
command line operations for interacting with Alluxio. To see a list of filesystem operations, run

```shell
$ ./bin/alluxio fs
```

List files in Alluxio with the `ls` command. To list all files in the root directory, use the
following command:

```shell
$ ./bin/alluxio fs ls /
```

At this moment, there are no files in Alluxio. Copy a file into Alluxio by using the
`copyFromLocal` shell command.

```shell
$ ./bin/alluxio fs copyFromLocal ${ALLUXIO_HOME}/LICENSE /LICENSE
Copied file://${ALLUXIO_HOME}/LICENSE to /LICENSE
```

List the files in Alluxio again to see the `LICENSE` file.

```shell
$ ./bin/alluxio fs ls /
-rw-r--r-- staff  staff     27040     02-17-2021 16:21:11:061 0% /LICENSE
```

The output shows the file has been written to Alluxio under storage successfully.
Check the directory set as the value of `alluxio.dora.client.ufs.root`, which is `/tmp` by default.

```shell
$ ls /tmp
LICENSE
```

The `cat` command prints the contents of the file.

```shell
$ ./bin/alluxio fs cat /LICENSE
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
...
```

When the file is read, it will also be cached by Alluxio to speed up future data access.

## Stopping Alluxio

Stop Alluxio with the following command:

```shell
$ ./bin/alluxio-stop.sh master
$ ./bin/alluxio-stop.sh worker
```

## Conclusion

Congratulations on completing the quick start guide for Alluxio! This guide covered how to
download and install Alluxio locally with examples of basic interactions via the Alluxio
shell. This was a simple example on how to get started with Alluxio.

There are several next steps available. Learn more about the various features of Alluxio in
our documentation. The resources below detail deploying Alluxio in various ways,
mounting existing storage systems, and configuring existing applications to interact with Alluxio.

## FAQ

### Why do I keep getting "Operation not permitted" for ssh and alluxio?

For the users who are using macOS 11(Big Sur) or later, when running the command
```shell
$ ./bin/alluxio format
```
you might get the error message:
```
alluxio-{{site.ALLUXIO_VERSION_STRING}}/bin/alluxio: Operation not permitted
```
This can be caused by the newly added setting options to macOS.
To fix it, open `System Preferences` and open `Sharing`.

![macOS System Preferences Sharing]({{ '/img/screenshot_sharing_setting.png' | relativize_url }})

On the left, check the box next to `Remote Login`. If there is `Allow full access to remote users` as shown in the
image, check the box next to it. Besides, click the `+` button and add yourself to the list of users that are allowed
for Remote Login if you are not already in it.

## Tuning

### Optional Dora Server-side Metadata Cache

By default, Dora worker caches metadata and data.
Set `alluxio.dora.client.metadata.cache.enabled` to `false` to disable the metadata cache.
If disabled, client will always fetch metadata from under storage directly.

### High performance data transmission over Netty

Set `alluxio.user.netty.data.transmission.enabled` to `true` to enable transmission of data between clients and
Dora cache nodes over Netty. This avoids serialization and deserialization cost of gRPC, as well as consumes less
resources on the worker side.

## Known limitations

1. Only one UFS is supported by Dora. Nested mounts are not supported yet.
1. The Alluxio Master node still needs to be up and running. It is used for Dora worker discovery,
   cluster configuration updates, as well as handling write I/O operations.
1. Alluxio Fuse is not supported with Dora on Kubernetes with the existing helm chart. The helm chart
   supporting Alluxio Fuse is under development.
