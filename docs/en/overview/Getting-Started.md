---
layout: global
title: Quick Start Guide
group: Overview
priority: 4
---

* Table of Contents
{:toc}

This quick start guide goes over how to run Alluxio on a local machine.
The guide will cover the following tasks:

* Download and configure Alluxio
* Validate the Alluxio environment
* Start Alluxio locally
* Perform basic tasks via Alluxio Shell
* **[Bonus]** Mount a public Amazon S3 bucket in Alluxio
* Stop Alluxio

**[Bonus]** This guide contains optional tasks that uses credentials from an
[AWS account with an access key id and secret access key](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html).
The optional sections will be labeled with **[Bonus]**.

**Note**  This guide is designed to start an Alluxio system with minimal setup on a single machine.

If you are trying to speedup SQL analytics, you can try the Presto Alluxio Getting Started tutorial:

<p align="center">
<a href="https://www.alluxio.io/alluxio-presto-sandbox-docker/">
 <img src="https://www.alluxio.io/app/uploads/2019/07/laptop-docker.png" width="250" alt="Laptop with Docker"/></a>
</p>

## Prerequisites

* MacOS or Linux
* [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* Enable remote login: see [instructions for MacOS users](http://osxdaily.com/2011/09/30/remote-login-ssh-server-mac-os-x/)
* **[Bonus]** AWS account and keys

## Downloading Alluxio

Download Alluxio from [this page](https://www.alluxio.io/download). Select the
desired release followed by the distribution built for default Hadoop.
Unpack the downloaded file with the following commands.

```console
$ tar -xzf alluxio-{{site.ALLUXIO_VERSION_STRING}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_VERSION_STRING}}
```

This creates a directory `alluxio-{{site.ALLUXIO_VERSION_STRING}}` with all of the Alluxio
source files and Java binaries. Through this tutorial, the path of this directory will be referred
to as `${ALLUXIO_HOME}`.

## Configuring Alluxio

In the `${ALLUXIO_HOME}/conf` directory, create the `conf/alluxio-site.properties` configuration
file by copying the template file.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Set `alluxio.master.hostname` in `conf/alluxio-site.properties` to `localhost`.

```console
$ echo "alluxio.master.hostname=localhost" >> conf/alluxio-site.properties
```

### [Bonus] Configuration for AWS

To configure Alluxio to interact with Amazon S3, add AWS access information to the Alluxio
configuration in `conf/alluxio-site.properties`. The following commands update the
configuration.

```console
$ echo "aws.accessKeyId=<AWS_ACCESS_KEY_ID>" >> conf/alluxio-site.properties
$ echo "aws.secretKey=<AWS_SECRET_ACCESS_KEY>" >> conf/alluxio-site.properties
```

Replace **`<AWS_ACCESS_KEY_ID>`** and **`<AWS_SECRET_ACCESS_KEY>`** with
a valid AWS access key ID and AWS secret access key respectively.

## Validating Alluxio environment

Alluxio provides commands to ensure the system environment is ready for running Alluxio services.
Run the following command to validate the environment for running Alluxio locally:

```console
$ ./bin/alluxio validateEnv local
```

This reports potential problems that might prevent Alluxio from starting locally.

Check out [this page]({{ '/en/operation/User-CLI.html' | relativize_url }}) for detailed
usage information regarding the `validateEnv` command.

## Starting Alluxio

Alluxio needs to be formatted before starting the process. The following command formats
the Alluxio journal and worker storage directories.

```console
$ ./bin/alluxio format
```

Note that if this command returns failures related to 'ValidateHdfsVersion',
and you are not planning to integrate HDFS to alluxio yet, you can ignore this failure for now.
By default, Alluxio is configured to start a master and worker process when running locally.
Start Alluxio on localhost with the following command:

```console
$ ./bin/alluxio-start.sh local SudoMount
```

Congratulations! Alluxio is now up and running! Visit
[http://localhost:19999](http://localhost:19999) and [http://localhost:30000](http://localhost:30000)
to see the status of the Alluxio master and worker respectively.

## Using the Alluxio Shell

The [Alluxio shell]({{ '/en/operation/User-CLI.html' | relativize_url }}) provides
command line operations for interacting with Alluxio. To see a list of filesystem operations, run

```console
$ ./bin/alluxio fs
```

List files in Alluxio with the `ls` command. To list all files in the root directory, use the
following command:

```console
$ ./bin/alluxio fs ls /
```

At this moment, there are no files in Alluxio. Copy a file into Alluxio by using the
`copyFromLocal` shell command.

```console
$ ./bin/alluxio fs copyFromLocal ${ALLUXIO_HOME}/LICENSE /LICENSE
Copied file://${ALLUXIO_HOME}/LICENSE to /LICENSE
```

List the files in Alluxio again to see the `LICENSE` file.

```console
$ ./bin/alluxio fs ls /
-rw-r--r-- staff  staff     27040       PERSISTED 02-17-2021 16:21:11:061 100% /LICENSE
```

The output shows the file that exists in Alluxio. Each line contains the owner and group of the file,
the size of the file, whether it has been persisted to its under file storage (UFS), the date it was created,
and the percentage of the file that is cached in Alluxio.

The `cat` command prints the contents of the file.

```console
$ ./bin/alluxio fs cat /LICENSE
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
...
```

With the default configuration, Alluxio uses the local file system as its UFS and automatically
persists data to it. The default path for the UFS is `${ALLUXIO_HOME}/underFSStorage`. Examine the contents of the UFS with:

```console
$ ls ${ALLUXIO_HOME}/underFSStorage
LICENSE
```

The LICENSE file also appears in the Alluxio file system through the
[master's web UI](http://localhost:19999/browse). Here, the **Persistence State** column
shows the file as **PERSISTED**.

View the amount of memory currently consumed by data in Alluxio under the **Storage Usage Summary**
on the main page of the [master's web UI](http://localhost:19999), or through the following command.


```console
$ ./bin/alluxio fs getUsedBytes
Used Bytes: 27040
```

This memory can be reclaimed by freeing it from Alluxio. Notice this does not remove
it from the Alluxio filesystem nor the UFS. Rather it is just removed from the cache in Alluxio.

```console
$ ./bin/alluxio fs free /LICENSE
/LICENSE was successfully freed from Alluxio space.

$ ./bin/alluxio fs getUsedBytes
Used Bytes: 0

$ ./bin/alluxio fs ls /
-rw-r--r-- staff  staff     27040       PERSISTED 02-17-2021 16:21:11:061 0% /LICENSE

$ ls ${ALLUXIO_HOME}/underFSStorage/
LICENSE
```

Accessing the data will fetch the file from the UFS and bring it back into
the cache in Alluxio.

```console
$ ./bin/alluxio fs copyToLocal /LICENSE ~/LICENSE.bak
Copied /LICENSE to file:///home/staff/LICENSE.bak

$ ./bin/alluxio fs getUsedBytes
Used Bytes: 27040

$ ./bin/alluxio fs ls /
-rw-r--r-- staff  staff     27040       PERSISTED 02-17-2021 16:21:11:061 100% /LICENSE

$ rm ~/LICENSE.bak
```

## [Bonus] Mounting in Alluxio

Alluxio unifies access to storage systems with the unified namespace feature. Read the [Unified
Namespace blog post](https://www.alluxio.io/resources/whitepapers/unified-namespace-allowing-applications-to-access-data-anywhere/)
and the [unified namespace documentation]({{ '/en/core-services/Unified-Namespace.html' | relativize_url }}) for more detailed
explanations of the feature.

This feature allows users to mount different storage systems into the Alluxio namespace and
access the files across various storage systems through the Alluxio namespace seamlessly.

Create a directory in Alluxio to store our mount points.

```console
$ ./bin/alluxio fs mkdir /mnt
Successfully created directory /mnt
```

Mount an existing S3 bucket to Alluxio. This guide uses the `alluxio-quick-start` S3 bucket.

```console
$ ./bin/alluxio fs mount --readonly alluxio://localhost:19998/mnt/s3 s3://alluxio-quick-start/data
Mounted s3://alluxio-quick-start/data at alluxio://localhost:19998/mnt/s3
```

List the files mounted from S3 through the Alluxio namespace by using the `ls` command.

```console
$ ./bin/alluxio fs ls /mnt/s3
-r-x------ staff  staff    955610 PERSISTED 01-09-2018 16:35:00:882   0% /mnt/s3/sample_tweets_1m.csv
-r-x------ staff  staff  10077271 PERSISTED 01-09-2018 16:35:00:910   0% /mnt/s3/sample_tweets_10m.csv
-r-x------ staff  staff     89964 PERSISTED 01-09-2018 16:35:00:972   0% /mnt/s3/sample_tweets_100k.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

The newly mounted files and directories are also visible in the
[Alluxio web UI](http://localhost:19999/browse?path=%2Fmnt%2Fs3).

With Alluxio's unified namespace, users can interact with data from different storage systems
seamlessly. The `ls -R` command recursively lists all the files that exist under a directory.

```console
$ ./bin/alluxio fs ls -R /
-rw-r--r-- staff  staff     26847 PERSISTED 01-09-2018 15:24:37:088 100% /LICENSE
drwxr-xr-x staff  staff         1 PERSISTED 01-09-2018 16:05:59:547  DIR /mnt
dr-x------ staff  staff         4 PERSISTED 01-09-2018 16:34:55:362  DIR /mnt/s3
-r-x------ staff  staff    955610 PERSISTED 01-09-2018 16:35:00:882   0% /mnt/s3/sample_tweets_1m.csv
-r-x------ staff  staff  10077271 PERSISTED 01-09-2018 16:35:00:910   0% /mnt/s3/sample_tweets_10m.csv
-r-x------ staff  staff     89964 PERSISTED 01-09-2018 16:35:00:972   0% /mnt/s3/sample_tweets_100k.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

This shows all the files across all of the mounted storage
systems. The `/LICENSE` file is from the local file system whereas the files under `/mnt/s3/` are
in S3.

## [Bonus] Accelerating Data Access with Alluxio

Since Alluxio leverages memory to store data, it can accelerate access to data. Check the status
of a file previously mounted from S3 into Alluxio:

```console
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

The output shows that the file is **Not In Memory**. This file is a sample of tweets.
Count the number of tweets with the word "kitten" and time the duration of the operation.

```console
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c kitten
889

real	0m22.857s
user	0m7.557s
sys	0m1.181s
```

Depending on your network connection, the operation may take over 20 seconds. If reading this file
takes too long, use a smaller dataset. The other files in the directory are smaller subsets
of this file. Alluxio can accelerate access to this data by using memory to store the data.

After reading the file by the `cat` command, check the status with the `ls` command:

```console
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002 100% /mnt/s3/sample_tweets_150m.csv
```

The output shows that the file is now 100% loaded to Alluxio, so reading the file should be
significantly faster.

Now count the number of tweets with the word "puppy".

```console
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c puppy
1553

real	0m1.917s
user	0m2.306s
sys	0m0.243s
```

Subsequent reads of the same file are noticeably faster since the data is stored in Alluxio
memory.

Now count how many tweets mention the word "bunny".

```console
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c bunny
907

real	0m1.983s
user	0m2.362s
sys	0m0.240s
```

Congratulations! You installed Alluxio locally and used Alluxio to accelerate access to data!

## Stopping Alluxio

Stop Alluxio with the following command:

```console
$ ./bin/alluxio-stop.sh local
```

## Conclusion

Congratulations on completing the quick start guide for Alluxio! This guide covered how to
download and install Alluxio locally with examples of basic interactions via the Alluxio
shell. This was a simple example on how to get started with Alluxio.

There are several next steps available. Learn more about the various features of Alluxio in
our documentation. The resources below detail deploying Alluxio in various ways,
mounting existing storage systems, and configuring existing applications to interact with Alluxio.

## Next Steps

### Deploying Alluxio

Alluxio can be deployed in many different environments.

* [Alluxio on Local Machine]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }})
* [Alluxio Standalone on a Cluster]({{ '/en/deploy/Running-Alluxio-On-a-Cluster.html' | relativize_url }})
* [Alluxio on Docker]({{ '/en/deploy/Running-Alluxio-On-Docker.html' | relativize_url }})

### Under Storage Systems

Various under storage systems can be accessed through Alluxio.

* [Alluxio with Azure Blob Store]({{ '/en/ufs/Azure-Blob-Store.html' | relativize_url }})
* [Alluxio with S3]({{ '/en/ufs/S3.html' | relativize_url }})
* [Alluxio with GCS]({{ '/en/ufs/GCS.html' | relativize_url }})
* [Alluxio with Minio]({{ '/en/ufs/Minio.html' | relativize_url }})
* [Alluxio with CephFS]({{ '/en/ufs/CephFS.html' | relativize_url }})
* [Alluxio with CephObjectStorage]({{ '/en/ufs/CephObjectStorage.html' | relativize_url }})
* [Alluxio with Swift]({{ '/en/ufs/Swift.html' | relativize_url }})
* [Alluxio with GlusterFS]({{ '/en/ufs/GlusterFS.html' | relativize_url }})
* [Alluxio with HDFS]({{ '/en/ufs/HDFS.html' | relativize_url }})
* [Alluxio with OSS]({{ '/en/ufs/OSS.html' | relativize_url }})
* [Alluxio with NFS]({{ '/en/ufs/NFS.html' | relativize_url }})

### Frameworks and Applications

Different frameworks and applications work with Alluxio.

* [Apache Spark with Alluxio]({{ '/en/compute/Spark.html' | relativize_url }})
* [Apache Hadoop MapReduce with Alluxio]({{ '/en/compute/Hadoop-MapReduce.html' | relativize_url }})
* [Apache HBase with Alluxio]({{ '/en/compute/HBase.html' | relativize_url }})
* [Apache Hive with Alluxio]({{ '/en/compute/Hive.html' | relativize_url }})
* [Presto with Alluxio]({{ '/en/compute/Presto.html' | relativize_url }})

## FAQ

### Why do I keep getting "Operation not permitted" for ssh and alluxio? 

For the users who are using macOS 11(Big Sur) or later, when running the command
```console
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


