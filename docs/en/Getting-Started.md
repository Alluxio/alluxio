---
layout: global
title: Quick Start Guide
group: Home
priority: 3
---

* Table of Contents
{:toc}

In this quick start guide, Alluxio will be installed on a local machine. 
The guide will cover the following tasks:

* Download and configure Alluxio
* Validating Alluxio environment
* Start Alluxio locally
* Perform basic tasks via Alluxio Shell
* **[Bonus]** Mount a public Amazon S3 bucket in Alluxio
* Stop Alluxio

**[Bonus]** This guide contains optional tasks that uses credentials from an
[AWS account with an access key id and secret access key](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html).
The optional sections will be labeled with **[Bonus]**.

**Note**  This guide is designed to start an Alluxio system with minimal setup. Alluxio
performs best in a distributed environment for big data workloads, but this scenario is difficult
to incorporate in a local environment. The performance benefits of Alluxio are illustrated in
the following whitepapers, which include further instructions for running Alluxio in a scaled
environment:
* [Accelerating on-demand data analytics with Alluxio](https://alluxio.com/resources/accelerating-on-demand-data-analytics-with-alluxio)
* [Accelerating data analytics on Ceph object storage with Alluxio](https://www.alluxio.com/blog/accelerating-data-analytics-on-ceph-object-storage-with-alluxio).

## Prerequisites

* Mac OS X or Linux
* [Java 8 or newer](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* **[Bonus]** AWS account and keys

### Setup SSH (Mac OS X only)

For Mac OS X, enable remote login to SSH into localhost. The setting is found in
**System Preferences**, under **Sharing**. Check that **Remote Login** is enabled.

## Downloading Alluxio

Download Alluxio from [this page](http://www.alluxio.org/download). Select the
{{site.ALLUXIO_RELEASED_VERSION}} release followed by the distribution built for default Hadoop.
Unpack the downloaded file with the following commands. The filename differs depending on which
pre-built binary was downloaded.

```bash
$ tar -xzf alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_RELEASED_VERSION}}
```

This creates a directory `alluxio-{{site.ALLUXIO_RELEASED_VERSION}}` with all of the Alluxio
source files and Java binaries. Through this tutorial, the path of this directory will be referred
to as `${ALLUXIO_HOME}`.

## Configuring Alluxio

In the `${ALLUXIO_HOME}/conf` directory, create the `conf/alluxio-site.properties` configuration
file by copying the template file.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Update `alluxio.master.hostname` in `conf/alluxio-site.properties` to `localhost` to inform Alluxio
the hostname of the machine running the Alluxio master.

```bash
$ echo "alluxio.master.hostname=localhost" >> conf/alluxio-site.properties
```

### [Bonus] Configuration for AWS

To configure Alluxio to interact with Amazon S3, add AWS access information to the Alluxio
configuration in `conf/alluxio-site.properties`. The following commands update the
configuration.

```bash
$ echo "aws.accessKeyId=<AWS_ACCESS_KEY_ID>" >> conf/alluxio-site.properties
$ echo "aws.secretKey=<AWS_SECRET_ACCESS_KEY>" >> conf/alluxio-site.properties
```

Replace **`<AWS_ACCESS_KEY_ID>`** and **`<AWS_SECRET_ACCESS_KEY>`** with 
a valid AWS access key ID and AWS secret access key respectively.

## Validating Alluxio environment

Alluxio provides commands to ensure the system environment is ready for running Alluxio services.
Run the following command to validate the environment for running Alluxio locally:

```bash
$ ./bin/alluxio validateEnv local
```

This reports potential problems that might prevent Alluxio from starting locally. For Alluxio
deployed on a cluster of nodes, validate the environment across all nodes by running:

```bash
$ ./bin/alluxio validateEnv all
```

Check out [this page]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}) for detailed
usage information regarding this command.

## Starting Alluxio

Alluxio needs to be formatted before starting the process. The following command formats
the Alluxio journal and worker storage directories.

```bash
$ ./bin/alluxio format
```

By default, Alluxio is configured to start a master and worker process when running locally.
Start Alluxio on localhost with the following command:

```bash
$ ./bin/alluxio-start.sh local SudoMount
```

Congratulations! Alluxio is now up and running! Visit
[http://localhost:19999](http://localhost:19999) and [http://localhost:30000](http://localhost:30000)
to see the status of the Alluxio master and worker respectively.

## Using the Alluxio Shell

The [Alluxio shell]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}) provides
command line operations for interacting with Alluxio. Examine possible operations to interact with
the Alluxio file system with the following command:

```bash
$ ./bin/alluxio fs
```

List files in Alluxio with the `ls` command. To list all files in the root directory, use the
following command:

```bash
$ ./bin/alluxio fs ls /
```

At this moment, there are no files in Alluxio. Copy a file into Alluxio by using the
`copyFromLocal` shell command.

```bash
$ ./bin/alluxio fs copyFromLocal LICENSE /LICENSE
Copied LICENSE to /LICENSE
```

List the files in Alluxio again to see the `LICENSE` file.

```bash
$ ./bin/alluxio fs ls /
-rw-r--r-- staff  staff     26847 NOT_PERSISTED 01-09-2018 15:24:37:088 100% /LICENSE
```

The output shows the file that exists in Alluxio, as well the size of the file, the date it was
created, the owner and group of the file, and the percentage of this file in Alluxio. TODO: what does this percentage represent???

The `cat` command prints the contents of the file.

```bash
$ ./bin/alluxio fs cat /LICENSE
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
...
```

With the default configuration, Alluxio uses the local file system as its under file storage (UFS). The
default path for the UFS is `./underFSStorage`. Examine the contents of the UFS with:

```bash
$ ls ./underFSStorage/
```

Note that the directory does not exist. This is because Alluxio is currently writing data only into
Alluxio space, not to the UFS.

Instruct Alluxio to persist the file from Alluxio space to the UFS by using the `persist` command.

```bash
$ ./bin/alluxio fs persist /LICENSE
persisted file /LICENSE with size 26847
```

The file should appear when examining the UFS path again.

```bash
$ ls ./underFSStorage
LICENSE
```

The LICENSE file also appears in the Alluxio file system through the
[master's web UI](http://localhost:19999/browse). Here, the **Persistence State** column
shows the file as **PERSISTED**.

## [Bonus] Mounting in Alluxio

Alluxio unifies access to storage systems with the unified namespace feature. Read the [Unified
Namespace blog post](http://www.alluxio.com/2016/04/unified-namespace-allowing-applications-to-access-data-anywhere/)
and the [unified namespace documentation]({{site.baseurl}}{% link en/advanced/Namespace-Management.md %}) for more detailed
explanations of the feature.

This feature allows users to mount different storage systems into the Alluxio namespace and
access the files across various storage systems through the Alluxio namespace seamlessly.

Create a directory in Alluxio to store our mount points.

```bash
$ ./bin/alluxio fs mkdir /mnt
Successfully created directory /mnt
```

Mount an existing S3 bucket to Alluxio. This guide uses the `alluxio-quick-start` S3 bucket.

```bash
$ ./bin/alluxio fs mount --readonly alluxio://localhost:19998/mnt/s3 s3a://alluxio-quick-start/data
Mounted s3a://alluxio-quick-start/data at alluxio://localhost:19998/mnt/s3
```

List the files mounted from S3 through the Alluxio namespace by using the `ls` command.

```bash
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

```bash
$ ./bin/alluxio fs ls -R /
-rw-r--r-- staff  staff     26847 PERSISTED 01-09-2018 15:24:37:088 100% /LICENSE
drwxr-xr-x staff  staff         1 PERSISTED 01-09-2018 16:05:59:547  DIR /mnt
dr-x------ staff  staff         4 PERSISTED 01-09-2018 16:34:55:362  DIR /mnt/s3
-r-x------ staff  staff    955610 PERSISTED 01-09-2018 16:35:00:882   0% /mnt/s3/sample_tweets_1m.csv
-r-x------ staff  staff  10077271 PERSISTED 01-09-2018 16:35:00:910   0% /mnt/s3/sample_tweets_10m.csv
-r-x------ staff  staff     89964 PERSISTED 01-09-2018 16:35:00:972   0% /mnt/s3/sample_tweets_100k.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

This shows all the files under the root of the Alluxio file system from all of the mounted storage
systems. The `/LICENSE` file is from the local file system whereas the files under `/mnt/s3/` are
from S3.

## [Bonus] Accelerating Data Access with Alluxio

Since Alluxio leverages memory to store data, it can accelerate access to data. Check the status
of a file previously mounted from S3 in Alluxio.

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

The output shows that the file is **Not In Memory**. This file is a sample of tweets.
Count the number of tweets with the word "kitten" and time the duration of the operation.

```bash
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

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002 100% /mnt/s3/sample_tweets_150m.csv
```

The output shows that the file is now 100% loaded to Alluxio, so reading the file should be
significantly faster.

Now count the number of tweets with the word "puppy".

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c puppy
1553

real	0m1.917s
user	0m2.306s
sys	0m0.243s
```

Subsequent reads of the same file are noticeably faster since the data is stored in Alluxio
memory. 

Now count how many tweets mention the word "bunny".

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c bunny
907

real	0m1.983s
user	0m2.362s
sys	0m0.240s
```

Congratulations! You installed Alluxio locally and used Alluxio to accelerate access to data!

## Stopping Alluxio

Stop Alluxio with the following command:

```bash
$ ./bin/alluxio-stop.sh local
```

## Conclusion

Congratulations on completing the quick start guide for Alluxio! This guide covered how to 
download and install Alluxio locally and examples of basic interactions via the Alluxio
shell. This was a simple example on how to get started with Alluxio.

There are several next steps available. Learn more about the various features of Alluxio in
our documentation. The resources below details deploying Alluxio in various ways,
mounting existing storage systems, and configuring existing applications to interact with Alluxio.

### Deploying Alluxio

Alluxio can be deployed in many different environments.

* [Alluxio on Local Machine]({{site.baseurl}}{% link en/deploy/Running-Alluxio-Locally.md %})
* [Alluxio Standalone on a Cluster]({{site.baseurl}}{% link en/deploy/Running-Alluxio-On-a-Cluster.md %})
* [Alluxio on Docker]({{site.baseurl}}{% link en/deploy/Running-Alluxio-On-Docker.md %})
* [Alluxio with Mesos on EC2]({{site.baseurl}}{% link en/deploy/Running-Alluxio-On-Mesos.md %})
* [Alluxio On YARN]({{site.baseurl}}{% link en/deploy/Running-Alluxio-On-Yarn.md %})

### Under Storage Systems

Various under storage systems can be accessed through Alluxio.

* [Alluxio with Azure Blob Store]({{site.baseurl}}{% link en/ufs/Azure-Blob-Store.md %})
* [Alluxio with S3]({{site.baseurl}}{% link en/ufs/S3.md %})
* [Alluxio with GCS]({{site.baseurl}}{% link en/ufs/GCS.md %})
* [Alluxio with Minio]({{site.baseurl}}{% link en/ufs/Minio.md %})
* [Alluxio with Ceph]({{site.baseurl}}{% link en/ufs/Ceph.md %})
* [Alluxio with Swift]({{site.baseurl}}{% link en/ufs/Swift.md %})
* [Alluxio with GlusterFS]({{site.baseurl}}{% link en/ufs/GlusterFS.md %})
* [Alluxio with MapR-FS]({{site.baseurl}}{% link en/ufs/MapR-FS.md %})
* [Alluxio with HDFS]({{site.baseurl}}{% link en/ufs/HDFS.md %})
* [Alluxio with OSS]({{site.baseurl}}{% link en/ufs/OSS.md %})
* [Alluxio with NFS]({{site.baseurl}}{% link en/ufs/NFS.md %})

### Frameworks and Applications

Different frameworks and applications work with Alluxio.

* [Apache Spark with Alluxio]({{site.baseurl}}{% link en/compute/Spark.md %})
* [Apache Hadoop MapReduce with Alluxio]({{site.baseurl}}{% link en/compute/Hadoop-MapReduce.md %})
* [Apache HBase with Alluxio]({{site.baseurl}}{% link en/compute/HBase.md %})
* [Apache Hive with Alluxio]({{site.baseurl}}{% link en/compute/Hive.md %})
* [Presto with Alluxio]({{site.baseurl}}{% link en/compute/Presto.md %})
