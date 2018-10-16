---
layout: global
title: Quick Start Guide
group: Home
priority: 0
---

* Table of Contents
{:toc}

The simplest way to quickly try out Alluxio is to install it locally on a single machine. In this
quick start guide, we will install Alluxio on your local machine, mount example data, and
perform basic tasks with the data in Alluxio. During this guide, you will:

* Download and configure Alluxio
* Validating Alluxio environment
* Start Alluxio locally
* Perform basic tasks via Alluxio Shell
* **[Bonus]** Mount a public Amazon S3 bucket in Alluxio
* Shutdown Alluxio

**[Bonus]** If you have an
[AWS account with an access key id and secret access key](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html),
you will be able to perform additional tasks in this guide. Sections of the guide which require
your AWS account information will be labeled with **[Bonus]**.

**Note**  This guide is meant for you to quickly start interacting with an Alluxio system. Alluxio
performs best in a distributed environment for big data workloads. Both of these qualities are
difficult to incorporate in a local environment. If you are interested in running a larger scale
example which highlights the performance benefits of Alluxio, try out the instructions in either of
these two whitepapers: [Accelerating on-demand data analytics with
Alluxio](https://alluxio.com/resources/accelerating-on-demand-data-analytics-with-alluxio),
[Accelerating data analytics on Ceph object storage with Alluxio](https://www.alluxio.com/blog/accelerating-data-analytics-on-ceph-object-storage-with-alluxio).

## Prerequisites

For the following quick start guide, you will need:

* Mac OS X or Linux
* [Java 8 or newer](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* **[Bonus]** AWS account and keys

### Setup SSH (Mac OS X)

If you are using Mac OS X, you may have to enable the ability to ssh into localhost. To enable remote
login, Open **System Preferences**, then open **Sharing**. Make sure **Remote Login** is enabled.

## Downloading Alluxio

First, [download the Alluxio release](http://www.alluxio.org/download). You can
download the latest {{site.ALLUXIO_RELEASED_VERSION}} release pre-built for various versions of
Hadoop from the [Alluxio download page](http://www.alluxio.org/download).

Next, you can unpack the download with the following commands. Your filename may be different
depending on which pre-built binaries you have downloaded.

```bash
$ tar -xzf alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_RELEASED_VERSION}}
```

This will create a directory `alluxio-{{site.ALLUXIO_RELEASED_VERSION}}` with all of the Alluxio
source files and Java binaries. Through this tutorial, the path of this directory will be referred
to as `${ALLUXIO_HOME}`.

## Configuring Alluxio

Before we start Alluxio, we have to configure it. We will be using most of the default settings.

In the `${ALLUXIO_HOME}/conf` directory, create the `conf/alluxio-site.properties` configuration
file from the template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Update `alluxio.master.hostname` in `conf/alluxio-site.properties` to the hostname of the machine
you plan to run Alluxio Master on.

```bash
$ echo "alluxio.master.hostname=localhost" >> conf/alluxio-site.properties
```

### [Bonus] Configuration for AWS

If you have an Amazon AWS account with your access key id and secret key, you can update your
Alluxio configuration now in preparation for interacting with Amazon S3 later in this guide. Add
your AWS access information to the Alluxio configuration by adding the keys to the
`conf/alluxio-site.properties` file. The following commands will update the configuration.

```bash
$ echo "aws.accessKeyId=<AWS_ACCESS_KEY_ID>" >> conf/alluxio-site.properties
$ echo "aws.secretKey=<AWS_SECRET_ACCESS_KEY>" >> conf/alluxio-site.properties
```

You will have to replace **`<AWS_ACCESS_KEY_ID>`** with your AWS access key id, and
**`<AWS_SECRET_ACCESS_KEY>`** with your AWS secret access key. Now, Alluxio is fully configured for
the rest of this guide.

## Validating Alluxio environment

Before starting Alluxio, you might want to make sure that your system environment is ready for running
Alluxio services. You can run the following command to validate your local environment with your
Alluxio configuration:

```bash
$ ./bin/alluxio validateEnv local
```

This will report potential problems that might prevent you from starting Alluxio services locally. If you configured Alluxio to run in a cluster and you want to validate environment on all nodes, you
can run the following command instead:

```bash
$ ./bin/alluxio validateEnv all
```

You can also make the command run only specific validation task. For example,

```bash
$ ./bin/alluxio validateEnv local ulimit
```

Will only run validation tasks that check your local system resource limits.

You can check out [this page]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}) for detailed usage information regarding this command.

## Starting Alluxio

Next, we will format Alluxio in preparation for starting Alluxio. The following command will format
the Alluxio journal and the worker storage directory in preparation for the master and worker to
start.

```bash
$ ./bin/alluxio format
```

Now, we can start Alluxio! By default, Alluxio is configured to start a master and worker on the
localhost. We can start Alluxio on localhost with the following command:

```bash
$ ./bin/alluxio-start.sh local SudoMount
```

Congratulations! Alluxio is now up and running! You can visit
[http://localhost:19999](http://localhost:19999) to see the status of the Alluxio master, and visit
[http://localhost:30000](http://localhost:30000) to see the status of the Alluxio worker.

## Using the Alluxio Shell

Now that Alluxio is running, we can examine the Alluxio file system with the
[Alluxio shell]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}). The Alluxio shell enables many command-line operations
for interacting with Alluxio. You can invoke the Alluxio shell with the following command:

```bash
$ ./bin/alluxio fs
```

This will print out the available Alluxio command-line operations.

For example, you can list files in Alluxio with the `ls` command. To list all files in the root directory, use the following command:

```bash
$ ./bin/alluxio fs ls /
```

Unfortunately, we do not have any files in Alluxio. We can solve that by copying a file into
Alluxio. The `copyFromLocal` shell command is used to copy a local file into Alluxio.

```bash
$ ./bin/alluxio fs copyFromLocal LICENSE /LICENSE
Copied LICENSE to /LICENSE
```

After copying the `LICENSE` file, we should be able to see it in Alluxio. List the files in
Alluxio with the command:

```bash
$ ./bin/alluxio fs ls /
-rw-r--r-- staff  staff     26847 NOT_PERSISTED 01-09-2018 15:24:37:088 100% /LICENSE
```

The output shows the file that exists in Alluxio, as well as some other useful information, like the
size of the file, the date it was created, the owner and group of the file, and the percentage of
this file in Alluxio.

You can also view the contents of the file through the Alluxio shell. The `cat` command will print
the contents of the file.

```bash
$ ./bin/alluxio fs cat /LICENSE
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
...
```

With the default configuration, Alluxio uses the local file system as its UnderFileSystem (UFS). The
default path for the UFS is `./underFSStorage`. We can see what is in the UFS with:

```bash
$ ls ./underFSStorage/
```

However, the directory doesn't exist! By default, Alluxio will write data only into
Alluxio space, not to the UFS.

However, we can tell Alluxio to persist the file from Alluxio space to the UFS. The shell command
`persist` will do just that.

```bash
$ ./bin/alluxio fs persist /LICENSE
persisted file /LICENSE with size 26847
```

Now, if we examine the local UFS again, the file should appear.

```bash
$ ls ./underFSStorage
LICENSE
```

If we browse the Alluxio file system in the [master's web UI](http://localhost:19999/browse) we can
see the LICENSE file as well as other useful information. Here, the **Persistence State** column
shows the file as **PERSISTED**.

## [Bonus] Mounting in Alluxio

Alluxio unifies access to storage systems with the unified namespace feature. Read the [Unified
Namespace blog post](http://www.alluxio.com/2016/04/unified-namespace-allowing-applications-to-access-data-anywhere/)
and the [unified namespace documentation]({{site.baseurl}}{% link en/advanced/Namespace-Management.md %}) for more detailed
explanations of the feature.

This feature allows users to mount different storage systems into the Alluxio namespace and
access the files across various storage systems through the Alluxio namespace seamlessly.

First, we will create a directory in Alluxio to store our mount points.

```bash
$ ./bin/alluxio fs mkdir /mnt
Successfully created directory /mnt
```

Next, we will mount an existing sample S3 bucket to Alluxio. We have provided a sample S3 bucket for
you to use in the rest of this guide.

```bash
$ ./bin/alluxio fs mount --readonly alluxio://localhost:19998/mnt/s3 s3a://alluxio-quick-start/data
Mounted s3a://alluxio-quick-start/data at alluxio://localhost:19998/mnt/s3
```

Now, the S3 bucket is mounted into the Alluxio namespace.

We can list the files from S3, through the Alluxio namespace. We can use the familiar `ls` shell
command to list the files from the S3 mounted directory.

```bash
$ ./bin/alluxio fs ls /mnt/s3
-r-x------ staff  staff    955610 PERSISTED 01-09-2018 16:35:00:882   0% /mnt/s3/sample_tweets_1m.csv
-r-x------ staff  staff  10077271 PERSISTED 01-09-2018 16:35:00:910   0% /mnt/s3/sample_tweets_10m.csv
-r-x------ staff  staff     89964 PERSISTED 01-09-2018 16:35:00:972   0% /mnt/s3/sample_tweets_100k.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

We can see the [newly mounted files and directories in the Alluxio web UI](http://localhost:19999/browse?path=%2Fmnt%2Fs3) as well.

With Alluxio's unified namespace, you can interact with data from different storage systems
seamlessly. For example, with the `ls` shell command, you can recursively list all the files that
exist under a directory.

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

This shows all the files under the root of the Alluxio file system, from all of the mounted storage
systems. The `/LICENSE` file is in your local file system, while the files under `/mnt/s3/` are in
S3.

## [Bonus] Accelerating Data Access with Alluxio

Since Alluxio leverages memory to store data, it can accelerate access to data. First, let’s take a
look at the status of a file in Alluxio (mounted from S3).

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

The output shows that the file is **Not In Memory**. This file is a sample of tweets. Let’s see how
many tweets mention the word "kitten". With the following command, we can count the number of
tweets with "kitten".

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c kitten
889

real	0m22.857s
user	0m7.557s
sys	0m1.181s
```

Depending on your network connection, the operation may take over 20 seconds. If reading this file
takes too long, you may use a smaller dataset. The other files in the directory are smaller subsets
of this file. As you can see, it takes a lot of time to access the data for each command. Alluxio
can accelerate access to this data by using memory to store the data.

After reading the file by the `cat` command, you can check the status with the `ls` command:

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002 100% /mnt/s3/sample_tweets_150m.csv
```

The output shows that the file is now 100% loaded to Alluxio. Now that the file is Alluxio, reading the file
should be much faster now.

Let’s count the number of tweets with the word "puppy".

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c puppy
1553

real	0m1.917s
user	0m2.306s
sys	0m0.243s
```

As you can see, reading the file was very quick, only a few seconds! And, since the data in Alluxio
memory, you can easily read the file again just as quickly. Let’s now count how many tweets mention
the word "bunny".

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c bunny
907

real	0m1.983s
user	0m2.362s
sys	0m0.240s
```

Congratulations! You installed Alluxio locally and used Alluxio to accelerate access to data!

## Stopping Alluxio

Once you are done with interacting with your local Alluxio installation, you can stop Alluxio with
the following command:

```bash
$ ./bin/alluxio-stop.sh local
```

## Conclusion

Congratulations on completing the quick start guide for Alluxio! You have successfully downloaded
and installed Alluxio on your local computer, and performed some basic interactions via the Alluxio
shell. This was a simple example on how to get started with Alluxio.

There are several next steps available. You can learn more about the various features of Alluxio in
our documentation. You can also deploy Alluxio in your environment, mount your existing
storage systems to Alluxio, or configure your applications to work with Alluxio. Additional
resources are below.

### Deploying Alluxio

Alluxio can be deployed in many different environments.

* [Alluxio on Local Machine]({{site.baseurl}}{% link en/deploy/Running-Alluxio-Locally.md %})
* [Alluxio Standalone on a Cluster]({{site.baseurl}}{% link en/deploy/Running-Alluxio-On-a-Cluster.md %})
* [Alluxio on Docker]({{site.baseurl}}{% link en/deploy/Running-Alluxio-On-Docker.md %})
* [Alluxio with Mesos on EC2]({{site.baseurl}}{% link en/deploy/Running-Alluxio-On-Mesos.md %})
* [Alluxio On YARN]({{site.baseurl}}{% link en/deploy/Running-Alluxio-On-Yarn.md %})

### Under Storage Systems

There are many Under storage systems that can be accessed through Alluxio.

* [Alluxio with Azure Blob Store]({{site.baseurl}}{% link en/ufs/Azure-Blob-Store.md %})
* [Alluxio with S3]({{site.baseurl}}{% link en/ufs/S3.md %})
* [Alluxio with GCS]({{site.baseurl}}{% link en/ufs/GCS.md %})
* [Alluxio with Minio]({{site.baseurl}}{% link en/ufs/Minio.md %})
* [Alluxio with Ceph]({{site.baseurl}}{% link en/ufs/Ceph.md %})
* [Alluxio with Swift]({{site.baseurl}}{% link en/ufs/Swift.md %})
* [Alluxio with GlusterFS]({{site.baseurl}}{% link en/ufs/GlusterFS.md %})
* [Alluxio with MapR-FS]({{site.baseurl}}{% link en/ufs/MapR-FS.md %})
* [Alluxio with HDFS]({{site.baseurl}}{% link en/ufs/HDFS.md %})
* [Alluxio with Secure HDFS]({{site.baseurl}}{% link en/ufs/Secure-HDFS.md %})
* [Alluxio with OSS]({{site.baseurl}}{% link en/ufs/OSS.md %})
* [Alluxio with NFS]({{site.baseurl}}{% link en/ufs/NFS.md %})

### Frameworks and Applications

Different frameworks and applications work with Alluxio.

* [Apache Spark with Alluxio]({{site.baseurl}}{% link en/compute/Spark.md %})
* [Apache Hadoop MapReduce with Alluxio]({{site.baseurl}}{% link en/compute/Hadoop-MapReduce.md %})
* [Apache HBase with Alluxio]({{site.baseurl}}{% link en/compute/HBase.md %})
* [Apache Hive with Alluxio]({{site.baseurl}}{% link en/compute/Hive.md %})
* [Presto with Alluxio]({{site.baseurl}}{% link en/compute/Presto.md %})
