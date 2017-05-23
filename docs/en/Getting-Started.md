---
layout: global
title: Quick Start Guide
group: User Guide
priority: 0
---

* Table of Contents
{:toc}

The simplest way to quickly try out Alluxio is to install it locally on a single machine. In this
quick start guide, we will install Alluxio on your local machine, mount example data, and
perform basic tasks with the data in Alluxio. During this guide, you will:

* Download and configure Alluxio
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
[Accelerating data analytics on ceph object storage with Alluxio](https://www.alluxio.com/blog/accelerating-data-analytics-on-ceph-object-storage-with-alluxio).

## Prerequisites

For the following quick start guide, you will need:

* Mac OS X or Linux
* [Java 7 or newer](Java-Setup.html)
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
source files and Java binaries.

## Configuring Alluxio

Before we start Alluxio, we have to configure it. We will be using most of the default settings.

Create the `conf/alluxio-env.sh` configuration file from the template. You can create the config
file with the following command:

```bash
$ ./bin/alluxio bootstrapConf localhost
```

### [Bonus] Configuration for AWS

If you have an Amazon AWS account with your access key id and secret key, you can update your
Alluxio configuration now in preparation for interacting with Amazon S3 later in this guide. Add
your AWS access information to the Alluxio configuration by adding the keys to the
`conf/alluxio-site.properties` file. The following commands will update the configuration.

```bash
$ echo "aws.accessKeyId=AWS_ACCESS_KEY_ID" >> conf/alluxio-site.properties
$ echo "aws.secretKey=AWS_SECRET_ACCESS_KEY" >> conf/alluxio-site.properties
```

You will have to replace **AWS_ACCESS_KEY_ID** with your AWS access key id, and
**AWS_SECRET_ACCESS_KEY** with your AWS secret access key. Now, Alluxio is fully configured for the
rest of this guide.

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
$ ./bin/alluxio-start.sh local
```

Congratulations! Alluxio is now up and running! You can visit
[http://localhost:19999](http://localhost:19999) to see the status of the Alluxio master, and visit
[http://localhost:30000](http://localhost:30000) to see the status of the Alluxio worker.

## Using the Alluxio Shell

Now that Alluxio is running, we can examine the Alluxio file system with the
[Alluxio shell](Command-Line-Interface.html). The Alluxio shell enables many command-line operations
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
26.22KB   06-20-2016 11:30:04:415  In Memory      /LICENSE
```

The output shows the file that exists in Alluxio, as well as some other useful information, like the
size of the file, the date it was created, and the in-memory status of the file.

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
and the [unified namespace documentation](Unified-and-Transparent-Namespace.html) for more detailed
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
$ ./bin/alluxio fs mount -readonly alluxio://localhost:19998/mnt/s3 s3a://alluxio-quick-start/data
Mounted s3a://alluxio-quick-start/data at alluxio://localhost:19998/mnt/s3
```

Now, the S3 bucket is mounted into the Alluxio namespace.

We can list the files from S3, through the Alluxio namespace. We can use the familiar `ls` shell
command to list the files from the S3 mounted directory.

```bash
$ ./bin/alluxio fs ls /mnt/s3
87.86KB   06-20-2016 12:50:51:660  Not In Memory  /mnt/s3/sample_tweets_100k.csv
933.21KB  06-20-2016 12:50:53:633  Not In Memory  /mnt/s3/sample_tweets_1m.csv
149.77MB  06-20-2016 12:50:55:473  Not In Memory  /mnt/s3/sample_tweets_150m.csv
9.61MB    06-20-2016 12:50:55:821  Not In Memory  /mnt/s3/sample_tweets_10m.csv
```

We can see the [newly mounted files and directories in the Alluxio web UI](http://localhost:19999/browse?path=%2Fmnt%2Fs3) as well.

With Alluxio's unified namespace, you can interact with data from different storage systems
seamlessly. For example, with the `ls` shell command, you can recursively list all the files that
exist under a directory.

```bash
$ ./bin/alluxio fs ls -R /
26.22KB   06-20-2016 11:30:04:415  In Memory      /LICENSE
1.00B     06-20-2016 12:28:39:176                 /mnt
4.00B     06-20-2016 12:30:41:986                 /mnt/s3
87.86KB   06-20-2016 12:50:51:660  Not In Memory  /mnt/s3/sample_tweets_100k.csv
933.21KB  06-20-2016 12:50:53:633  Not In Memory  /mnt/s3/sample_tweets_1m.csv
149.77MB  06-20-2016 12:50:55:473  Not In Memory  /mnt/s3/sample_tweets_150m.csv
9.61MB    06-20-2016 12:50:55:821  Not In Memory  /mnt/s3/sample_tweets_10m.csv
```

This shows all the files under the root of the Alluxio file system, from all of the mounted storage
systems. The `/LICENSE` file is in your local file system, while the files under `/mnt/s3/` are in
S3.

## [Bonus] Accelerating Data Access with Alluxio

Since Alluxio leverages memory to store data, it can accelerate access to data. First, let’s take a
look at the status of a file in Alluxio (mounted from S3).

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
149.77MB  06-20-2016 12:50:55:473  Not In Memory  /mnt/s3/sample_tweets_150m.csv
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
of this file.

Now, let’s see how many tweets mention the word "puppy".

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c puppy
1553

real	0m25.998s
user	0m6.828s
sys	0m1.048s
```

As you can see, it takes a lot of time to access the data for each command. Alluxio can accelerate
access to this data by using memory to store the data. However, the `cat` shell command does not
cache data in Alluxio memory. There is a separate shell command, `load`, which tells
Alluxio to store the data in memory. You can tell Alluxio to load the data into memory with the
following command.

```bash
$ ./bin/alluxio fs load /mnt/s3/sample_tweets_150m.csv
```

After loading the file, you can check the status with the ls command:

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
149.77MB  06-20-2016 12:50:55:473  In Memory      /mnt/s3/sample_tweets_150m.csv
```

The output shows that the file is now **In Memory**. Now that the file is memory, reading the file
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

* [Alluxio on Local Machine](Running-Alluxio-Locally.html)
* [Alluxio on Virtual Box](Running-Alluxio-on-Virtual-Box.html)
* [Alluxio Standalone on a Cluster](Running-Alluxio-on-a-Cluster.html)
* [Alluxio Standalone with Fault Tolerance](Running-Alluxio-Fault-Tolerant.html)
* [Alluxio on EC2](Running-Alluxio-on-EC2.html)
* [Alluxio on GCE](Running-Alluxio-on-GCE.html)
* [Alluxio with Mesos on EC2](Running-Alluxio-on-Mesos.html)
* [Alluxio with Fault Tolerance on EC2](Running-Alluxio-Fault-Tolerant-on-EC2.html)
* [Alluxio with YARN on EC2](Running-Alluxio-on-EC2-Yarn.html)

### Under Storage Systems

There are many Under storage systems that can be accessed through Alluxio.

* [Alluxio with GCS](Configuring-Alluxio-with-GCS.html)
* [Alluxio with S3](Configuring-Alluxio-with-S3.html)
* [Alluxio with Swift](Configuring-Alluxio-with-Swift.html)
* [Alluxio with GlusterFS](Configuring-Alluxio-with-GlusterFS.html)
* [Alluxio with HDFS](Configuring-Alluxio-with-HDFS.html)
* [Alluxio with MapR-FS](Configuring-Alluxio-with-MapR-FS.html)
* [Alluxio with Secure HDFS](Configuring-Alluxio-with-secure-HDFS.html)
* [Alluxio with OSS](Configuring-Alluxio-with-OSS.html)
* [Alluxio with NFS](Configuring-Alluxio-with-NFS.html)

### Frameworks and Applications

Different frameworks and applications work with Alluxio.

* [Apache Spark with Alluxio](Running-Spark-on-Alluxio.html)
* [Apache Hadoop MapReduce with Alluxio](Running-Hadoop-MapReduce-on-Alluxio.html)
* [Apache Flink with Alluxio](Running-Flink-on-Alluxio.html)
* [Apache Zeppelin with Alluxio](Accessing-Alluxio-from-Zeppelin.html)
* [Apache HBase with Alluxio](Running-HBase-on-Alluxio.html)
