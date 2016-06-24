---
layout: global
title: Getting Started
group: User Guide
priority: 0
---

The simplest way to quickly try out Alluxio is to install it locally on a single machine. In this
quick start guide, we will install Alluxio on your local machine, mount example data, and and
perform basic tasks with the data in Alluxio. During this guide, you will:

* Download and configure Alluxio
* Start Alluxio locally
* Mount a storage system in Alluxio
* Perform basic tasks via Alluxio Shell
* Shutdown Alluxio

## Prerequisites

For the following quick start guide, you will need:

* Mac OS X or Linux
* Java 7 or newer
* AWS account and keys

### Setup SSH (Mac OSX)

If you are using Mac OSX, you may have to enable the ability to ssh into localhost. To enable remote
login, Open **System Preferences**, then open **Sharing**. Make sure **Remote Login** is enabled.

## Downloading Alluxio

First, download the the Alluxio release, and unpack it.

```bash
$ curl -O http://alluxio.org/downloads/files/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz
$ tar -xzf alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_RELEASED_VERSION}}
```

This will create a directory **alluxio-{{site.ALLUXIO_RELEASED_VERSION}}** with all of the Alluxio
source files and Java binaries.

## Configuring Alluxio

Before we start Alluxio, we have to configure it. We will be using most of the default settings, but
we will have to add some parameters for accessing Amazon S3 later.

First, create the **conf/alluxio-env.sh** file from the template. You can create the config file
with the following command:

```bash
$ ./bin/alluxio bootstrap-conf localhost
```

Afterwards, open the config file **conf/alluxio-env.sh** in your favorite editor to modify it. Add
the following line to add your AWS S3 access information.

```
ALLUXIO_JAVA_OPTS="-Dfs.s3n.awsAccessKeyId=AWS_ACCESS_KEY_ID -Dfs.s3n.awsSecretAccessKey=AWS_SECRET_ACCESS_KEY"
```

You will have to replace **AWS_ACCESS_KEY_ID** with your AWS access key id, and
**AWS_SECRET_ACCESS_KEY** with your AWS secret access key. Those are all the Alluxio configuration
changes required for the rest of this guide.

## Starting Alluxio

Next, we will format Alluxio in preparation for starting Alluxio. The following command will format
the Alluxio journal in preparation for the Alluxio master to start.

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

This will print out the Alluxio command-line operations available.

You can list files in Alluxio with the `ls` command. List the files in the root directory with the
following command:

```bash
$ ./bin/alluxio fs ls /
```

Unfortunately, we do not have any files in Alluxio. We can solve that by copying a file into
Alluxio. The `copyFromLocal` shell command is used to copy a local file into Alluxio.

```bash
$ ./bin/alluxio fs copyFromLocal LICENSE /LICENSE
Copied LICENSE to /LICENSE
```

After copying the **LICENSE** file, we should be able to see it in Alluxio. List the files in
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

However, the directory doesn't exist! This is because by default, Alluxio will write data into
Alluxio space only, and not to the UFS.

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

## Mounting in Alluxio

Alluxio unifies access to storage systems with the unified namespace feature. Read the [Unified
Namespace blog post](http://www.alluxio.com/2016/04/unified-namespace-allowing-applications-to-access-data-anywhere/)
and the [unified namespace documentation](Unified-and-Transparent-Namespace.html) for more detailed
explanations of the feature.

This feature allows users to mount different storage systems into the Alluxio namespace and simply
access the files across various storage systems through the Alluxio namespace seamlessly.

First, we will create a directory in Alluxio to store our mount points.

```bash
$ ./bin/alluxio fs mkdir /mnt
Successfully created directory /mnt
```

Next, we will mount an S3 bucket to Alluxio. We have a sample bucket to use for this guide.

```bash
$ ./bin/alluxio fs mount -readonly alluxio://localhost:19998/mnt/s3 s3n://alluxio-quick-start/data
Mounted s3n://alluxio-quick-start/data at alluxio://localhost:19998/mnt/s3
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

With Alluxio's unified namespace, you can easily interact with data from different storage systems
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

## Accelerating Data Access with Alluxio

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
access to this data by using memory to store the data. However, the `cat` shell command prevents
Alluxio from caching the data in memory. There is a separate shell command, `load`, which tells
Alluxio to store the data in memory. You can tell Alluxio load the data into memory with the
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
Now that you have installed and run Alluxio locally, and ran a few simple examples, you can stop
Alluxio with the following command:

```bash
$ ./bin/alluxio-stop.sh all
```

## Next Steps

Now that you have successfully started and interacted with Alluxio locally, there are several next
steps available.

### Deploying Alluxio

Alluxio can be deployed in many different environments.

* [Alluxio on Local Machine](Running-Alluxio-Locally.html)
* [Alluxio on Virtual Box](Running-Alluxio-on-Virtual-Box.html)
* [Alluxio Standalone on a Cluster](Running-Alluxio-on-a-Cluster.html)
* [Alluxio Standalone with Fault Tolerance](Running-Alluxio-Fault-Tolerant.html)
* [Alluxio on EC2](Running-Alluxio-on-EC2.html)
* [Alluxio on GCE](Running-Alluxio-on-GCE.html)
* [Alluxio with Mesos on EC2](Running-Alluxio-on-EC2-Mesos.html)
* [Alluxio with Fault Tolerance on EC2](Running-Alluxio-Fault-Tolerant-on-EC2.html)
* [Alluxio with YARN on EC2](Running-Alluxio-on-EC2-Yarn.html)

### Under Storage Systems

There are many Under storage systems that can be accessed through Alluxio.

* [Alluxio with GCS](Configuring-Alluxio-with-GCS.html)
* [Alluxio with S3](Configuring-Alluxio-with-S3.html)
* [Alluxio with Swift](Configuring-Alluxio-with-Swift.html)
* [Alluxio with GlusterFS](Configuring-Alluxio-with-GlusterFS.html)
* [Alluxio with HDFS](Configuring-Alluxio-with-HDFS.html)
* [Alluxio with Secure HDFS](Configuring-Alluxio-with-secure-HDFS.html)
* [Alluxio with OSS](Configuring-Alluxio-with-OSS.html)
* [Alluxio with NFS](Configuring-Alluxio-with-NFS.html)

### Frameworks

Different frameworks and applications work with Alluxio.

* [Apache Spark with Alluxio](Running-Spark-on-Alluxio.html)
* [Apache Hadoop MapReduce with Alluxio](Running-Hadoop-MapReduce-on-Alluxio.html)
* [Apache Flink with Alluxio](Running-Flink-on-Alluxio.html)
* [Apache Zeppelin with Alluxio](Accessing-Alluxio-from-Zeppelin.html)
