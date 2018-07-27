---
layout: global
title: Getting Started on a Cluster
group: User Guide
priority: 2
---

* Table of Contents
{:toc}

The [Quick Start Guide](Getting-Started.html) shows how to install Alluxio locally on a single machine.
In this advanced quick start guide, we will install Alluxio on a 3-node Linux cluster, configure a
AWS S3 bucket as UFS (under file system), and perform basic tasks with the data in Alluxio.

During this guide, you will:

* Download and configure Alluxio in a Linux cluster
* Configure for AWS
* Validate Alluxio environment
* Set up a distributed storage as UFS
* Start Alluxio on multiple nodes
* Perform basic tasks via Alluxio Shell
* **[Bonus]** Run sample Hadoop MapReduce job in Alluxio cluster
* **[Bonus]** Run sample Apache Spark job in Alluxio cluster
* Shutdown Alluxio

**[Required]** In this documentation, we will use AWS S3 as the example Alluxio under file system.
Please make sure you have an [AWS account with an access key id and secret access key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys),
so you will be able to use S3 in this guide. If you don't have an S3 account, you can also use an existing HDFS cluster as the under
file system. The [Set up a distributed storage as UFS](#set-up-a-distributed-storage-as-ufs) section will be slightly different.

**Note**  This guide is meant for you to quickly start interacting with an Alluxio system in a distributed environment.
If you are interested in running a larger scale example which highlights the performance benefits of Alluxio,
try out the instructions in either of these two whitepapers: [Accelerating on-demand data analytics with
Alluxio](https://alluxio.com/resources/accelerating-on-demand-data-analytics-with-alluxio),
[Accelerating data analytics on ceph object storage with Alluxio](https://www.alluxio.com/blog/accelerating-data-analytics-on-ceph-object-storage-with-alluxio).

## Prerequisites

For the following quick start guide, you will need:

* 3-node Linux cluster
* [Java 7 or newer](Java-Setup.html)
* AWS account and keys

### Set up SSH (Linux)

Please make sure on all the nodes, ssh is enabled.

Optionally, setting up password-less ssh
will make distributed copy and configuration easier. In this doc, we use the example of manual
downloading and starting Alluxio on multiple nodes, for demo purpose only. The manual process here
can be much simplified and automated using `./bin/alluxio copyDirr` or Ansible scripts. Please refer to this [doc](Running-Alluxio-on-a-Cluster.html)
and [Running-Alluxio-on-EC2](Running-Alluxio-on-EC2.html) for the alternatives.

## Download Alluxio

First, download the Alluxio release locally. You can
download the latest {{site.ALLUXIO_RELEASED_VERSION}} release pre-built for various versions of
Hadoop from the [Alluxio download page](http://www.alluxio.org/download).

Second, `scp` the release tarball to all the Linux nodes. Feel free to use scriptable way (such as Ansible)
to distribute the tarball and automate the following process on multiple machines. In this documentation,
manual commands are shown and most of them should be executed on all the nodes. The raw commands are described
to help users understand how Alluxio components are started individually.

Next, unpack the download with the following commands on all the nodes. Your file name may be different
depending on which of the pre-built binaries you have downloaded.

```bash
$ tar -xzf alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_RELEASED_VERSION}}
```

This will create a directory `alluxio-{{site.ALLUXIO_RELEASED_VERSION}}` with all of the Alluxio
scripts and binaries.

## Configure Alluxio

Before we start Alluxio, we have to configure it. Please ensure all the subsequent commands are run on all the Linux nodes.

In the `${ALLUXIO_HOME}/conf` directory, create the `conf/alluxio-site.properties` configuration
file from the template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Update `alluxio.master.hostname` in `conf/alluxio-site.properties` to the hostname of the machine
you plan to run Alluxio Master on. Let's name it Alluxio Master node, and assume its hostname is
**${ALLUXIO_MASTER_HOSTNAME}**.

```bash
$ echo "alluxio.master.hostname=${ALLUXIO_MASTER_HOSTNAME}" >> conf/alluxio-site.properties
```

### Configure for AWS

Please ensure all the subsequent commands are run on all the Linux nodes.

If you have an Amazon AWS account with your access key id and secret key, you can update your
Alluxio configuration now in preparation for interacting with Amazon S3 later in this guide. Add
your AWS access information to the Alluxio configuration by adding the keys to the
`conf/alluxio-site.properties` file. The following commands will update the configuration.

```bash
$ echo "aws.accessKeyId=${AWS_ACCESS_KEY_ID}" >> conf/alluxio-site.properties
$ echo "aws.secretKey=${AWS_SECRET_ACCESS_KEY}" >> conf/alluxio-site.properties
```

You will have to replace **AWS_ACCESS_KEY_ID** with your AWS access key id, and
**AWS_SECRET_ACCESS_KEY** with your AWS secret access key. Now, Alluxio is fully configured for the
rest of this guide.

## Validate Alluxio environment

Before starting Alluxio, you might want to make sure that your system environment is ready for running
Alluxio services. You can run the following command to validate environment on each node:

```bash
$ sudo bin/alluxio validateEnv local
```

You can also make the command run only specific validation tasks. For example,

```bash
$ sudo bin/alluxio validateEnv local ulimit
```

Will only run validation tasks that check your system resource limits, on the specific node.

You can check out [this page](Developer-Tips.html) for detailed usage information regarding this command.

## Set up a distributed storage as UFS

The [local Getting-Started guide](Getting-Started.html) shows the steps of how to start Alluxio with default local file system.
In a distributed cluster, Alluxio requires a distributed storage system as under file system.
In this doc, we use AWS S3 as an example here. If you want to use an existing distributed HDFS as UFS,
please refer to this [doc](Configuring-Alluxio-with-HDFS.html).

In preparation for using S3 with Alluxio, create a bucket (or use an existing bucket). Choose a directory in the S3 bucket
to use as the under storage, creating it if it doesn't already exist.
For the purposes of this guide, the S3 bucket name is called
`S3_BUCKET`, and the directory in that bucket is called `S3_DIRECTORY`.

You need to configure Alluxio to use S3 as its under storage system by modifying
`conf/alluxio-site.properties`. The first modification is to specify an **existing** S3
bucket and directory as the under storage system. On all the nodes, you specify it by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.underfs.address=s3a://S3_BUCKET/S3_DIRECTORY
```

Note that in the previous `Configuration for AWS` section, the required AWS credential is already setup on
all the Alluxio nodes. This ensures those nodes have the access to this S3 bucket.

If you want to enable more advanced features for S3 UFS, please refer to this [documentation](Configuring-Alluxio-with-S3.html).

## Start Alluxio

Next, we will format Alluxio in preparation for starting Alluxio. The following command will format
the Alluxio journal on the master node.

```bash
$ bin/alluxio format
```

Now, we can start Alluxio! In this doc, we will start one Alluxio Master and two Alluxio Workers.

On one of the nodes (naming it Master Node), run the following command to start Alluxio Master:

```bash
$ bin/alluxio-start.sh master
```

Congratulations! Alluxio master is now up and running! You can visit
http://ALLUXIO_MASTER_HOSTNAME:19999 to see the status of the Alluxio master.

On the other two nodes (naming them Worker1 and Worker2, respectively), run the following command to start
 Alluxio worker:

```bash
$ bin/alluxio-start.sh worker SudoMount
```

In a few seconds, Alluxio workers will register with the Alluxio master.
You can visit http://ALLUXIO_WORKER_HOSTNAME:30000 to see the status of an Alluxio worker.

You can run the following command to see if any configuration errors or warnings exists:

```bash
$ bin/alluxio fsadmin doctor
```

See the [Configuration checker docs](Configuration-Settings.md#server-configuration-checker) for more details.

## Use the Alluxio Shell

Now that Alluxio is running, we can examine the Alluxio file system with the
[Alluxio shell](Command-Line-Interface.html). The Alluxio shell enables many command-line operations
to interact with Alluxio. You can invoke the Alluxio shell from any of the nodes with
the following command:

```bash
$ bin/alluxio fs
```

This will print out the available Alluxio command-line operations.

For example, you can list files in Alluxio with the `ls` command. To list all files in the root directory, use the following command:

```bash
$ bin/alluxio fs ls /
```

Currently, we do not have any files in Alluxio. We can solve that by copying a file into
Alluxio. The `copyFromLocal` shell command is used to copy a local file into Alluxio.

```bash
$ bin/alluxio fs copyFromLocal LICENSE /LICENSE
Copied LICENSE to /LICENSE
```

After copying the `LICENSE` file, we should be able to see it in Alluxio. List the files in
Alluxio with the command:

```bash
$ bin/alluxio fs ls /
-rw-r--r--   ubuntu    ubuntu    26.22KB   NOT_PERSISTED 09-22-2017 09:30:08:781  100%      /LICENSE
```

The output shows the file that exists in Alluxio, as well as some other useful information, like the
size of the file, the date it was created, and how much of the file is cached in Alluxio.

You can view the contents of the file through the Alluxio shell. The `cat` command will print
the contents of the file.

```bash
$ bin/alluxio fs cat /LICENSE
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
...
```

With this UFS configuration, Alluxio uses the specified S3 bucket as its under file system (UFS).

We can check whether this file exists in the S3 bucket. However, the directory doesn't exist on S3!
By default, Alluxio will write data only into Alluxio space, not to the UFS. However, we can tell
Alluxio to persist the file from Alluxio space to the UFS. The shell command `persist` will do just that.

```bash
$ bin/alluxio fs persist /LICENSE
persisted file /LICENSE with size 26847
```

Now, if we examine S3 bucket again, the file should appear.

If we browse the Alluxio file system in the master's web UI at port 19999 we can
see the LICENSE file as well as other useful information. Here, the **Persistence State** column
shows the file as **PERSISTED**.

## [Bonus] Run a Hadoop MapReduce job on Alluxio cluster
Please refer to this [doc](Running-Hadoop-MapReduce-on-Alluxio.html) for how to install Hadoop MapReduce on this cluster,
and run a Hadoop MapReduce job on the installed Alluxio cluster.

Please make sure all the environment and configuration are set up on all the nodes.

## [Bonus] Run a Spark job on Alluxio cluster

Please refer to this [doc](Running-Spark-on-Alluxio.html) for how to install Apache Spark on this cluster,
and run a sample Spark job on the installed Alluxio cluster.

Please make sure all the environment and configuration are set up on all the nodes.

## Shutdown Alluxio

Once you are done with interacting with your Alluxio cluster installation, you can stop Alluxio with
the following command:

On master node:

```bash
$ bin/alluxio-stop.sh master
```

On worker nodes:

```bash
$ bin/alluxio-stop.sh worker
```

## Tips
If you want to save the manual steps to ssh and configure on all the nodes, and you can also start
Alluxio just from the master node. Set up password-less ssh on the server nodes, and follow the
instructions in this [doc](Running-Alluxio-on-a-Cluster.html). There you can manage Alluxio on a cluster
in a more scalable and convenient way, by leveraging `alluxio/conf/workers`, `copyDir` and `bin/alluxio-start.sh all`.

## Conclusion

Congratulations on completing the quick start guide for Alluxio on a cluster! You have successfully downloaded
and installed Alluxio on a small Linux cluster, and performed some basic interactions via the Alluxio
shell. This was a simple example on how to get started with an Alluxio cluster.

There are several next steps available. You can learn more about the various features of Alluxio in
our documentation. You can also deploy Alluxio in your environment, mount your existing
storage systems to Alluxio, or configure your applications to work with Alluxio. Additional
resources are below.

### Deploying Alluxio

Alluxio can be deployed in many different environments.

* [Alluxio on Local Machine](Running-Alluxio-Locally.html)
* [Alluxio Standalone on a Cluster](Running-Alluxio-on-a-Cluster.html)
* [Alluxio on Virtual Box](Running-Alluxio-on-Virtual-Box.html)
* [Alluxio on Docker](Running-Alluxio-On-Docker.html)
* [Alluxio on EC2](Running-Alluxio-on-EC2.html)
* [Alluxio on GCE](Running-Alluxio-on-GCE.html)
* [Alluxio with Mesos on EC2](Running-Alluxio-on-Mesos.html)
* [Alluxio with Fault Tolerance on EC2](Running-Alluxio-Fault-Tolerant-on-EC2.html)
* [Alluxio YARN Integration](Running-Alluxio-Yarn-Integration.html)
* [Alluxio Standalone with YARN](Running-Alluxio-Yarn-Standalone.html)

### Under Storage Systems

There are many Under storage systems that can be accessed through Alluxio.

* [Alluxio with Azure Blob Store](Configuring-Alluxio-with-Azure-Blob-Store.html)
* [Alluxio with S3](Configuring-Alluxio-with-S3.html)
* [Alluxio with GCS](Configuring-Alluxio-with-GCS.html)
* [Alluxio with Minio](Configuring-Alluxio-with-Minio.html)
* [Alluxio with Ceph](Configuring-Alluxio-with-Ceph.html)
* [Alluxio with Swift](Configuring-Alluxio-with-Swift.html)
* [Alluxio with GlusterFS](Configuring-Alluxio-with-GlusterFS.html)
* [Alluxio with MapR-FS](Configuring-Alluxio-with-MapR-FS.html)
* [Alluxio with HDFS](Configuring-Alluxio-with-HDFS.html)
* [Alluxio with Secure HDFS](Configuring-Alluxio-with-secure-HDFS.html)
* [Alluxio with OSS](Configuring-Alluxio-with-OSS.html)
* [Alluxio with NFS](Configuring-Alluxio-with-NFS.html)

### Frameworks and Applications

Different frameworks and applications work with Alluxio.

* [Apache Spark with Alluxio](Running-Spark-on-Alluxio.html)
* [Apache Hadoop MapReduce with Alluxio](Running-Hadoop-MapReduce-on-Alluxio.html)
* [Apache HBase with Alluxio](Running-HBase-on-Alluxio.html)
* [Apache Flink with Alluxio](Running-Flink-on-Alluxio.html)
* [Presto with Alluxio](Running-Presto-with-Alluxio.html)
* [Apache Hive with Alluxio](Running-Hive-with-Alluxio.html)
* [Apache Zeppelin with Alluxio](Accessing-Alluxio-from-Zeppelin.html)
