---
layout: global
title: Configuring Alluxio with Azure Blob Store
nickname: Alluxio with Azure Blob Store
group: Under Store
priority: 0
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [Azure Blob Store](https://azure.microsoft.com/en-in/services/storage/blobs/) as the under storage system.

## Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these machines. You can either [compile the binaries from Alluxio source code](Building-Alluxio-Master-Branch.html), or [download the precompiled binaries directly](Running-Alluxio-Locally.html).

Then, if you haven't already done so, create your configuration file with `bootstrapConf` command.
For example, if you are running Alluxio on your local machine, `ALLUXIO_MASTER_HOSTNAME` should be set to `localhost`

```
$ ./bin/alluxio bootstrapConf <ALLUXIO_MASTER_HOSTNAME>
```

Alternatively, you can also create the configuration file from the template and set the contents manually.

```
$ cp conf/alluxio-env.sh.template conf/alluxio-env.sh
```

Also, in preparation for using Azure blob store with Alluxio, create a new container in your Azure storage account or use an existing container. You should also note that the directory you want to use in that container, either by creating a new directory in the container, or using an existing one. For the purposes of this guide, the Azure storage account name is called `AZURE_ACCOUNT`, the container in that storage account is called `AZURE_CONTAINER` and the directory in that bucket is called `AZURE_DIRECTORY`. For more information about Azure storage account, Please see [here](https://docs.microsoft.com/en-us/azure/storage/storage-create-storage-account)

## Configuring Alluxio

Alluxio can support the Azure blob store via the HDFS interface. You can find more about running Hadoop on Azure Blob Store over [here](http://hadoop.apache.org/docs/r2.7.1/hadoop-azure/index.html).
Download azure storage java library(version 2.2.0) from [here](https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage) and hadoop azure libraries corresponding to your Hadoop version from [here](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure). Please make sure to use `azure-storage-2.2.0.jar` and not any other higher version due to version conflicts with hadoop-azure libraries.

You need to add the above mentioned libraries in the `ALLUXIO_CLASSPATH`. You can do this by adding following line in the `conf/alluxio-env.sh`:
```
export ALLUXIO_CLASSPATH=PATH_TO_HADOOP_AZURE_JAR/hadoop-azure-2.7.3.jar:PATH_TO_AZURE_STORAGE_JAR/azure-storage-2.2.0.jar
```

You need to configure Alluxio to use Azure Blob Store as its under storage system. The first modification is to specify the underfs address and setting hdfs prefixes so that Alluxio can recognize `wasb://` scheme by modifying `conf/alluxio-site.properties` to include:

```
alluxio.underfs.address=wasb://AZURE_CONTAINER@AZURE_ACCOUNT.blob.core.windows.net/AZURE_DIRECTORY/
alluxio.underfs.hdfs.prefixes=hdfs://,glusterfs:///,maprfs:///,wasb://
```

Next you need to specify credentials and the implementation class for the `wasb://` scheme by adding the following properties in `conf/core-site.xml`:
```
<configuration>
<property>
  <name>fs.AbstractFileSystem.wasb.impl</name>
  <value>org.apache.hadoop.fs.azure.Wasb</value>
</property>
<property>
  <name>fs.azure.account.key.AZURE_ACCOUNT.blob.core.windows.net</name>
  <value>YOUR ACCESS KEY</value>
</property>
</configuration>
```

After these changes, Alluxio should be configured to work with Azure Blob Store as its under storage system, and you can try to run Alluxio locally with it.

## Running Alluxio Locally with Azure Blob Store

After everything is configured, you can start up Alluxio locally to see that everything works.

```
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at [http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```
$ ./bin/alluxio runTests
```

After this succeeds, you can visit your container AZURE_CONTAINER to verify the files and directories created by Alluxio exist. For this test, you should see files named like:

```
AZURE_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE
```

To stop Alluxio, you can run:

```
$ ./bin/alluxio-stop.sh local
```
