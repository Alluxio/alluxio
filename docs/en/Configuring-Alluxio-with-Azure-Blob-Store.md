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

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these machines. You can either [compile the binaries from Alluxio source code](Building-Alluxio-From-Source.html), or [download the precompiled binaries directly](Running-Alluxio-Locally.html).

Also, in preparation for using Azure Blob Store with Alluxio, create a new container in your Azure
storage account or use an existing container. You should also note that the directory you want to
use in that container, either by creating a new directory in the container, or using an existing
one. For the purposes of this guide, the Azure storage account name is called `<AZURE_ACCOUNT>`, the
container in that storage account is called `<AZURE_CONTAINER>` and the directory in that container is
called `<AZURE_DIRECTORY>`. For more information about Azure storage account, Please see
[here](https://docs.microsoft.com/en-us/azure/storage/storage-create-storage-account).


## Configuring Alluxio

### Root Mount

To use Azure blob store as the UFS of Alluxio root mount point,
you need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

The first modification is to specify the underfs address by modifying `conf/alluxio-site.properties` to include:

```
alluxio.underfs.address=wasb://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.blob.core.windows.net/<AZURE_DIRECTORY>/
```

Next, specify credentials for the Azure account of the root mount point by adding the following
properties in `conf/alluxio-site.properties`:

```
alluxio.master.mount.table.root.option.fs.azure.account.key.<AZURE_ACCOUNT>.blob.core.windows.net=<YOUR ACCESS KEY>
```

### Nested Mount
An Azure blob store location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's [Command Line Interface](Command-Line-Interface.html) can be used for this purpose.

```bash
$ ./bin/alluxio fs mount --option fs.azure.account.key.<AZURE_ACCOUNT>.blob.core.windows.net=<AZURE_ACCESS_KEY>\
  /mnt/azure wasb://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.blob.core.windows.net/<AZURE_DIRECTORY>/
```

After these changes, Alluxio should be configured to work with Azure Blob Store as its under storage system, and you can run Alluxio locally with it.

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

After this succeeds, you can visit your container `<AZURE_CONTAINER>` to verify the files and directories created by Alluxio exist. For this test, you should see files named like:

```
<AZURE_CONTAINER>/<AZURE_DIRECTORY>/default_tests_files/BASIC_CACHE_PROMOTE_CACHE_THROUGH
```

To stop Alluxio, you can run:

```
$ ./bin/alluxio-stop.sh local
```
