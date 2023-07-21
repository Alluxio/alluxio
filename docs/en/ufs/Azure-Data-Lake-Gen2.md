---
layout: global
title: Azure Storage Gen2
---


This guide describes how to configure Alluxio with [Azure Data Lake Storage Gen2](https://docs.microsoft.com/en-in/azure/storage/blobs/data-lake-storage-introduction) as the under storage system.

Azure Data Lake Storage Gen2 is a set of capabilities dedicated to big data analytics, built on Azure Blob Storage. It converges the capabilities of Azure Data Lake Storage Gen1 with Azure Blob Storage.

## Prerequisites

In preparation for using Azure Data Lake storage with Alluxio, [create a new Data Lake storage in your Azure
account](https://docs.microsoft.com/en-in/azure/storage/blobs/create-data-lake-storage-account) or use an existing Data Lake storage. 
You should also note the directory you want to use, either by creating a new directory, or using an existing one. You also need a 
[SharedKey](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key).
For the purposes of this guide, the Azure storage account name is called `<AZURE_ACCOUNT>`,
the directory in that storage account is called `<AZURE_DIRECTORY>`, and the name of the container is called `<AZURE_CONTAINER>`.


## Setup with Shared Key

### Root Mount

To use Azure Data Lake Storage as the UFS of Alluxio root mount point,
you need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify the underfs address by modifying `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

Specify the Shared Key by adding the following property in `conf/alluxio-site.properties`:

```properties
fs.azure.account.key.<AZURE_ACCOUNT>.dfs.core.windows.net=<SHARED_KEY>
```

## Setup with OAuth 2.0 Client Credentials

### Root Mount

To use Azure Data Lake Storage as the UFS of Alluxio root mount point,
you need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify the underfs address by modifying `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

Specify the OAuth 2.0 Client Credentials by adding the following property in `conf/alluxio-site.properties`:
(Please note that for URL Endpoint, use the V1 token endpoint)

```properties
fs.azure.account.oauth2.client.endpoint=<OAUTH_ENDPOINT>
fs.azure.account.oauth2.client.id=<CLIENT_ID>
fs.azure.account.oauth2.client.secret=<CLIENT_SECRET>
```

## Setup with Azure Managed Identities

### Root Mount

To use Azure Data Lake Storage as the UFS of Alluxio root mount point,
you need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify the underfs address by modifying `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

Specify the Azure Managed Identities by adding the following property in `conf/alluxio-site.properties`:

```properties
fs.azure.account.oauth2.msi.endpoint=<MSI_ENDPOINT>
fs.azure.account.oauth2.client.id=<CLIENT_ID>
fs.azure.account.oauth2.msi.tenant=<TENANT>
```

## Running Alluxio Locally with Data Lake Storage

Start up Alluxio locally to see that everything works.

```shell
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```shell
$ ./bin/alluxio runTests
```

Visit your directory `<AZURE_DIRECTORY>` to verify the files and directories created by Alluxio exist. For this test, you should see files named like:

```
<AZURE_DIRECTORY>/default_tests_files/BASIC_CACHE_PROMOTE_CACHE_THROUGH
```

To stop Alluxio, you can run:

```shell
$ ./bin/alluxio-stop.sh local
```
