---
layout: global
title: Azure Storage Gen2
nickname: Azure Data Lake Storage Gen2 
group: Storage Integrations
priority: 2
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [Azure Data Lake Storage Gen2](https://docs.microsoft.com/en-in/azure/storage/blobs/data-lake-storage-introduction) as the under storage system.

## Prerequisites

The Alluxio binaries must be on your machine.
You can either [compile the binaries from Alluxio source code]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}),
or [download the precompiled binaries directly]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

In preparation for using Azure Data Lake storage with Alluxio, [create a new Data Lake storage in your Azure
account](https://docs.microsoft.com/en-in/azure/storage/blobs/create-data-lake-storage-account) or use an existing Data Lake storage. 
You should also note the directory you want to use, either by creating a new directory, or using an existing one. You also need a 
[SharedKey](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key(.
For the purposes of this guide, the Azure storage account name is called `<AZURE_ACCOUNT>`,
the directory in that storage account is called `<AZURE_DIRECTORY>`, and the name of the container is called `<AZURE_CONTAINER>`.


## Setup with Shared Key

### Root Mount

To use Azure Data Lake Storage as the UFS of Alluxio root mount point,
you need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify the underfs address by modifying `conf/alluxio-site.properties` to include:

```properties
alluxio.master.mount.table.root.ufs=abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

Specify the Shared Key by adding the following property in `conf/alluxio-site.properties`:

```properties
alluxio.master.mount.table.root.option.fs.azure.account.key.<AZURE_ACCOUNT>.dfs.core.windows.net=<SHARED_KEY>
```

### Nested Mount
An Azure Data Lake store location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's
[Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

```console
$ ./bin/alluxio fs mount \
  --option fs.azure.account.key.<AZURE_ACCOUNT>.dfs.core.windows.net=<SHARED_KEY> \
  /mnt/abfs abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

After these changes, Alluxio should be configured to work with Azure Data Lake storage as its under storage system, and you can run Alluxio locally with it.

## Setup with OAuth 2.0 Client Credentials

### Root Mount

To use Azure Data Lake Storage as the UFS of Alluxio root mount point,
you need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify the underfs address by modifying `conf/alluxio-site.properties` to include:

```properties
alluxio.master.mount.table.root.ufs=abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

Specify the OAuth 2.0 Client Credentials by adding the following property in `conf/alluxio-site.properties`:
(Please note that for URL Endpoint, use the V1 token endpoint)

```properties
alluxio.master.mount.table.root.option.fs.azure.account.oauth2.client.endpoint=<OAUTH_ENDPOINT>
alluxio.master.mount.table.root.option.fs.azure.account.oauth2.client.id=<CLIENT_ID>
alluxio.master.mount.table.root.option.fs.azure.account.oauth2.client.secret=<CLIENT_SECRET>
```

### Nested Mount
An Azure Data Lake store location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's
[Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

```console
$ ./bin/alluxio fs mount \
  --option fs.azure.account.oauth2.client.endpoint=<OAUTH_ENDPOINT> \
  --option fs.azure.account.oauth2.client.id=<CLIENT_ID> \
  --option fs.azure.account.oauth2.client.secret=<CLIENT_SECRET> \
  /mnt/abfs abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

After these changes, Alluxio should be configured to work with Azure Data Lake storage as its under storage system, and you can run Alluxio locally with it.

## Running Alluxio Locally with Data Lake Storage

Start up Alluxio locally to see that everything works.

```console
./bin/alluxio format
./bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
./bin/alluxio runTests
```

Visit your directory `<AZURE_DIRECTORY>` to verify the files and directories created by Alluxio exist. For this test, you should see files named like:

```
<AZURE_DIRECTORY>/default_tests_files/BASIC_CACHE_PROMOTE_CACHE_THROUGH
```

To stop Alluxio, you can run:

```console
./bin/alluxio-stop.sh local
```
