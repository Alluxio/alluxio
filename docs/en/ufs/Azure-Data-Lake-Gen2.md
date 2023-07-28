---
layout: global
title: Azure Data Lake Storage Gen2
---


This guide describes how to configure Alluxio with [Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/){:target="_blank"} as the under storage system.

Azure Data Lake Storage Gen2 is a set of capabilities dedicated to big data analytics, built on Azure Blob Storage. It converges the capabilities of Azure Data Lake Storage Gen1 with Azure Blob Storage.

For more information about Azure Data Lake Storage Gen1, please read its [documentation](https://docs.microsoft.com/en-in/azure/storage/blobs/data-lake-storage-introduction){:target="_blank"}.

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using Azure Data Lake Storage Gen2 with Alluxio, [create a new Data Lake storage in your Azure account](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account){:target="_blank"} or use an existing Data Lake storage.
<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<AZURE_CONTAINER>`</td>
        <td markdown="span">The container you want to use</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<AZURE_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in the container, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<AZURE_ACCOUNT>`</td>
        <td markdown="span">Your Azure storage account</td>
    </tr>
</table>

You also need a 
[SharedKey](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key){:target="_blank"} to authorize requests.

## Basic Setup 

To use Azure Data Lake Storage Gen2 as the UFS of Alluxio root mount point,
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

### Setup with Shared Key

Specify the Shared Key by adding the following property in `conf/alluxio-site.properties`:

```properties
fs.azure.account.key.<AZURE_ACCOUNT>.dfs.core.windows.net=<AZURE_SHARED_KEY>
```

### Setup with OAuth 2.0 Client Credentials

Specify the OAuth 2.0 Client Credentials by adding the following property in `conf/alluxio-site.properties`:
(Please note that for URL Endpoint, use the V1 token endpoint)

```properties
fs.azure.account.oauth2.client.endpoint=<OAUTH_ENDPOINT>
fs.azure.account.oauth2.client.id=<CLIENT_ID>
fs.azure.account.oauth2.client.secret=<CLIENT_SECRET>
```

## Setup with Azure Managed Identities

Specify the Azure Managed Identities by adding the following property in `conf/alluxio-site.properties`:

```properties
fs.azure.account.oauth2.msi.endpoint=<MSI_ENDPOINT>
fs.azure.account.oauth2.client.id=<CLIENT_ID>
fs.azure.account.oauth2.msi.tenant=<MSI_TENANT>
```

## Running Alluxio Locally with Data Lake Storage

Once you have configured Alluxio to Azure Data Lake Storage Gen2, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.
