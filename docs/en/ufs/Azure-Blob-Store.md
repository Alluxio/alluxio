---
layout: global
title: Azure Blob Store
---


This guide describes how to configure Alluxio with [Azure Blob Store](https://azure.microsoft.com/en-us/products/storage/blobs/){:target="_blank"} as the under storage system. 

Azure Blob Storage is Microsoft's object storage solution for the cloud. Blob Storage is optimized for storing massive amounts of unstructured data.

For more information about Azure Blob Store, please read its
[documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/){:target="_blank"}.

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using Azure Blob Store with Alluxio:
<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<AZURE_CONTAINER>`</td>
        <td markdown="span">[Create a new container in your Azure storage account](https://docs.microsoft.com/en-us/azure/storage/storage-create-storage-account){:target="_blank"} or use an existing container.</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<AZURE_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use in that container, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<AZURE_ACCOUNT>`</td>
        <td markdown="span">Your Azure storage account</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<AZURE_ACCOUNT_KEY>`</td>
        <td markdown="span">Your Azure account credientials</td>
    </tr>
</table>

## Basic Setup

To use Azure Blob Store as the UFS of Alluxio root mount point, you need to configure Alluxio to use under storage systems by modifying `conf/alluxio-site.properties`. If it does not exist, create the configuration file from the template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify the underfs address by modifying `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=wasbs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.blob.core.windows.net/<AZURE_DIRECTORY>/
```

Specify credentials for the Azure account of the root mount point by adding the following properties in `conf/alluxio-site.properties`:

```properties
fs.azure.account.key.<AZURE_ACCOUNT>.blob.core.windows.net=<AZURE_ACCOUNT_KEY>
```

## Running Alluxio Locally with Azure Blob Store

Once you have configured Alluxio to Azure Blob Store, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.

## Troubleshooting
### What should I do if I get http not support error?
If you mount the Blob and configure the Blob path start with `wasb://`, you may see the error as follows:

```
alluxio.exception.AlluxioException: com.microsoft.azure.storage.StorageException: The account being accessed does not support http.
```
You can change the Blob path start with `wasbs://`.
