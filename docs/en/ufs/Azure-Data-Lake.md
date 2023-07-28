---
layout: global
title: Azure Data Lake Storage Gen1
---


This guide describes how to configure Alluxio with [Azure Data Lake Storage Gen1](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/){:target="_blank"} as the under storage system. 

Azure Data Lake Storage is an enterprise-wide hyper-scale repository for big data analytic workloads. Azure Data Lake enables you to capture data of any size, type, and ingestion speed in one single place for operational and exploratory analytics. It is designed to store and analyze large amounts of structured, semi-structured, and unstructured data.

For more information about Azure Data Lake Storage Gen1, please read its [documentation](https://docs.microsoft.com/en-in/azure/data-lake-store/data-lake-store-overview){:target="_blank"}.

**Note**: Azure Data Lake Storage Gen1 will be retired on Feb 29, 2024. Be sure to migrate to Azure Data Lake Storage Gen2 prior to that date. See how [here](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-migrate-gen1-to-gen2-azure-portal){:target="_blank"}.

## Prerequisites

If you haven't already, please see [Prerequisites]({{ '/en/ufs/Storage-Overview.html#prerequisites' | relativize_url }}) before you get started.

In preparation for using Azure Data Lake Storage Gen1 with Alluxio, [create a new Data Lake storage in your Azure
account](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-get-started-portal){:target="_blank"} or use an existing Data Lake storage.
<table class="table table-striped">
    <tr>
        <td markdown="span" style="width:30%">`<AZURE_DIRECTORY>`</td>
        <td markdown="span">The directory you want to use, either by creating a new directory or using an existing one</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<AZURE_ACCOUNT>`</td>
        <td markdown="span">Your Azure storage account</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<CLIENT_ID>`</td>
        <td markdown="span">See [Get application ID and authentication key](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal#sign-in-to-the-application){:target="_blank"} for instructions on how to retrieve the application (client) ID and authentication key (also called the client secret) for your application</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<AUTHENTICATION_KEY>`</td>
        <td markdown="span">See [Get application ID and authentication key](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal#sign-in-to-the-application){:target="_blank"} for instructions on how to retrieve the application (client) ID and authentication key (also called the client secret) for your application</td>
    </tr>
    <tr>
        <td markdown="span" style="width:30%">`<TENANT_ID>`</td>
        <td markdown="span">See [Get tenant ID](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal#sign-in-to-the-application){:target="_blank"} for instructions on how to retrieve the tenant ID</td>
    </tr>
</table>

You also need to set up 
[Service-to-service authentication](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-service-to-service-authenticate-using-active-directory){:target="_blank"} for your storage account.

## Basic Setup

To use Azure Data Lake Storage Gen1 as the UFS of Alluxio root mount point,
you need to configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Specify the underfs address by modifying `conf/alluxio-site.properties` to include:

```properties
alluxio.dora.client.ufs.root=adl://<AZURE_ACCOUNT>.azuredatalakestore.net/<AZURE_DIRECTORY>/
```

Specify the application ID, authentication key and tenant ID for the Azure AD application used for the Azure account of the root mount point by adding the following
properties in `conf/alluxio-site.properties`:

```properties
fs.adl.account.<AZURE_ACCOUNT>.oauth2.client.id=<CLIENT_ID>
fs.adl.account.<AZURE_ACCOUNT>.oauth2.credential=<AUTHENTICATION_KEY>
fs.adl.account.<AZURE_ACCOUNT>.oauth2.refresh.url=https://login.microsoftonline.com/<TENANT_ID>/oauth2/token
```

After these changes, Alluxio should be configured to work with Azure Data Lake storage as its under storage system, and you can run Alluxio locally with it.

## Running Alluxio Locally with Data Lake Storage

Once you have configured Alluxio to Azure Data Lake Storage Gen1, try [running Alluxio locally]({{ '/en/ufs/Storage-Overview.html#running-alluxio-locally' | relativize_url}}) to see that everything works.
