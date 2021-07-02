---
layout: global
title: Azure Data Lake Storage
nickname: Azure Data Lake Storage
group: Storage Integrations
priority: 2
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [Azure Data Lake Storage Gen1](https://docs.microsoft.com/en-in/azure/data-lake-store/data-lake-store-overview) as the under storage system.

## Prerequisites

The Alluxio binaries must be on your machine.
You can either [compile the binaries from Alluxio source code]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}),
or [download the precompiled binaries directly]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

In preparation for using Azure Data Lake storage with Alluxio, create a new Data Lake storage in your Azure
account or use an existing Data Lake storage. You should also note the directory you want to
use, either by creating a new directory, or using an existing one. You also need to set up 
[Service-to-service authentication](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-service-to-service-authenticate-using-active-directory) for your storage account.
For the purposes of this guide, the Azure storage account name is called `<AZURE_ACCOUNT>`
and the directory in that storage account is called `<AZURE_DIRECTORY>`. For more information 
about Azure storage account, Please see
[here](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-get-started-portal).


## Basic Setup

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
alluxio.master.mount.table.root.ufs=adl://<AZURE_ACCOUNT>.azuredatalakestore.net/<AZURE_DIRECTORY>/
```

Specify the application ID, authentication key and tenant ID for the Azure AD application used for the Azure account of the root mount point by adding the following
properties in `conf/alluxio-site.properties`:
- For instructions on how to retrieve the application ID and authentication key (also called the client secret) for your application, see [Get application ID and authentication key](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal#get-tenant-and-app-id-values-for-signing-in).
- For instructions on how to retrieve the tenant ID, see [Get tenant ID](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal#get-tenant-and-app-id-values-for-signing-in).

```properties
alluxio.master.mount.table.root.option.fs.adl.account.<AZURE_ACCOUNT>.oauth2.client.id=<APPLICATION_ID>
alluxio.master.mount.table.root.option.fs.adl.account.<AZURE_ACCOUNT>.oauth2.credential=<AUTHENTICATION_KEY>
alluxio.master.mount.table.root.option.fs.adl.account.<AZURE_ACCOUNT>.oauth2.refresh.url=https://login.microsoftonline.com/<TENANT_ID>/oauth2/token
```

### Nested Mount
An Azure Data Lake store location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's
[Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

```console
$ ./bin/alluxio fs mount \
  --option fs.adl.account.<AZURE_ACCOUNT>.oauth2.client.id=<APPLICATION_ID>
  --option fs.adl.account.<AZURE_ACCOUNT>.oauth2.credential=<AUTHENTICATION_KEY>
  --option fs.adl.account.<AZURE_ACCOUNT>.oauth2.refresh.url=https://login.microsoftonline.com/<TENANT_ID>/oauth2/token
  /mnt/adls adl://<AZURE_ACCOUNT>.azuredatalakestore.net/<AZURE_DIRECTORY>/
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
