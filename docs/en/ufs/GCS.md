---
layout: global
title: Google Cloud Storage
nickname: Google Cloud Storage
group: Storage Integrations
priority: 2
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [Google Cloud Storage (GCS)](https://cloud.google.com/storage/)
as the under storage system.

## Prerequisites

The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

In preparation for using GCS with Alluxio, create a bucket (or use an existing bucket). You
should also note the directory you want to use in that bucket, either by creating a new directory in
the bucket, or using an existing one. For the purposes of this guide, the GCS bucket name is called
`GCS_BUCKET`, and the directory in that bucket is called `GCS_DIRECTORY`.

For more information on GCS, please read its
[documentation](https://cloud.google.com/storage/docs/overview).

## Setup 

Alluxio provides two ways to access GCS. GCS version 1 is implemented based on 
[jets3t](http://www.jets3t.org/) library which is design for AWS S3. 
Thus, it only accepts Google cloud storage interoperability access/secret keypair 
which allows full access to all Google cloud storages inside a Google cloud project.
No permission or access control can be placed on the interoperability keys.
The conjunction of Google interoperability API and jets3t library also impact the performance of the default GCS UFS module. 

The default GCS UFS module (GCS version 2) is implemented based on Google Cloud API
which accepts [Google application credentials](https://cloud.google.com/docs/authentication/getting-started).
Based on the application credentials, Google cloud can determine what permissions an authenticated client 
has for its target Google cloud storage bucket. Besides, GCS with Google cloud API has much better performance
than the default one in metadata and read/write operations. 

## Basic Setup

A GCS bucket can be mounted to the Alluxio either at the root of the namespace, or at a nested directory.

### Root Mount Point

Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Configure Alluxio to use GCS as its root under storage system. The first modification is to
specify an **existing** GCS bucket and directory as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.master.mount.table.root.ufs=gs://GCS_BUCKET/GCS_DIRECTORY
```

Choose your preferred GCS UFS version and provide the corresponding Google credentials.

{% navtabs rootMount %}
{% navtab GCS version 2 %}

In`conf/alluxio-site.properties`, add:
```properties
alluxio.master.mount.table.root.option.fs.gcs.credential.path=/path/to/<google_application_credentials>.json
```
This property key provides the path to the Google application credentials json file. Note that the
Google application credentials json file should be placed in all the Alluxio nodes in the same path.
If the nodes running the Alluxio processes already contain the GCS credentials, this property may not be needed
but it is always recommended to set this property explicitly.

{% endnavtab %}

{% navtab GCS version 1 %}

In`conf/alluxio-site.properties`, add:
```properties
alluxio.master.mount.table.root.option.alluxio.underfs.gcs.version=1
alluxio.master.mount.table.root.option.fs.gcs.accessKeyId=<GCS_ACCESS_KEY_ID>
alluxio.master.mount.table.root.option.fs.gcs.secretAccessKey=<GCS_SECRET_ACCESS_KEY>
```
- The first property key tells Alluxio to load the Version 1 GCS UFS module which uses the [jets3t](http://www.jets3t.org/) library.
- Replace `<GCS_ACCESS_KEY_ID>` and `<GCS_SECRET_ACCESS_KEY>` with actual
[GCS interoperable storage access keys](https://console.cloud.google.com/storage/settings),
or other environment variables that contain your credentials.
Note: GCS interoperability is disabled by default. Please click on the Interoperability tab
in [GCS setting](https://console.cloud.google.com/storage/settings) and enable this feature.
Click on `Create a new key` to get the Access Key and Secret pair.

{% endnavtab %}
{% endnavtabs %}

After these changes, Alluxio should be configured to work with GCS as its under storage system, and
you can [Run Alluxio Locally with GCS](#running-alluxio-locally-with-gcs).

### Nested Mount Point

An GCS location can be mounted at a nested directory in the Alluxio namespace to have unified access
to multiple under storage systems. Alluxio's [Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }}) can be used for this purpose.

First, within `conf/alluxio-site.properties`, specify the master host:
```properties
alluxio.master.hostname=localhost
```

Then, mount GCS:
{% navtabs nestedMount %}
{% navtab GCS version 2 %}
```console
$ ./bin/alluxio fs mount \
  --option fs.gcs.credential.path=/path/to/<google_application_credentials>.json \
  /gcs gs://GCS_BUCKET/GCS_DIRECTORY
```
{% endnavtab %}
{% navtab GCS version 1 %}
```console
$ ./bin/alluxio fs mount \
  --option alluxio.underfs.gcs.version=1 \
  --option fs.gcs.accessKeyId=<GCS_ACCESS_KEY_ID> \
  --option fs.gcs.secretAccessKey=<GCS_SECRET_ACCESS_KEY> \
  /gcs gs://GCS_BUCKET/GCS_DIRECTORY
```
{% endnavtab %}
{% endnavtabs %}

## Running Alluxio Locally with GCS

Start up Alluxio locally to see that everything works.

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local SudoMount
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```console
$ ./bin/alluxio runTests
```

Visit your GCS directory `GCS_BUCKET/GCS_DIRECTORY` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like:

```
GCS_BUCKET/GCS_DIRECTORY/default_tests_files/BASIC_CACHE_THROUGH
```

To stop Alluxio, you can run:

```console
$ ./bin/alluxio-stop.sh local
```

## Advanced Setup

### Customize the Directory Suffix

Directories are represented in GCS as zero-byte objects named with a specified suffix. The
directory suffix can be updated with the configuration parameter
[alluxio.underfs.gcs.directory.suffix]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.underfs.gcs.directory.suffix).

## GCS Access Control

If Alluxio security is enabled, Alluxio enforces the access control inherited from underlying object
storage.

The GCS credentials specified in Alluxio config represents a GCS user. GCS service backend checks
the user permission to the bucket and the object for access control. If the given GCS user does not
have the right access permission to the specified bucket, a permission denied error will be thrown.
When Alluxio security is enabled, Alluxio loads the bucket ACL to Alluxio permission on the first
time when the metadata is loaded to Alluxio namespace.

### Mapping from GCS ACL to Alluxio permission

Alluxio checks the GCS bucket READ/WRITE ACL to determine the owner's permission mode to a Alluxio
file. For example, if the GCS user has read-only access to the underlying bucket, the mounted
directory and files would have `0500` mode. If the GCS user has full access to the underlying bucket,
the mounted directory and files would have `0700` mode.

### Mount point sharing

If you want to share the GCS mount point with other users in Alluxio namespace, you can enable
`alluxio.underfs.object.store.mount.shared.publicly`.

### Permission change

Command such as `chown`, `chgrp`, and `chmod` to Alluxio directories and files do **NOT** propagate to the underlying
GCS buckets nor objects.

### Mapping from GCS user to Alluxio file owner (GCS Version 1 only)

By default, Alluxio tries to extract the GCS user id from the credentials. Optionally,
`alluxio.underfs.gcs.owner.id.to.username.mapping` can be used to specify a preset gcs owner id to
Alluxio username static mapping in the format `id1=user1;id2=user2`. The Google Cloud Storage IDs
can be found at the console [address](https://console.cloud.google.com/storage/settings). Please use
the "Owners" one.

### Accessing GCS through Proxy (GCS Version 2 only)

If the Alluxio cluster is behind a corporate proxy or a firewall, the Alluxio GCS integration may not be able to access
the internet with the default settings.

Add the following java options to `conf/alluxio-env.sh` before starting the Alluxio Masters and Workers.

```bash
ALLUXIO_MASTER_JAVA_OPTS+=" -Dhttps.proxyHost=<proxy_host> -Dhttps.proxyPort=<proxy_port> -Dhttp.proxyHost=<proxy_host> -Dhttp.proxyPort=<proxy_port> -Dhttp.nonProxyHosts=<non_proxy_host>"
ALLUXIO_WORKER_JAVA_OPTS+=" -Dhttps.proxyHost=<proxy_host> -Dhttps.proxyPort=<proxy_port> -Dhttp.proxyHost=<proxy_host> -Dhttp.proxyPort=<proxy_port> -Dhttp.nonProxyHosts=<non_proxy_host>"
```

An example value for `http.nonProxyHosts` is `localhost|127.*|[::1]|192.168.0.0/16`.

If username and password are required for the proxy, add the `http.proxyUser`, `https.proxyUser`, `http.proxyPassword`, and `https.proxyPassword` java options.
