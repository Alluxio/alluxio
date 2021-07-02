---
layout: global
title: WEB
nickname: WEB
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}

This guide describes the instructions to configure WEB as Alluxio's under
storage system.

## Initial Setup

The Alluxio binaries must be on your machine. You can either
[compile Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), or
[download the binaries locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

## Configuring Alluxio
Alluxio unifies access to different storage systems through the [unified namespace]({{ '/en/core-services/Unified-Namespace.html' | relativize_url }}) feature. An WEB location can be either mounted at the root of the Alluxio namespace or at a nested directory.

### ROOT MOUNT
Configure Alluxio to use under storage systems by modifying
`conf/alluxio-site.properties`. If it does not exist, create the configuration file from the
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

the following environment variable assignment needs to be added to
`conf/alluxio-site.properties`, and we actually support both http:// and https:// protocols.

```
alluxio.master.hostname=localhost
# alluxio.master.mount.table.root.ufs=[https|http]://<HOSTNAME>:<PORT>/DIRECTORY/
# A sample for this article
alluxio.master.mount.table.root.ufs=https://downloads.alluxio.io/downloads/files/
```

Specify the settings for parsing a WEB page(Optional):
```
alluxio.underfs.web.connnection.timeout=<WEB_CONNECTION_TIMEOUT>
alluxio.underfs.web.header.last.modified=<WEB_HEADER_LAST_MODIFIED>
alluxio.underfs.web.parent.names=<WEB_PARENT_NAMES>
alluxio.underfs.web.titles=<WEB_TITLES>
```
Here, `alluxio.underfs.web.connnection.timeout` is the timeout setting for an http connection
(unit: second, default: 60s). 
`alluxio.underfs.web.header.last.modified` represents the format to parse the last modified field for a directory 
or a file from an http response header (default: "EEE, dd MMM yyyy HH:mm:ss zzz"). 
`alluxio.underfs.web.parent.names` indicates the start row index of the files list, 
which can be set as multiple flags separated by commas (default: "Parent Directory,..,../"). 
`alluxio.underfs.web.titles` is a flag that can be used to check if a web page is a directory. 
It can also be set with multiple values separated by commas (default: "Index of ,Directory listing for ").

### NESTED MOUNT

An WEB location can be mounted at a nested directory in the Alluxio namespace to have unified access to multiple under storage systems. Alluxio's [Mount Command]({{ '/en/operation/User-CLI.html' | relativize_url }}#mount) can be used for this purpose.
For example, the following command mounts a directory inside an WEB directory into Alluxio directory
`/web`:

```console 
$ ./bin/alluxio fs mount \
  --option alluxio.underfs.web.connnection.timeout=<WEB_CONNECTION_TIMEOUT> \
  --option alluxio.underfs.web.header.last.modified=<WEB_HEADER_LAST_MODIFIED> \
  --option alluxio.underfs.web.parent.names=<WEB_PARENT_NAMES> \
  --option alluxio.underfs.web.titles=<WEB_TITLES> \
  /web [https|http]://<HOSTNAME>:<PORT>/DIRECTORY/ 
```

## Running Alluxio with WEB

Run the following command to start Alluxio filesystem.

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

To verify that Alluxio is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder.

Visit your WEB volume by running the following command:

```console
$ ./bin/alluxio fs ls /
```

After waiting for a while, you can see the following result:

```
dr--r-----                                              0       PERSISTED 05-21-2019 12:53:22:000  DIR /1.4.0
dr--r-----                                              0       PERSISTED 05-21-2019 12:54:23:000  DIR /1.5.0
dr--r-----                                              0       PERSISTED 05-21-2019 12:55:06:000  DIR /1.6.0
dr--r-----                                              0       PERSISTED 05-21-2019 12:55:38:000  DIR /1.6.1
dr--r-----                                              0       PERSISTED 05-21-2019 12:57:00:000  DIR /1.7.0
dr--r-----                                              0       PERSISTED 05-21-2019 12:57:57:000  DIR /1.7.1
dr--r-----                                              0       PERSISTED 05-21-2019 13:00:25:000  DIR /1.8.0
dr--r-----                                              0       PERSISTED 05-21-2019 13:02:07:000  DIR /1.8.1
dr--r-----                                              0       PERSISTED 05-24-2019 05:16:31:000  DIR /2.0.0
dr--r-----                                              0       PERSISTED 05-21-2019 13:02:11:000  DIR /2.0.0-preview
```

Stop Alluxio by running:

```console
$ ./bin/alluxio-stop.sh local
```
