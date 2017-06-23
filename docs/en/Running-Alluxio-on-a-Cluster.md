---
layout: global
title: Running Alluxio on a Cluster
nickname: Alluxio on Cluster
group: Deploying Alluxio
priority: 2
---

* Table of Contents
{:toc}

## Download Alluxio

First download the `Alluxio` tar file, and extract it.

{% include Running-Alluxio-on-a-Cluster/download-extract-Alluxio-tar.md %}

## Configure Alluxio

In the `${ALLUXIO_HOME}/conf` directory, create the `conf/alluxio-site.properties` configuration
file from the template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Update `alluxio.master.hostname` in `conf/alluxio-site.properties` to the hostname of the machine
you plan to run Alluxio Master on. Add the IP addresses of all the worker nodes to the
`conf/workers` file. Finally, sync all the information to worker nodes. You can use

{% include Running-Alluxio-on-a-Cluster/sync-info.md %}

to sync files and folders to all hosts specified in the `alluxio/conf/workers` file.

## Start Alluxio

Now, you can start Alluxio:

{% include Running-Alluxio-on-a-Cluster/start-Alluxio.md %}

To verify that Alluxio is running, you can visit `http://<alluxio_master_hostname>:19999`, check the
log in the directory `alluxio/logs`, or run a sample program:

{% include Running-Alluxio-on-a-Cluster/run-tests.md %}

**Note**: If you are using EC2, make sure the security group settings on the master node allows
 incoming connections on the alluxio web UI port.
