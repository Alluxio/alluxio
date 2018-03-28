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

First [download](https://alluxio.org/download) the Alluxio tar file, and extract it.

## Configure Alluxio

In the `${ALLUXIO_HOME}/conf` directory, create the `conf/alluxio-site.properties` configuration
file from the template.

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Update `alluxio.master.hostname` in `conf/alluxio-site.properties` to the hostname of the machine
you plan to run Alluxio Master on. Add the IP addresses or hostnames of all the worker nodes to the
`conf/workers` file. You cannot use local file system as Alluxio's under storage system if there
are multiple nodes in the cluster. Instead you need to set up a shared storage to which all Alluxio servers
have access. The shared storage can be network file system (NFS), HDFS, S3, and so on. For example,
you can refer to [Configuring Alluxio with S3](Configuring-Alluxio-with-S3.html) and follow its
instructions to set up S3 as Alluxio's under storage.

Finally, sync all the information to the worker nodes. You can use

{% include Running-Alluxio-on-a-Cluster/sync-info.md %}

to sync files and folders to all hosts specified in the `alluxio/conf/workers` file. If you have
downloaded and extracted Alluxio tar file on the master only, you can use the `copyDir` command
to sync the entire extracted Alluxio directory to workers. You can also use this command
to sync any change to `conf/alluxio-site.properties` to the workers.

## Start Alluxio

Now, you can start Alluxio:

{% include Running-Alluxio-on-a-Cluster/start-Alluxio.md %}

To verify that Alluxio is running, you can visit `http://<alluxio_master_hostname>:19999`, check the
log in the directory `alluxio/logs`, or run a sample program:

{% include Running-Alluxio-on-a-Cluster/run-tests.md %}

**Note**: If you are using EC2, make sure the security group settings on the master node allows
 incoming connections on the alluxio web UI port.
