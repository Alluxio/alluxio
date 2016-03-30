---
layout: global
title: Debugging Guide
group: Resources
---

* Table of Contents
{:toc}

This page is a collection of high-level guides and tips regarding how to diagnose issues encountered in
Alluxio.

Note: this doc is not intended to be the full list of Alluxio questions.
Feel free to post questions on the [Alluxio Mailing List](https://groups.google.com/forum/#!forum/alluxio-users).

## Where are the Alluxio logs?

Alluxio generates Master, Worker and Client logs under the dir `{ALLUXIO_HOME}/logs`. They are
named as `master.log`, `master.out`, `worker.log`, `worker.out` and `user.log`.

The master and worker logs are very useful to understand what happened in Alluxio Master and
Workers, when you ran into any issues. If you do not understand the error messages,
try to search them in the [Mailing List](https://groups.google.com/forum/#!forum/alluxio-users),
in case the problem has been discussed before.

## Alluxio Setup FAQ

#### Q: I'm new to Alluxio and getting started. I failed to set up Alluxio on my local machine. What shall I do?

A: First check `{ALLUXIO_HOME}/logs` to see if there are any master or worker logs. Follow the clue
indicated by the error logs. Otherwise please double check if you missed any configuration
steps in [Running-Alluxio-Locally](Running-Alluxio-Locally.html).

Typical issues:
- `ALLUXIO_UNDERFS_ADDRESS` is not configured correctly.
- If `ssh localhost` failed, make sure the public ssh key for the host is added in `~/.ssh/authorized_keys`.

#### Q: I'm trying to deploy Alluxio in a cluster with Spark/HDFS. Are there any suggestions?

A: Please follow [Running-Alluxio-on-a-Cluster](Running-Alluxio-on-a-Cluster.html),
[Configuring-Alluxio-with-HDFS](Configuring-Alluxio-with-HDFS.html).

Tips:
- Usually, the best performance gains occur when Alluxio workers are co-located with the nodes of the computation frameworks.
- You can use Mesos and Yarn integration if you are already using Mesos or Yarn to manage your cluster. Using Mesos or Yarn can benefit management.
- If the under storage is remote (like S3 or remote HDFS), using Alluxio can be especially beneficial.

#### Q: I'm having problems setting up Alluxio cluster on EC2. Can you advice?

A: Please follow [Running-Alluxio-on-EC2.html](Running-Alluxio-on-EC2.html) for details.

Typical issues:
- Please make sure AWS access keys and Key Pairs are set up.
- If the UnderFileSystem is S3, check the S3 bucket name in `ufs.yml` is the name of an existing bucket, without the `s3://` or `s3n://` prefix.
- If you are not able to access the UI, please check that your security group allows incoming traffic on port 19999.

## Alluxio Performance FAQ

#### Q: I tested Alluxio/Spark against HDFS/Spark (running simple word count of GBs of files). There is no discernible performance difference. Why?

A: Alluxio accelerates your system performance by leveraging temporal or spatial locality using distributed in-memory storage
(and tiered storage). If your workloads don't have any locality, you will not see tremendous performance boost. 

## Environment

Alluxio can be configured under a variety of modes, in different production environments.
Please make sure the Alluxio version being deployed is update-to-date and supported.

When posting questions on the [Mailing List](https://groups.google.com/forum/#!forum/alluxio-users),
please attach the full environment information, including
- Alluxio version
- OS version
- Java version
- UnderFileSystem type and version
- Computing framework type and version
- Cluster information, e.g. the number of nodes, memory size in each node, intra-datacenter or cross-datacenter