---
layout: global
title: Debugging Guide
group: User Guide
priority: 0
---

* Table of Contents
{:toc}

This page is a collection of high-level guides and tips regarding how to diagnose issues encountered in
Alluxio. Note: this doc is not intended to be the full list of Alluxio questions.
Feel free to post questions on Alluxio [Mailing List](https://groups.google.com/forum/alluxio-users)

### Environment

Alluxio can be configured under a variety of modes, in different production environments.
Please make sure the Alluxio being deployed, is most recent stable/supported version.

When you post questions on mailing list, please attach the full environment information, including
- Alluxio version
- OS version
- Java version
- UnderFileSystem type and version
- Computing framework type and version
- Cluster information 

### Where are the Alluxio logs?

Alluxio generates Master, Worker and Client logs under the dir `{ALLUXIO_HOME}/logs`. They are
named as master.log, master.out worker.log, worker.out and user.log.

The master and worker logs are always useful to understand what happened in Alluxio Master and
Workers, when you ran into any issues. Try to search the error message in the user
mailing list, to see if the problem has been discussed before.

### Alluxio Setup FAQ

Q: I'm new to Alluxio and trying to get started. But I failed to set up Alluxio on my local
machine. What shall I do?

A: First check `{ALLUXIO_HOME}/logs` to see if there are any master or worker logs. If the logs
show some errors, follow the clue. Otherwise please double check if you missed any configuration
steps in [Running-Alluxio-Locally](Running-Alluxio-Locally.html).

Typical mis-configurations: 
- `ALLUXIO_UNDERFS_ADDRESS` is not configured correctly;
- Could not `ssh localhost` . Please make sure the public ssh key for the host is added in `~/
.ssh/authorized_keys`

Q: I'm trying to deploy Alluxio in a cluster with Spark/HDFS. Can you advice? 

A: Please follow [Running-Alluxio-on-a-Cluster](Running-Alluxio-on-a-Cluster.html),
[Configuring-Alluxio-with-HDFS](Configuring-Alluxio-with-HDFS.html). 

Tips: 
- Usually, the best performance gains occur when Alluxio workers are co-located with the nodes of the computation frameworks.
- Also, using Alluxio can be very beneficial if/when the under storage is remote (like S3 or remote HDFS).
- You can use mesos and yarn if you are already using mesos or yarn to manage your cluster. Using mesos or yarn can benefit management.

Q: I want to set up Alluxio cluster on EC2, with default UnderFileSystem as S3. What shall I do?

A: Please follow [Running-Alluxio-on-EC2.html](Running-Alluxio-on-EC2.html) for details.

Typical mis-configurations:
- Please make sure AWS access keys and Key Pairs are set up coordinately.
- Check the S3 bucket name in `ufs.yml` is the name of an existing bucket, without the s3:// or
s3n:// prefix.
- If you are not able to access the UI, please check that your security group allows
incoming traffic on port 19999.

### Alluxio Performance FAQ

Q: I tested alluxio/Spark against HDFS/Spark (running simple work count of GBs of files). There is
no discernible performance difference. Why?

A: Alluxio accelerates your system performance by leveraging data locality using distributed in-memory storage
(and tiered storage). If your workloads do not have good data locality, and most accesses still need to go to
the UnderFileSystem, you will not see tremendous performance difference.