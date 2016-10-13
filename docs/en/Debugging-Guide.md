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

Alluxio generates Master, Worker and Client logs under the dir `${ALLUXIO_HOME}/logs`. They are
named as `master.log`, `master.out`, `worker.log`, `worker.out` and `user_${USER}.log`.

The master and worker logs are very useful to understand what happened in Alluxio Master and
Workers, when you ran into any issues. If you do not understand the error messages,
try to search them in the [Mailing List](https://groups.google.com/forum/#!forum/alluxio-users),
in case the problem has been discussed before.

## Setup FAQ

#### Q: I'm new to Alluxio and getting started. I failed to set up Alluxio on my local machine. What shall I do?

A: First check `${ALLUXIO_HOME}/logs` to see if there are any master or worker logs. Follow the clue
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
- If the UnderFileSystem is S3, check the S3 bucket name in `ufs.yml` is the name of an existing
bucket, without the `s3://`, `s3a://`, or `s3n://` prefix.
- If you are not able to access the UI, please check that your security group allows incoming traffic on port 19999.


## Usage FAQ

#### Q: I'm seeing error messages like "Frame size (67108864) larger than max length (16777216)". What is wrong?

A: This problem can be caused by different possible reasons.

- Please double-check if the port of Alluxio master address is correct. The default listening port for Alluxio master is port 19998,
while a common mistake causing this error message is due to using a wrong port in master address(e.g., using port 19999 which is the default Web UI port for Alluxio master).
- Please ensure that the security settings of Alluxio client and master are consistent.
Alluxio provides different approaches to [authenticate](Security.html#authentication) users by configuring `alluxio.security.authentication.type`.
This error happens if this property is configured with different values across servers and clients
(e.g., one uses the default value `NOSASL` while the other is customized to `SIMPLE`).
Please read [Configuration-Settings](Configuration-Settings.html) for how to customize Alluxio clusters and applications.

#### Q: I'm copying or writing data to Alluxio while seeing error messages like "Failed to cache: Not enough space to store block on worker". Why?

A: This error indicates insufficient space left on Alluxio workers to complete your write request.

- If you are copying a file to Alluxio using `copyFromLocal`, by default this shell command applies `LocalFirstPolicy`
and stores data on the local worker (see [location policy](File-System-API.html#location-policy)).
In this case, you will see the above error once the local worker does not have enough space.
To distribute the data of your file on different workers, you can change this policy to `RoundRobinPolicy` (see below).

```bash
$ bin/alluxio fs -Dalluxio.user.file.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy copyFromLocal foo /alluxio/path/foo
```

- Check if you have any files unnecessarily pinned in memory and unpin them to release space.
See [Command-Line-Interface](Command-Line-Interface.html) for more details.
- Increase the capacity of workers by changing `alluxio.worker.memory.size` property.
See [Configuration](Configuration-Settings.html#common-configuration) for more description.

## Performance FAQ

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
