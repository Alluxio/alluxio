---
layout: global
title: Frequently Asked Questions
nickname: FAQ
group: Home
priority: 10
---

* Table of Contents
{:toc}

## What is Alluxio?

[Alluxio](http://www.alluxio.org/), formerly Tachyon, is an open source, memory speed, virtual
distributed storage. It enables any application to interact with any data from any storage system
at memory speed. Read more about Alluxio [Overview]({{ site.baseurl }}{% link en/Overview.md %}).

## What platforms and Java versions can Alluxio run on?

Alluxio requires JVM 1.8 or above to run on various distributions of Linux / MacOS.

## What license is Alluxio under?

Alluxio is open sourced under the Apache 2.0 license.

## Why is my analytics job not running faster after deploying Alluxio?

Some possible reasons to consider:
1. The job is computation bound and does not spend significant time in reading or writing data.
Because the bottleneck is not in I/O performance, the benefit from faster Alluxio I/O is small.
1. The persistent storage is co-located with compute (e.g. Alluxio is connected to a local
HDFS) and the input data of the job is in the OS
[buffer cache](https://www.tldp.org/LDP/sag/html/buffer-cache.html).
1. Due to misconfiguration, clients are not able to identify their corresponding local Alluxio worker.
This results in reading from remote Alluxio workers through the network, resulting in low data-locality.
1. Input data is not loaded into Alluxio yet or already evicted, causing the job to read from the
under storage instead of the Alluxio cache.

## Should I deploy Alluxio as a stand-alone system or through an orchestration framework?

It is recommended to deploy Alluxio as a stand-alone system. Orchestration frameworks supported include:
- [YARN]({{site.baseurl }}{% link en/deploy/Running-Alluxio-On-Yarn.md %})
- [Mesos]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-On-Mesos.md %})
- [Kubernetes]({{ site.baseurl }}{% link en/deploy/Running-Alluxio-On-Kubernetes.md %})

## Which programming language does Alluxio support?

Alluxio is primarily developed in Java and exposes Java-like File APIs for other applications to
interact with. Alluxio supports other language bindings including [Python]({{ site.baseurl
}}{% link en/api/FS-API.md %}#python) and [Golang]({{ site.baseurl }}{% link en/api/FS-API.md
%}#go).

## What happens if my data set does not fit in memory?

It is not required for the input data set to fit in Alluxio storage space in order for
applications to work. Alluxio will transparently load data on demand from the under storage.
To help fit more data in Alluxio's storage space, configure Alluxio to leverage other storage
resources such as SSD and HDD in addition to memory to extend Alluxio storage capacity.
Read more about Alluxio storage setup
[here]({{site.baseurl}}{% link en/advanced/Alluxio-Storage-Management.md %}).

## Does Alluxio support a fault tolerant mode?

Yes. See instructions about [Deploy Alluxio on a Cluster]({{ site.baseurl }}{% link
en/deploy/Running-Alluxio-On-a-Cluster.md %}).

## Will Alluxio rebalance cached blocks to the newly added nodes in order to balance memory space utilization?

No, rebalancing of data blocks in Alluxio is not currently supported.

## How can I add support for other under store systems?

Support for other under storages is in progress by many contributors. See the
[documentation]({{site.baseurl}}{% link en/ufs/Ufs-Extensions.md %}) for adding other under storage
systems.

## Does Alluxio require HDFS?

No, in addition to HDFS, Alluxio can also run on different under storage systems such as Amazon S3 or Swift.

## How can I learn more about Alluxio?

Read the recent [blogs](https://alluxio.org/resources/posts) and
[presentations](https://alluxio.org/resources/presentations).

Join the meetup group for Alluxio at
[http://www.meetup.com/Alluxio/](http://www.meetup.com/Alluxio/).
Other Alluxio events can be found [here](https://alluxio.org/resources/events).

## Where can I report issues or propose new features?

[JIRA](https://alluxio.atlassian.net/projects/ALLUXIO) is used to track feature development and issues.
To report an issue or propose a feature, post on the JIRA Alluxio issue tracker.
Registration is required before posting.

## Where can I get more help?

For any questions related to installation, contribution or feedback, please send an email to the
[Alluxio User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users).
We look forward to seeing you there.

## How can I contribute to Alluxio?

Thank you for your interest in contributing. Please read [our contributor guide]({{ site.baseurl }}{%
link en/contributor/Contributor-Getting-Started.md %}).
