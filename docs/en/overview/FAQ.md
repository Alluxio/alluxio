---
layout: global
title: Frequently Asked Questions
nickname: FAQ
group: Overview
priority: 5
---

* Table of Contents
{:toc}

## What is Alluxio?

[Alluxio](https://www.alluxio.io/), formerly Tachyon, is an open source, memory speed, virtual
distributed storage. It enables any application to interact with any data from any storage system
at memory speed. Read more about Alluxio [here]({{ '/en/Overview.html' | relativize_url }}).

## What platforms and Java versions can Alluxio run on?

Alluxio requires JDK 1.8 or JDK 11 to run on various distributions of Linux / MacOS.

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
- [Kubernetes]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url }})

## Which programming language does Alluxio support?

Alluxio is primarily developed in Java and exposes Java-like File APIs for other applications to
interact with. Alluxio supports other language bindings (experimental currently) including
[Python]({{ '/en/api/FS-API.html' | relativize_url }}#python) and
[Golang]({{ '/en/api/FS-API.html' | relativize_url }}#go).

Alluxio can be run as a FUSE mount exposing a [POSIX API]({{ '/en/api/POSIX-API.html' | relativize_url }}).
This enables any program which normally accesses a local file system to access data from Alluxio without
modification. This is a common way for applications written in non-Java languages or non-Hadoop APIs
to access Alluxio data without needing to rewrite the application.

## What happens if my data set does not fit in memory?

It is not required for the input data set to fit in Alluxio storage space in order for
applications to work. Alluxio will transparently load data on demand from the under storage.
To help fit more data in Alluxio's storage space, configure Alluxio to leverage other storage
resources such as SSD and HDD in addition to memory to extend Alluxio storage capacity.
Read more about Alluxio storage setup
[here]({{ '/en/core-services/Caching.html' | relativize_url }}).

## Does Alluxio support a high availability mode?

Yes. See instructions about
[Deploy Alluxio on a Cluster with HA]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url }}).

## Will Alluxio rebalance cached blocks to the newly added nodes in order to balance memory space utilization?

No, rebalancing of data blocks in Alluxio is not currently supported.

## How can I add support for other under store systems?

Support for other under storages is in progress by many contributors. See the
[documentation]({{ '/en/ufs/Ufs-Extension-API.html' | relativize_url }}) for adding other under storage
systems.

## Does Alluxio require HDFS?

No, Alluxio can run on many under storage systems such as Amazon S3 or Swift in addition to HDFS.

## How can I learn more about Alluxio?

Join the [Alluxio community Slack Channel](https://www.alluxio.io/slack) to chat with users and
developers.

Read the Alluxio book to learn Alluxio comprehensively.

<p align="center">
<a href="https://book.douban.com/subject/34761887">
<img style=" width: 25%;" src="{{ '/img/alluxio_book.png' | relativize_url }}" alt="Alluxio Book"/>
</a>
</p>

Read the recent [blogs](https://www.alluxio.io/blog) and
[presentations](https://www.alluxio.io/resources/presentations/).

Join the meetup group for Alluxio at
[http://www.meetup.com/Alluxio/](http://www.meetup.com/Alluxio/).
Other Alluxio events can be found [here](https://www.alluxio.io/events/).

## Where can I report issues or propose new features?

[Github Issues](https://github.com/alluxio/alluxio/issues) is used to track feature
development and issues.
To report an issue or propose a feature, post on the Github issue.

## Where can I get more help?

For any questions related to installation, contribution or feedback, please
join our [Alluxio community Slack Channel](https://www.alluxio.io/slack) or
send an email to the
[Alluxio User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users).
We look forward to seeing you there.

## How can I contribute to Alluxio?

Thank you for your interest in contributing. Please read
[our contributor guide]({{ '/en/contributor/Contributor-Getting-Started.html' | relativize_url }}).
