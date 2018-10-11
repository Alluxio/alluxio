---
layout: global
title: Frequently Asked Questions
nickname: FAQ
group: Home
priority: 0
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

## My analytics job does not get faster after deploying Alluxio, why?

There are multiple possible reasons to check out:
1. The job can be computation bound and does not spend too much time in reading/writing data.
In this case, speeding up I/O performance which is not the bottleneck will help less.
1. The persistent storage is colocated with compute (e.g., Alluxio is connected to a local
HDFS), and the input data of the job is in the OS
[buffer cache](https://www.tldp.org/LDP/sag/html/buffer-cache.html).
1. Due to some mis-configuration, tasks of this job are not reading from their local Alluxio workers
but from remote Alluxio workers through network, resulting in low data-locality.
1. Input data is not loaded into Alluxio yet or already evicted out, the job is not really reading
from Alluxio cache but from the under storage.

## Shall I run Alluxio as a stand-alone system or on YARN, Mesos or Kubernetes?

We recommend to run Alluxio as a stand-alone system.  We support running Alluxio on [YARN]({{
site.baseurl }}{% link en/deploy/Running-Alluxio-On-Yarn.md %}), [Mesos]({{ site.baseurl }}{% link
en/deploy/Running-Alluxio-on-Mesos.md %}), and [Kubernetes]({{ site.baseurl }}{% link
en/deploy/Running-Alluxio-On-Kubernetes.md %}).

## Which programming language does Alluxio support?

Alluxio is primarily developed in Java and exposes Java-like File APIs for other applications to
interact with. Alluxio supports other language bindings including [Python client]({{ site.baseurl
}}{% link en/api/FS-API.md %}#python), [Golang client]({{ site.baseurl }}{% link en/api/FS-API.md
%}#go).

## What happens if my data set does not fit in memory?

- It is not required for the input data set to fit in Alluxio storage space in order for
applications to work. Alluxio will transparently and dynamically load data on demand from under
storage.
- To help more hot data fit in Alluxio storage space, you can configure Alluxio to leverage storage
resource like SSD and HDD in addition to memory to extend Alluxio storage capacity. You can read
more about Alluxio storage setup [here](Alluxio-Storage.html).

## Does Alluxio support a fault tolerant mode?

Yes. Please see instructions about [Deploy Alluxio on a Cluster]({{ site.baseurl }}{% link
en/deploy/Running-Alluxio-On-a-Cluster.md %}).

## Will Alluxio re-balance / move the cached blocks to the newly added nodes in order to balance memory space utilization?

No, re-balancing of data blocks in Alluxio is not supported currently.

## How can I add support for other under store systems?

Yes, in fact support for other under storages is in progress by many contributors. Here is the
[documentation](DevelopingUFSExtensions.html) for adding other under storage systems.

## Does Alluxio always need HDFS?

No, Alluxio runs on different under storage systems like HDFS, Amazon S3, Swift.

## How can I learn more about Alluxio?

You can read the recent [blogs](/resources/posts) and [presentations](/resources/presentations).

We also hosts meetup group for Alluxio and please join
[http://www.meetup.com/Alluxio/](http://www.meetup.com/Alluxio/). You can also find more Alluxio
events [here]({{ site.baseurl }}/resources/events).

## Where can I report issues or propose new features?

We use [JIRA](https://alluxio.atlassian.net/projects/ALLUXIO) to track development and issues.  If
you would like to report an issue or propose a feature, post it to the JIRA Alluxio issue
tracker. You need to register before posting.

## Where can I get more help?

For any questions related installation, contribution and comments please post an email to [Alluxio
User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users).  We look
forward to seeing you there.

## How can I contribute to Alluxio?

Thank you for the interest to contribute. Please read [our contribute guide]({{ site.baseurl }}{%
link en/contributor/Contributor-Getting-Started.md %}).
