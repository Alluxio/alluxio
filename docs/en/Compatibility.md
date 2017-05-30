---
layout: global
title: Compatibility 
nickname: Compatibility 
group: Features
priority: 1
---

* Table of Contents
{:toc}

This document lists features introduced in {{site.ALLUXIO_RELEASED_VERSION}} that are incompatible 
with the previous Alluxio versions. By default, it only lists features that are incompatible with
the last major Alluxio version.

# Journal
Alluxio journal needs to be upgraded manually when upgrading Alluxio cluster to 
{{site.ALLUXIO_RELEASED_VERSION}} from any Alluxio cluster deployed with vesion < 1.5.0 by running
the following command in the home directory of the new cluster. 

```bash
$ ./bin/alluxio upgradeJournal -journalDirectoryV0 YourJournalDirectoryV0 
```

It is strongly recommended to backup the old journal (journal v0) before running the journal
upgrade tool to avoid losing any data in case of failures.

# Alluxio client and server 
Alluxio {{site.ALLUXIO_RELEASED_VERSION}} introduces a new data transfer protocol between the
client and Alluxio workers which makes Alluxio client and server incompatible with older Alluxio 
clusters. So the Alluxio client and server needs to be upgraded together when upgrading Alluxio 
cluster to {{site.ALLUXIO_RELEASED_VERSION}} from Alluxio cluster with version < 1.5.0.
