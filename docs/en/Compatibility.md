---
layout: global
title: Compatibility 
nickname: Compatibility 
group: Features
priority: 1
---

* Table of Contents
{:toc}

This document lists features that are not backward compatible. 

# Journal
Alluxio journal needs to be upgraded manually when upgrading Alluxio cluster to >= 1.5.0 from any 
Alluxio cluster < 1.5.0 by running the following commands in the home directory of the new cluster. 

```bash
# Back up the v0 journal to avoid losing data in case of failures while running journal upgrader.
$ cp -r YourJournalDirectoryV0 YourJournalBackupDirectory
# Upgrade the journal.
$ ./bin/alluxio upgradeJournal -journalDirectoryV0 YourJournalDirectoryV0 
```

# Alluxio client and server 
Alluxio 1.5.0 introduces a new data transfer protocol between the client and Alluxio workers 
which makes Alluxio client and server incompatible with Alluxio clusters < 1.5.0. So the 
Alluxio client and server needs to be upgraded together when upgrading Alluxio 
cluster to >= 1.5.0 from Alluxio cluster < 1.5.0.
