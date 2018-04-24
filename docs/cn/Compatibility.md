---
layout: global
title: 兼容性
nickname: 兼容性
group: Features
priority: 1
---

* 内容列表
{:toc}

这个文件列出了不向后兼容的特性。

# 日志

当Alluxio集群从1.5.0之前的版本升级到1.5.0之后的版本，你需要在新集群的home目录下运行以下命令来手动升级Alluxi日志。

```bash
# Back up the v0 journal to avoid losing data in case of failures while running journal upgrader.
$ cp -r YourJournalDirectoryV0 YourJournalBackupDirectory
# Upgrade the journal.
$ ./bin/alluxio upgradeJournal -journalDirectoryV0 YourJournalDirectoryV0 
```

# Alluxio客户端和服务器

Alluxio 1.5.0在客户端和worker节点之间引入了新的数据传输协议，这使得客户端与1.5.0版本之前的Alluxio集群中的服务器不兼容。
所以客户端和服务器需要一起从1.5.0之前的版本升级到1.5.0之后的版本。
