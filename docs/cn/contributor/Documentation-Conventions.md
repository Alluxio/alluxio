---
layout: global
title: 文档规范
nickname: 文档规范
group: Contributor Resources
priority: 4
---

* 内容列表
{:toc}

该文档提供了书写交付专业简明Alluxio技术文档指南。

## The C's

按重要顺序：
1. 正确 (Correctness)
2. 简洁 (Conciseness)
3. 一致(Consistency)
4. 正式(Ceremonialism)


### 正确性 = 没有错误

不正确的文档还不如没有文件。

* 传达准确信息
* U使用拼写检查器修正拼写错误
* 首字母缩写词大写
    * 例如 AWS, TTL, UFS, API, URL, SSH, I/O
* 专有名词大写
    * 例如 Alluxio, Hadoop, Java

### 简洁 = 不要使用多余的词汇

不是用来传达必要信息的词汇没有人愿意读。

* 用[祈使语气](https://en.wikipedia.org/wiki/Imperative_mood)，与发命令时用的口气一样
    * “运行命令以启动进程”
    * 而**不是**“下一步，您可以运行命令以启动该进程”
    * “在配置中包括一个SocketAppender ...”
    * 而**不是**“ SocketAppender可以被包含在配置中……”
* 使用[主动语态](https://en.wikipedia.org/wiki/Active_voice)
    * “配置错误时该进程失败”
    * 而**不是**“配置错误时该过程将失败”
    * 而**不是**“众所周知配置错误会导致启动进程失败”
* 不要使用不必要的标点符号
    * 避免使用括号来降低一个部分的重要性
        * 错误样例：“Alluxio做为生态系统中的新数据访问层，位于任何持久性存储系统（例如Amazon S3，Microsoft Azure对象存储，Apache HDFS或OpenStack Swift）和计算框架之间（例如Apache Spark，Presto， 或Hadoop MapReduce）。”
* 减少不添加内容的从句的使用
    * 删除以下用法:
        * 例如， …
        * 然而，…
        * 首先，...

### 一致性 = 请勿使用同一个字词或概念的不同形式

在整个技术文档中会使用许多技术术语，如果以多种方式表达同一想法，则可能会引起不必要的迷惑。

* 请参阅下面的术语表
    * 如果有疑问，请搜索以查找类似的文档如何表达相同的术语。
* 应使用反引号注释类似代码的文本
    * 文件路径
    * 属性键和值
    * Bash命令或标志
* C代码块应使用相关的文件或用法类型对进行注释，例如：
    * ```` ```java```` 为Java源代码
    * ```` ```properties```` 为Java属性文件
    * ```` ```console```` 为一个shell中的交互式时域
    * ```` ```bash```` 为shell脚本
* 以Alluxio开头的术语，例如命名空间，缓存或存储，应以“ the”开头，以区别于常用术语，但如果不是专有名词，则保持小写
    * 例如数据将被复制到 the Alluxio 存储中。... 
    * 例如当新文件添加到 the Alluxio 命名空间后，...
    * 例如The Alluxio master从不直接读取或写入数据 ...

### 正式 = 不要听起来像是随意的对话

文档不是对话。请不要用类似与他人闲聊的语气。

列出项目时，请使用串行逗号，也称为牛津逗号
* 例如：“ Alluxio与Amazon S3，Apache HDFS，和Microsoft Azure Object Store等存储系统集成。” 注意“ HDFS”之后的最后一个逗号。
* 避免适用缩略词；删除撇号并展开
    * Don’t -> Do not
* 用一个空格将一个句子的结束句号与下一个句子的开始字符分开； 这是自1950年代就有的规范。
避免适用缩略词
    * Doc -> Documentation


## 术语表

| 正确术语 | 非精准术语 |
|-------------------------|-----------------------------------------------------|
| File system | Filesystem |
| Leading master | Leader, lead master, primary master |
| Backup master | Secondary master, following master, follower master |
| Containerized | Dockerized |
| Superuser | Super-user, super user |
| I/O | i/o, IO |
| High availability mode | Fault tolerance mode (Use of "fault tolerance" is fine, but not when interchangeable with “high availability”) |
| Hostname | Host name |

## 换行

每个句子都另起一行，以方便查看差异。 
对于文档文件，我们没有针对文档的每行字符数限制，但是建议随时将句子分行以方便阅读，避免读者在阅读时做不必要的水平滚动。


## 资源

* [技术写作风格准则](https://en.wikiversity.org/wiki/Technical_writing_style)
* [不同情绪的样例](https://en.oxforddictionaries.com/grammar/moods)
* [主动与被动语气样例](https://writing.wisc.edu/Handbook/CCS_activevoice.html)
