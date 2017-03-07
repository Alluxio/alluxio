---
layout: global
title: FAQ
nickname: FAQ
group: User Guide
priority: 2
---

## Alluxio 常见问题

1. 什么是Alluxio?

   [Alluxio](http://www.alluxio.org/)（之前名为Tachyon），是一个开源的具有内存级速度的虚拟分布式存储系统，
   使得应用程序可以以内存级速度与任何存储系统中的数据进行交互。

2. Alluxio可以运行在哪些平台和Java版本上?

   Alluxio可以在Linux / Mac的各种发行版上使用Java 1.7或更高版本运行。

3. 是否有简单的方法来查看群集的状态和运行状况？
  
   是的，一旦安装好Alluxio，可以通过访问`http://master_ip:19999`的UI界面来查看。

4. 如果向集群添加新节点，为了平衡节点之间的内存空间利用率，Alluxio是否会进行重新平衡（即将缓存的块移动到新节点）？

   否，当前还未实现对数据块的重新平衡。
   
5. Alluxio master节点是否具备容错能力？

   是的。请参考[此页](Running-Alluxio-Fault-Tolerant.html)。

6. 什么是底层存储系统？Alluxio支持多少种底层存储系统？

   Alluxio 使用底层存储系统作为其持久化存储系统，当前支持 [Amazon S3](Configuring-Alluxio-with-S3.html), 
   [Swift](Configuring-Alluxio-with-Swift.html), [GCS](Configuring-Alluxio-with-GCS.html), [HDFS](Configuring-Alluxio-with-HDFS.html)等其他存储系统。

7. 是否可以支持其他底层存储系统？

   可以，事实上许多Alluxio贡献者正在进行相关工作以支持其他底层存储系统。请参考该[介绍](Integrating-Under-Storage-Systems.html)。
   
8. Alluxio使用何种语言进行开发？
   
   Alluxio主要使用Java语言进行开发，并为其他应用程序提供类似Java文件的API以进行交互。而我们也正在进行对其他语言的绑定工作。

9. 如果数据集不适合存储在内存中该怎么办？

   这取决于系统设置。Alluxio 会使用本地SSD和HDD进行存储，热数据被保存在Alluxio中而冷数据被保存在底层存储系统中。
   可以[在此](Tiered-Storage-on-Alluxio.html)阅读有关分层存储设置的更多信息。
   
10. Alluxio可以作为一个单机系统运行以进行实验和验证吗？

    是的，Alluxio可以作为一个单机系统运行。[这里](Running-Alluxio-Locally.html)是操作指南。

11. Alluxio可以以集群模式运行吗？

    是的，Alluxio可以以集群模式运行。[这里](Running-Alluxio-on-a-Cluster.html)是操作指南。

12. Alluxio必须在HDFS上运行吗？

    不是的，Alluxio可以运行在不同的底层存储系统上，如HDFS，Amazon S3，Swift和GlusterFS等。

13. Alluxio可以和其他框架一起工作吗？

    是的, Alluxio 可以和[Spark](Running-Spark-on-Alluxio.html), [Flink](Running-Flink-on-Alluxio.html), [Hadoop](Running-Hadoop-MapReduce-on-Alluxio.html),
      [HBase](Running-HBase-on-Alluxio.html), [Hive](Running-Hive-with-Alluxio.html)等框架一起工作。

14. 如何了解Alluxio更多相关信息？

    可以阅读近期的[博客](/resources/posts) 和[介绍](/resources/presentations)。

15. Alluxio是否有meetup等活动？
   
    是的，我们已经为Alluxio发起了meetup组织，请加入[http://www.meetup.com/Alluxio/](http://www.meetup.com/Alluxio/)。
    也可以通过[这里](/resources/events)找到更多Alluxio相关的活动。

16. Alluxio使用什么许可证？

    Alluxio使用Apache 2.0许可证。

17. 如何为Alluxio贡献代码？

    非常感谢您的关注。您可以查看 [Contributing-to-Alluxio](/contribute)，还可以
    [在此](https://alluxio.atlassian.net/browse/ALLUXIO-2532?jql=project%20%3D%20ALLUXIO%20AND%20status%20%3D%20Open%20AND%20labels%20%3D%20NewContributor%20AND%20assignee%20in%20(EMPTY))查看JIRA上的一些问题（标注给新贡献者的）。

18. 在哪里可以报告问题或提出新功能？

    我们使用[JIRA](https://alluxio.atlassian.net/projects/ALLUXIO)来跟进开发过程和相关问题。如果您想报告问题或提出新功能，请将其发布到JIRA上的Alluxio问题跟进器上。您需要在发布之前注册。

19. 在哪里可以获取更多的帮助？

    对于任何与安装、代码贡献和评价相关的问题，请发送邮件到[Alluxio 用户邮件列表](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)。
    我们期待在那儿与您探讨。
