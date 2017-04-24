---
layout: global
title: FAQ
nickname: FAQ
group: User Guide
priority: 2
---

## Alluxio Frequently Asked Questions.

1. What is Alluxio?

   [Alluxio](http://www.alluxio.org/), formerly Tachyon, is an open source, memory speed, virtual
   distributed storage. It enables any application to interact with any data from any storage
   system at memory speed.

2. What platforms and Java versions does Alluxio run on?

   Alluxio can run with Java 1.7 or above on various distributions of Linux / Mac.

3. Is there an easy way to see the status and health of a cluster?

   Yes, Once Alluxio is installed, the UI is available at `http://master_ip:19999`

4. If I add new nodes to the cluster, will Alluxio re-balance / move the cached blocks to the newly
added nodes in order to balance memory space utilization between the nodes?

   No, Currently re-balancing of data blocks in not available.

5. Is Alluxio master fault tolerant?

   Yes. Please see this [page](Running-Alluxio-Fault-Tolerant.html).

6. What are under storage systems and how many under storage systems are supported?

   Alluxio uses under storage systems as its persistent storage system. It currently supports [Amazon
   S3](Configuring-Alluxio-with-S3.html), [Swift](Configuring-Alluxio-with-Swift.html), [GCS
   ](Configuring-Alluxio-with-GCS.html), [HDFS](Configuring-Alluxio-with-HDFS.html) and many more.

7. Can I easily add support for other under store systems?

   Yes, in fact support for other under storages is in progress by many contributors. Here is the
   [introduction](Integrating-Under-Storage-Systems.html) to adding other under storage systems.

8. In which language is Alluxio developed?

   Alluxio is primarily developed in Java and exposes Java-like File APIs for other applications to
   interact with. We are working on other language bindings though.

9. What happens if my data set does not fit in memory?

   Depends on the system setup, Alluxio may leverage local SSD and HDD. It keeps hot data in
   Alluxio, and cold data in under storage systems. You can read more about tiered storage setup 
   [here](Tiered-Storage-on-Alluxio.html).

10. Can I run Alluxio as a stand-alone system for experimenting and validation?

    Yes, Alluxio can run as a stand-alone system. Instructions are available [here](Running-Alluxio-Locally.html).

11. Can I run Alluxio in a cluster mode?

    Yes, Alluxio can be run in a cluster mode. Instructions are available [here](Running-Alluxio-on-a-Cluster.html).

12. Does Alluxio always need HDFS?

    No, Alluxio runs on different under storage systems like HDFS, Amazon S3, Swift and GlusterFS.

13. Does Alluxio work alongside with other Frameworks?

    Yes, Alluxio works with [Spark](Running-Spark-on-Alluxio.html), [Flink](Running-Flink-on-Alluxio.html),
    [Hadoop](Running-Hadoop-MapReduce-on-Alluxio.html),  [HBase](Running-HBase-on-Alluxio.html),
    [Hive](Running-Hive-with-Alluxio.html), etc.

14. How can I learn more about Alluxio?

    You can read the recent [blogs](/resources/posts) and [presentations](/resources/presentations).

15. Are there any meetups for Alluxio?

    Yes. We have started meetup group for Alluxio and please join
    [http://www.meetup.com/Alluxio/](http://www.meetup.com/Alluxio/). You can also find more
    Alluxio events [here](/resources/events).

16. What license is Alluxio under?

    Alluxio is under the Apache 2.0 license.

17. How can I contribute to Alluxio?

    Thank you for the interest to contribute. Please look into [Contributing-to-
    Alluxio](/contribute). Also, please look into some of the JIRA issues (marked for New
    Contributors) [here](https://alluxio.atlassian.net/browse/ALLUXIO-2532?jql=project%20%3D%20ALLUXIO%20AND%20status%20%3D%20Open%20AND%20labels%20%3D%20NewContributor%20AND%20assignee%20in%20(EMPTY))

18. Where can I report issues or propose new features?

    We use [JIRA](https://alluxio.atlassian.net/projects/ALLUXIO) to track development and issues.
    If you would like to report an issue or propose a feature, post it to the JIRA Alluxio issue
    tracker. You need to register before posting.

19. Where can I get more help?

    For any questions related installation, contribution and comments please post an email to
    [Alluxio User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users).
    We look forward to seeing you there.
