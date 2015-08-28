---
layout: global
title: FAQ
group: FAQ
---


**What is Tachyon?**

> [Tachyon](/) is a memory centric, distributed, reliable
   storage system enabling data sharing across clusters at memory speed.

**What platforms and Java versions does Tachyon run on?**

>   Tachyon can run with Java 1.6 or above on various distributions of Linux / Mac.

**Is there an easy way to see the status and health of a cluster?**

>   Yes, once Tachyon is installed, the UI is available at `http://master_ip:19999`

**If I add new nodes to the cluster, will tachyon re-balance / move the cached blocks to the
newly added nodes in order to balance disk space utilization between the nodes?**

>   No, currently re-balancing of data blocks is not available.

**Is Tachyon master fault tolerant?**

>   Yes. Please see this [page](/documentation/Fault-Tolerant-Tachyon-Cluster.html).

**What is UnderFS and how many UnderFS are supported?**

>   Tachyon uses UnderFS as its persistent storage system. It currently supports Amazon S3, HDFS,
   GlusterFS. Many more are coming.

**Can I easily add support to other UnderFS?**

>   Yes, in fact support for other UnderFS is in progress by many contributors. Here is the
   [introduction](/documentation/Setup-UFS.html) to adding other UnderFS.

**In which language is Tachyon developed?**

>   Tachyon is primarily developed in Java and exposes Java-like File API's for other applications
   to interact with. We are working on other language bindings though.

**What happens if my data set does not fit in memory?**

>   Depending on the system setup, Tachyon may leverage local SSD and HDD. It keeps hot data in
   Tachyon, and cold data in UnderFS.

**Can I run Tachyon in stand-alone mode for experimenting and validation?**

>    Yes, Tachyon can run in stand-alone mode. Instructions are available
    [here](/documentation/Running-Tachyon-Locally.html).

**Can I run Tachyon in a cluster mode?**

>    Yes, Tachyon can be run in a cluster mode. Instructions are available
    [here](/documentation/Running-Tachyon-on-a-Cluster.html).

**Does Tachyon always needs HDFS?**

>    No, Tachyon runs on different UnderFS like HDFS, Amazon S3, GlusterFS.
    Instructions on configuration are available [here](/documentation/Setup-UFS.html).

**Does Tachyon work along side with other frameworks?**

>    Yes, Tachyon works with [Hadoop](http://hadoop.apache.org/), [Spark](http://spark.apache.org/),
    [HBase](http://hbase.apache.org/), [Flink](http://flink.apache.org/), etc.

**How can I learn more about Tachyon?**

>    Here are some of the links to presentations from previous summits:
>
>    * [Strata Conference 2014](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-10-16-Strata.pdf)
>    * [Spark Summit 2014](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-06-30_Spark_Summit.pdf)
>    * [Strata Conference 2013](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2013-10-28_Strata.pdf)

**Are there any meetups for Tachyon?**

>    Yes. We have started a meetup group for Tachyon so please join
    [meetup.com/Tachyon](http://www.meetup.com/Tachyon/).
    Stay tuned for upcoming meetups.

**What license is Tachyon under?**

>    Tachyon is under the Apache 2.0 license.

**How can I contribute to Tachyon?**

>    Thank you for the interest to contribute. Please look into
    [Contributing to Tachyon](/documentation/master/Contributing-to-Tachyon.html).
    Also, please look into some of the JIRA issues (marked for Beginners)
    [here](https://tachyon.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20NewContributor%20AND%20status%20%3D%20OPEN).

**Where can I report issues or propose new features?**

>    We use [JIRA](https://tachyon.atlassian.net/browse/TACHYON) to track development and issues. If you
    would like to report an issue or propose a feature, post it to the JIRA Tachyon issue tracker. You
    need to register before posting.

**Where can I get more help?**

>    For any questions related to installation, contribution and comments please send an email to
    [Tachyon User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/tachyon-users). We
    look forward to seeing you there.
