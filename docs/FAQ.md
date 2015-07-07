---
layout: global
title: Tachyon FAQ
---

## Tachyon Frequently Asked Questions.

1. What is Tachyon?

   [Tachyon](http://tachyon-project.org/index.html) is a memory centric distributed, reliable
   storage system enabling data sharing across cluster at memory speed.

2. What platforms and Java versions does Tachyon run on?

   Tachyon can run with Java 1.6 or above on various distributions of Linux / Mac.

3. Is there an easy way to see the status and health of a cluster?

   Yes, Once Tachyon is installed, the UI is available at `http://master_ip:19999`

4. If I add new nodes to the cluster, will tachyon re-balance / move the cached blocks to the
newly added nodes in order to balance disk space utilization between the nodes?

   No, Currently re-balancing of data blocks in not available.

5. Is Tachyon master fault tolerant?

   Yes. Please see this [page](http://tachyon-project.org/Fault-Tolerant-Tachyon-Cluster.html).

6. What is under FS and how many Underlayer FS are supported?

   Tachyon uses UnderFS as its persistent storage system. It currently support Amazon S3, HDFS,
   GlusterFS. Many more are coming.

7. Can I easily add support to other UnderFS?

   Yes, in fact support for other Under FS is in progress by many contributors. Here is the
   [introduction](http://tachyon-project.org/Setup-UFS.html) to adding other UnderFS.

8. In which language is Tachyon developed?

   Tachyon is primarily developed in Java and exposes Java-like File API's for other applications
   to interact with. We are working on other language binding though.

9. What happens if my data set does not fit in memory?

   Depends on the system setup, Tachyon may leverage local SSD and HDD. It keeps hot data in
   Tachyon, and cold data in UnderFS.

10. Can I run Tachyon as a standalone for experimenting and validation?

    Yes, Tachyon run in a stand-alone mode. Instructions are available
    [here](http://tachyon-project.org/Running-Tachyon-Locally.html).

11. Can I run Tachyon in a cluster mode?

    Yes, Tachyon can be run in a cluster mode. Instructions are available
    [here](http://tachyon-project.org/Running-Tachyon-on-a-Cluster.html).

12. Does Tachyon always needs HDFS?

    No, Tachyon runs on different UnderFS like HDFS, Amazon S3, GlusterFS.
    Instructions on configuration are [available](http://tachyon-project.org/Setup-UFS.html).

13. Does tachyon work along side with other Frameworks?

    Yes, Tachyon works with [Hadoop](http://hadoop.apache.org/), [Spark](http://spark.apache.org/),
    [HBase](http://hbase.apache.org/), [Flink](http://flink.apache.org/), etc.

14. How can I learn more about Tachyon?

    Here are some of the links to presentations from previous summits:

    * [Strata Conference 2014](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-10-16-Strata.pdf)
    * [Spark Summit 2014](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-06-30_Spark_Summit.pdf)
    * [Strata Conference 2013](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2013-10-28_Strata.pdf)

15. Are there any meetups for tachyon?

    Yes. We have started meetup group for Tachyon and please join
    [http://www.meetup.com/Tachyon/](http://www.meetup.com/Tachyon/).
    Stay Tuned for upcoming meetups.

16. What license is Tachyon under?

    Tachyon is under the Apache 2.0 license.

17. How can I contribute to Tachyon?

    Thank you for the interest to contribute. Please look into
    [Contributing-to-Tachyon](http://tachyon-project.org/master/Contributing-to-Tachyon.html).
    Also, please look into some of the JIRA issues (marked for Beginners)
    [here](https://tachyon.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20NewContributor%20AND%20status%20%3D%20OPEN)

18. Where can I report issues or propose new features?

    We use [JIRA](https://tachyon.atlassian.net/browse/TACHYON) to track development and issues. If you
    would like to report an issue or propose a feature, post it to the JIRA Tachyon issue tracker. You
    need to register before posting.

19. Where can I get more help?

    For any questions related installation, contribution and comments please post an email to
    [Tachyon User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/tachyon-users). We
    look forward seeing you there.
