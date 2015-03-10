---
layout: global
title: Tachyon FAQ
---

## Tachyon Frequently Asked Questions.

1. What is Tachyon?
[Tachyon](http://tachyon-project.org/index.html) is an in-memory distributed, reliable storage system enabling data sharing across cluster at memory speed. Data reliability is achieved by using lineage concept. In memory speed is acheived by caching working file sets, avoiding performing disk reads.

2. What platforms and Java versions does Tachyon run on?
Currently Tachyon has been tested with Oracle Java 1.7 running on various distributions of Linux.

3. Is there an easy way to see the status and health of a cluster?
Yes, Once Tachyon is installed, please login into [http://master_ip:19999](http://master_ip:19999)

4. If I add new nodes to the cluster, will tachyon rebalance/move the cached blocks to the newly added nodes in order to balance disk space utilization between the nodes?
No, Currently re-balancing of data blocks in not available.

5. How do you achieve Tachyon master redundancy?
We use Tachyon Master/Stand-by architecture. Also, zookeeper can be used to pick a Tachyon master.

6. What is underlayer FS and how many Underlayer FS are supported?
Underlayer FS is a general distributed filesystems on which tachyon works seamlessly used as long term storage for fault tolerant and to re-compute missing data. Tachyon inherently is not a filesystem.

7. Can I easily add support to other UnderFS?
Yes, in fact support for other Under FS is in progress by many contributors.

8. In which language is Tachyon developed?
Tachyon is primarily developed in Java and exposes Java-like File API's for other applications to interact with.

9. What happens if my data set does not fit in memory?
If the file does not fit in memory, then the file will not be cached, but at the same time all operations are successfully performed.

10. Can I run Tachyon as a standalone for experimenting and validation?
Yes, Tachyon can be run in a stand-alone mode.Instructions are available [here](http://tachyon-project.org/Running-Tachyon-Locally.html)

11. Can I run Tachyon in a cluster mode?
Yes, Tachyon can be run in a cluster mode. Instructions are available [here](http://tachyon-project.org/Running-Tachyon-on-a-Cluster.html)

12. Does Tachyon always needs HDFS?
No, Tachyon runs on different underlayer storage systems like HDFS, Amazon S3, GlusterFS. Instructions on configuration is [available](http://tachyon-project.org/Setup-UFS.html).

13. Does tachyon work along side with other Frameworks?
Yes, Tachyon works with [hadoop](http://hadoop.apache.org/), [Spark](http://spark.apache.org/), [HBase](http://hbase.apache.org/), [Flink](http://flink.apache.org/), Shark

14. How can I learn more about Tachyon?
Here are some of the links to presentations from previous summits.
		[Strata Conference 2014](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-10-16-Strata.pdf)
		[Spark Summit 2014](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-06-30_Spark_Summit.pdf)
		[Haoyuan Talk](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2013-10-28_Strata.pdf)

	Also, please look into some of videos presented by Haoyuan Li
	[Video 1](https://www.youtube.com/watch?v=4lMAsd2LNEE)
	[Video 2](https://www.youtube.com/watch?v=wJ2ocPoj9rs)

15. Are there any meetups for tachyon?
Yes, We also have started meetup group for Tachyon and please join [http://www.meetup.com/Tachyon/](http://www.meetup.com/Tachyon/). Stay Tuned for upcoming meetup.

16. What license is Tachyon under?
Tachyon is under the Apache 2.0 license.

17. I am a beginner and How can I contribute to Tachyon?
Thank you for the interest to contribute. Please look into [Startup-Tasks-for-New-Contributors](http://tachyon-project.org/master/Startup-Tasks-for-New-Contributors.html).
Also, please look into some of the JIRA bugs (marked for Beginners) [here](https://tachyon.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20Beginner)
To gain access to [JIRA] https://tachyon.atlassian.net/browse/TACHYON) bugs, you should login.

18. Where can I report issues related to Tachyon?
We use [JIRA](https://tachyon.atlassian.net/browse/TACHYON) to track development and issues. If you would like to report an issue, post it to the JIRA Tachyon issue tracker. You need to register before posting a bug.

19. Where can I get more help?
For any questions related installation, contribution and comments please post an email to [Tachyon User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/tachyon-users).
We are happy to help.
