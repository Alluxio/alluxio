---
layout: global
title: Running Shark on Tachyon
---

The additional prerequisite for this part is [Shark](https://github.com/amplab/shark/wiki) (0.7 or
later). We also assume that the user is running on Tachyon 0.3.0 or later and have set up Tachyon
and Hadoop in accordance to these guides [Local Mode](Running-Tachyon-Locally.html) or [Cluster Mode
](Running-Tachyon-on-a-Cluster.html).

Shark 0.7 adds a new storage format to support efficiently reading data from
[Tachyon](http://tachyonproject.org), which enables data sharing and isolation across instances of
Shark. Our meetup
[slide](http://files.meetup.com/3138542/2013-05-09%20Shark%20%40%20Spark%20Meetup.pdf) gives a good
overview of the benefits of using Tachyon to cache Shark's tables. In summary, the followings are
four major ones:

-   In-memory data sharing across multiple Shark instances (i.e. stronger isolation)
-   Instant recovery of in-memory tables
-   Reduce heap size =\> faster GC in shark
-   If the table is larger than the memory size, only the hot columns will be cached in memory

### Setup

In order to use Spark on Tachyon, you need to setup `Tachyon 0.3.0`
first, either [Local
Mode](https://github.com/amplab/tachyon/wiki/Running-Tachyon-Locally),
or [Cluster
Mode](https://github.com/amplab/tachyon/wiki/Running-Tachyon-on-a-Cluster),
with HDFS.

Then, edit `shark-env.sh` and add

    export TACHYON_MASTER="TachyonMasterHost:TachyonMasterIp"
    export TACHYON_WAREHOUSE_PATH=/sharktables

### Cache Shark table in Tachyon

##### Specify TBLPROPERTIES(“shark.cache” = “tachyon”), for example:

    CREATE TABLE data TBLPROPERTIES(“shark.cache” = “tachyon”) AS SELECT a, b, c from data_on_disk WHERE month=“May”;

##### Specify table's name ending with \_tachyon, for example:

    CREATE TABLE orders_tachyon AS SELECT * FROM orders;

After creating the table in Tachyon, you can query it like query normal tables.
