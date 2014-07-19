---
layout: global
title: Running Shark on Tachyon
---

The additional prerequisite for this part is [Shark](https://github.com/amplab/shark/wiki).
We also assume that the user has set up Tachyon and Hadoop in accordance to these guides
[Local Mode](Running-Tachyon-Locally.html) or [Cluster Mode](Running-Tachyon-on-a-Cluster.html).

Shark 0.7 adds a new storage format to support efficiently reading data from
[Tachyon](http://tachyonproject.org), which enables data sharing and isolation across instances of
Shark. Our meetup [slide](http://goo.gl/fVmxCG) gives a good overview of the benefits of using
Tachyon to cache Shark's tables. In summary, the followings are four major ones:

-   In-memory data sharing across multiple Shark instances (i.e. stronger isolation)
-   Instant recovery of in-memory tables
-   Reduce heap size results in faster GC in shark
-   If the table is larger than the amount of available memory, only hot columns will be cached in memory

### Shark Compatibility

<table class="table">
<tr><th>Tachyon Version</th><th>Shark Version</th></tr>
<tr>
  <td>0.2.1</td>
  <td>0.7.x</td>
</tr>
<tr>
  <td>0.3.0</td>
  <td>0.8.1</td>
</tr>
<tr>
  <td>0.4.0</td>
  <td>0.9.0</td>
</tr>
<tr>
  <td>0.4.1</td>
  <td>0.9.1 +</td>
</tr>
<tr>
  <td>0.5.0</td>
  <td>0.9.1 +</td>
</tr>
</table>

### Setup

In order to run Shark on Tachyon, you need to setup `Tachyon` first, either in
[Local Mode](https://github.com/amplab/tachyon/wiki/Running-Tachyon-Locally) or
in
[Cluster Mode](https://github.com/amplab/tachyon/wiki/Running-Tachyon-on-a-Cluster),
with HDFS.

Then add the following lines to `shark-env.sh`.

    export TACHYON_MASTER="tachyon://TachyonMasterHost:TachyonMasterPort"
    export TACHYON_WAREHOUSE_PATH=/sharktables

### Caching Shark tables in Tachyon

There are a couple ways to create tables that are cached on Tachyon. Running
these queries requires some data to already be on the filesystom or loaded into
Shark.

##### Specify TBLPROPERTIES(“shark.cache” = “tachyon”), for example:

    CREATE TABLE data TBLPROPERTIES(“shark.cache” = “tachyon”) AS SELECT a, b, c from data_on_disk WHERE month=“May”;

##### Specify the table's name ending with \_tachyon, for example:

    CREATE TABLE orders_tachyon AS SELECT * FROM orders;

After creating the table in Tachyon, you can query it like any other table.
