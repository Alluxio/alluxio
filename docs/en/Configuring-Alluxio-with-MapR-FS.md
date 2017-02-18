---
layout: global
title: Configuring Alluxio with MapR-FS
nickname: Alluxio with MapR-FS
group: Under Store
priority: 3
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with [MapR-FS](https://www.mapr.com/products/mapr-fs)
as the under storage system.

## Compiling Alluxio with MapR Version

Alluxio must be [compiled](Building-Alluxio-Master-Branch.html) with the correct MapR distribution
to integrate with MapR-FS. Here are some values of `hadoop.version` for different MapR
distributions:

<table class="table table-striped">
<tr><th>MapR Version</th><th>hadoop.version</th></tr>
<tr>
  <td>5.2</td>
  <td>2.7.0-mapr-1607</td>
</tr>
<tr>
  <td>5.1</td>
  <td>2.7.0-mapr-1602</td>
</tr>
<tr>
  <td>5.0</td>
  <td>2.7.0-mapr-1506</td>
</tr>
<tr>
  <td>4.1</td>
  <td>2.5.1-mapr-1503</td>
</tr>
<tr>
  <td>4.0.2</td>
  <td>2.5.1-mapr-1501</td>
</tr>
<tr>
  <td>4.0.1</td>
  <td>2.4.1-mapr-1408</td>
</tr>
</table>

## Configuring Alluxio for MapR-FS

Once you have compiled Alluxio with the appropriate `hadoop.version` for your MapR distribution, you
may have to configure Alluxio to recognize the MapR-FS scheme and URIs. Alluxio uses the HDFS client
to access MapR-FS, and by default is already configured to do so. However, if the configuration has
been changed, you can enable the HDFS client to access MapR-FS URIs by adding the URI prefix
`maprfs:///` to the configuration variable `alluxio.underfs.hdfs.prefixes` like below:

```properties
alluxio.underfs.hdfs.prefixes=hdfs://,maprfs:///
```

This configuration parameter should be set for all the Alluxio servers (masters, workers). Please
read how to [configure Alluxio](Configuration-Settings.html). For Alluxio processes, this parameter
can be set in the property file `alluxio-site.properties`. For more information, please read about
[configuration of Alluxio with property files](Configuration-Settings.html#property-files).

This parameter should also be set any client that accesses Alluxio. This means the parameter should
be set for any application (MapReduce, Spark, Flink, etc.) that accesses Alluxio. This can typically
be done by adding `-Dalluxio.underfs.hdfs.prefixes=hdfs://,maprfs:///` to the command. For more
information, please read about [configurating applications for Alluxio](Configuration-Settings.html
#application-settings).

## Configuring Alluxio to use MapR-FS as Under File System

There are various ways to configure Alluxio to use MapR-FS as the Under File System. If you want to
mount MapR-FS to the root of Alluxio, add the following to `conf/alluxio-site.properties`:

```properties
alluxio.underfs.address=maprfs:///<path in MapR-FS>/
```

You can also mount a directory in MapR-FS to a directory in the Alluxio namespace.

```bash
$ ${ALLUXIO_HOME}/bin/alluxio fs mount /<path in Alluxio>/ maprfs:///<path in MapR-FS>/
```

## Running Alluxio Locally with MapR-FS

After everything is configured, you can start up Alluxio locally to see that everything works.

{% include Common-Commands/start-alluxio.md %}

This should start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

After this succeeds, you can visit MapR-FS web UI to verify the files and directories created by
Alluxio exist. For this test, you should see files named like:
`/default_tests_files/Basic_CACHE_THROUGH`

Next, you can run a simple example program:

{% include Common-Commands/runTests.md %}

You can stop Alluxio any time by running:

{% include Common-Commands/stop-alluxio.md %}
