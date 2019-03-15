---
layout: global
title: Key Value System Client API (alpha)
nickname: Key Value System API
group: Features
priority: 4
---

* Table of Contents
{:toc}

# Overview
In addition to [Filesystem API](File-System-API.html) which allows applications to read, write or
manage files, Alluxio also serves key-value system on top of Alluxio filesystem.
Like files in Alluxio filesystem, the semantics of key-value system are also write-once:

* Users can create a key-value store and insert key-value pairs into the store. A store becomes
immutable after it is complete.
* Users can open key-value stores after they are complete.

Each single key-value store is denoted by an AlluxioURI like `alluxio://path/my-kvstore`.
Depending on the total size and block size specified by the user, a single key-value
store may consist of one or multiple partitions, but the internal is managed by Alluxio and thus
transparent to users.


# Configuration Parameters For Key-Value System

Key-Value support in Alluxio is disabled by default, and it can be enabled in Alluxio by setting
`alluxio.keyvalue.enabled` to true (see [configuration parameters](Configuration-Settings.html))

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.key-value-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.key-value-configuration[item][propertyName] }}</td>
  </tr>
{% endfor %}
</table>

# Quick Test

After enabling Key-Value support in Alluxio, you can run `./bin/alluxio runKVTest` to test whether
the Key-Value system is running. You should see `Passed the test!` at the bottom of the output if
the Key-Value system is correctly started.

# Accessing Key-Value System in Java Application

### Getting a Key-Value System Client

To obtain an Alluxio key-value system client in Java code, use:

{% include Key-Value-Store-API/get-key-value-system.md %}

## Creating a new key-value store

To create a new key-value store, use `KeyValueSystem#createStore(AlluxioURI)`, which returns
a writer to add key-value pairs. For example:

{% include Key-Value-Store-API/create-new-key-value.md %}

Note that,

* Before the writer closes, the store is not complete and can not be read;
* It is possible that the store is larger than the maximum allowed size of one partition, and in
this case, the writer will save key-value pairs into multiple partitions. But the switch is
transparent.
* The keys to insert should be sorted and with no duplicated keys.

## Retrieving value from a store

To query a complete key-value store, use `KeyValueSystem#openStore(AlluxioURI)`, which returns
a reader to retrieve value by the key. For example:

{% include Key-Value-Store-API/read-value.md %}

## Iterating key-value pairs over a store

{% include Key-Value-Store-API/iterate-key-values.md %}

## Examples

See more [examples](https://github.com/Alluxio/alluxio/tree/master/examples/src/main/java/alluxio/examples/keyvalue) in the codebase.

# Accessing Key-Value System in Hadoop MapReduce

## MapReduce InputFormat

Alluxio provides an implementation of `InputFormat` for Hadoop MapReduce programs to access
a key-value store. It takes a key-value URI, and emits key-value pairs stored in the store:

{% include Key-Value-Store-API/set-input-format.md %}


## MapReduce OutputFormat

Similarly, Alluxio also provides implementations of `OutputFormat` and `OutputCommitter` for Hadoop
 MapReduce programs to create a key-value store by taking a key-value URI, and saving key-value
 pairs to the key-value store:

{% include Key-Value-Store-API/set-output-format.md %}

## Examples

See an [example](https://github.com/Alluxio/alluxio/blob/master/examples/src/main/java/alluxio/examples/keyvalue/hadoop/CloneStoreMapReduce.java) in the codebase.

If you have [configured Alluxio to use HDFS as under storage](Configuring-Alluxio-with-HDFS.md), and have enabled
Key-Value system, you can run the example via

{% include Key-Value-Store-API/run-mapreduce-example.md %}
