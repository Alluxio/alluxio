---
layout: global
title: Key Value Store Client API
nickname: Key Value Store API
group: Features
priority: 4
---

* Table of Contents
{:toc}

# Overview
In addition to [Filesystem API](File-System-API.html) which allows applications to read, write or
manage files, Alluxio also serves key-value stores on top of Alluxio filesystem.
Like files in Alluxio filesystem, the semantics of key-value stores is also write-once:

* Users can create a key-value store and insert key-value pairs into the store. A store becomes
immutable after it is complete. 
* Users can open key-value stores after they are complete.

Each single key-value store is denoted by a AlluxioURI like `tachyon://path/my-kvstore`. 
Depending on the total size and block size specified by the user, a single key-value 
store may consist of one or multiple partitions, but the internal is managed by Alluxio and thus
transparent to users.

# Accessing Key-Value Store in Java Application

### Getting a Key-Value Store Client

To obtain a Alluxio key-value store client in Java code, use:

```java
KeyValueSystem kvs = KeyValueSystem.Factory().get();
```

## Creating a new key-value store

To create a new key-value store, use `KeyValueSystem#createStore(AlluxioURI)`, which returns
a writer to add key-value pairs. For example:

```java
KeyValueStoreWriter writer = kvs.createStore(new AlluxioURI("tachyon://path/my-kvstore"));
// Insert key-value pair ("100", "foo")
writer.put("100", "foo");
// Insert key-value pair ("200", "bar")
writer.put("200", "bar");
// Close and complete the store
writer.close();
```

Note that, 

* Before the writer closes, the store is not complete and can not be read;
* It is possible that the store is larger than the maximum allowed size of one partition, and in 
this case, the writer will save key-value pairs into multiple partitions. But the switch is 
transparent.
* The keys to insert should be sorted and with no duplicated keys. 

## Retrieving value from a store

To query a complete key-value store, use `KeyValueSystem#openStore(AlluxioURI)`, which returns
a reader to retrieve value by the key. For example:

```java
KeyValueStoreReader reader = kvs.openStore(new AlluxioURI("tachyon://path/kvstore/"));
// Return "foo"
reader.get("100"); 
// Return null as no value associated with "300"
reader.get("300");
// Close the reader on the store
reader.close();
```

## Iterating key-value pairs over a store

```java
KeyValueStoreReader reader = kvs.openStore(new AlluxioURI("tachyon://path/kvstore/"));
KeyValueIterator iterator = reader.iterator();
while (iterator.hasNext()) {
  KeyValuePair pair = iterator.next();
  ByteBuffer key = pair.getkKey();
  ByteBuffer value = pair.getValue();
}
// Close the reader on the store
reader.close()
```

# Accessing Key-Value Store in Hadoop MapReduce
 
## MapReduce InputFormat

Alluxio provides an implementation of `InputFormat` for Hadoop MapReduce programs to access
a key-value store. It takes a key-value URI, and emits key-value pairs stored in the store:
 
```java
conf.setInputFormat(KeyValueInputFormat.class);
FileInputFormat.setInputPaths(conf, new Path("tachyon://input-store"));
```


## MapReduce OutputFormat
Similarly, Alluxio also provides an implementation of `OutputFormat` for Hadoop MapReduce programs
 to create a key-value store by taking a key-value URI, and saving key-value pairs to the
 KeyValueStore:
 
```java
conf.setOutputKeyClass(BytesWritable.class);
conf.setOutputValueClass(BytesWritable.class);
conf.setOutputFormat(KeyValueOutputFormat.class);
conf.setOutputCommitter(KeyValueOutputCommitter.class);
FileOutputFormat.setOutputPath(conf, new Path("tachyon://output-store"));
```

# Configuration Parameters For Key-Value Stores

Key-Value support in Alluxio is disabled by default, and it can be enabled in Alluxio by setting 
`tachyon.keyvalue.enabled` to true (see
[configuration parameters](Configuration-Settings.html))

These are the configuration parameters for tiered storage.

<table class="table table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
<tr>
  <td>tachyon.keyvalue.enabled</td>
  <td>false</td>
  <td>
  Whether the keyvalue interface is enabled.
  </td>
</tr>
<tr>
  <td>tachyon.keyvalue.partition.size.bytes.max</td>
  <td>512MB
  <td>
  Maximum size of each partition.
  </td>
</tr>
</table>
