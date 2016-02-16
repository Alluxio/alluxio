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
manage files, Tachyon also serves key-value stores on top of Tachyon filesystem.
Like files in Tachyon filesystem, the semantics of key-value stores is also write-once:

* Users can create a key-value store and insert key-value pairs into the store. A store becomes
immutable after it is complete. 
* Users can open key-value stores after they are complete.

Each single key-value store is denoted by a TachyonURI like `tachyon://path/my-kvstore`. 
Depending on the total size and block size specified by the user, a single key-value 
store may consist of one or multiple partitions, but the internal is managed by Tachyon and thus
transparent to users.

# Accessing Key-Value Store in Java Application

### Getting a Key-Value Store Client

To obtain a Tachyon key-value store client in Java code, use:

{% include Key-Value-Store-API/get-key-value-system.md %}

## Creating a new key-value store

To create a new key-value store, use `KeyValueSystem#createStore(TachyonURI)`, which returns
a writer to add key-value pairs. For example:

{% include Key-Value-Store-API/create-new-key-value.md %}

Note that, 

* Before the writer closes, the store is not complete and can not be read;
* It is possible that the store is larger than the maximum allowed size of one partition, and in 
this case, the writer will save key-value pairs into multiple partitions. But the switch is 
transparent.
* The keys to insert should be sorted and with no duplicated keys. 

## Retrieving value from a store

To query a complete key-value store, use `KeyValueSystem#openStore(TachyonURI)`, which returns
a reader to retrieve value by the key. For example:

{% include Key-Value-Store-API/read-value.md %}

## Iterating key-value pairs over a store

{% include Key-Value-Store-API/iterate-key-values.md %}

# Accessing Key-Value Store in Hadoop MapReduce
 
## MapReduce InputFormat

Tachyon provides an implementation of `InputFormat` for Hadoop MapReduce programs to access
a key-value store. It takes a key-value URI, and emits key-value pairs stored in the store:
 
{% include Key-Value-Store-API/set-input-format.md %}


## MapReduce OutputFormat
Similarly, Tachyon also provides an implementation of `OutputFormat` for Hadoop MapReduce programs
 to create a key-value store by taking a key-value URI, and saving key-value pairs to the
 KeyValueStore:
 
{% include Key-Value-Store-API/set-output-format.md %}

# Configuration Parameters For Key-Value Stores

Key-Value support in Tachyon is disabled by default, and it can be enabled in Tachyon by setting 
`tachyon.keyvalue.enabled` to true (see
[configuration parameters](Configuration-Settings.html))

These are the configuration parameters for Key-Value support.

{% include Key-Value-Store-API/key-value-configuration.md %}
