---
layout: global
title: Developer Tips
nickname: Developer Tips
group: Dev Resources
---

* Table of Contents
{:toc}

This page is a collection of tips and howtos geared towards developers of Tachyon.

### Change a Thrift RPC definition

Tachyon uses thrift for RPC communication between clients and servers. The `.thrift`
files defined in `common/src/thrift/` are used to auto-generate Java code for calling the
RPCs on clients and implementing the RPCs on servers. To change a Thrift definition, you
must first [install the Thrift compiler](https://thrift.apache.org/docs/install/).
If you have brew, you can do this by running

```bash
$ brew install thrift
```

Then to regenerate the Java code, run

```bash
$ bin/tachyon thriftGen
```

### Change a Protocol Buffer Message

Tachyon uses protocol buffers to read and write journal messages. The `.proto` files
defined in `servers/src/proto/journal/` are used to auto-generate Java definitions for
the protocol buffer messages. To change one of these messages, first read about
[updating a message type](https://developers.google.com/protocol-buffers/docs/proto#updating)
to make sure your change will not break backwards compatibility. Next,
[install protoc](https://github.com/google/protobuf#protocol-buffers---googles-data-interchange-format).
If you have brew, you can do this by running

```bash
$ brew install protobuf
```

Then to regenerate the Java code, run

```bash
$ bin/tachyon protoGen
```
