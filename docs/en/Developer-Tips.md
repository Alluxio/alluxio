---
layout: global
title: Developer Tips
nickname: Developer Tips
group: Resources
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

{% include Developer-Tips/install-thrift.md %}

Then to regenerate the Java code, run

{% include Developer-Tips/thriftGen.md %}

### Change a Protocol Buffer Message

Tachyon uses protocol buffers to read and write journal messages. The `.proto` files
defined in `servers/src/proto/journal/` are used to auto-generate Java definitions for
the protocol buffer messages. To change one of these messages, first read about
[updating a message type](https://developers.google.com/protocol-buffers/docs/proto#updating)
to make sure your change will not break backwards compatibility. Next,
[install protoc](https://github.com/google/protobuf#protocol-buffers---googles-data-interchange-format).
If you have brew, you can do this by running

{% include Developer-Tips/install-protobuf.md %}

Then to regenerate the Java code, run

{% include Developer-Tips/protoGen.md %}

### Full list of the commands in bin/tachyon

Most commands in `bin/tachyon` are for developers. The following table explains the description and
the syntax of each command.

<table class="table table-striped">
<tr><th>Command</th><th>Args</th><th>Description</th></tr>
</tr>
{% for dscp in site.data.table.Developer-Tips %}
<tr>
  <td>{{dscp.command}}</td>
  <td>{{dscp.args}}</td>
  <td>{{site.data.table.en.Developer-Tips.[dscp.command]}}</td>
</tr>
{% endfor %}
</table>

In addition, these commands have different prerequisites. The prerequisite for the `format`,
`formatWorker`, `journalCrashTest`, `readJournal`, `version` and `validateConf` commands is that you
have already built Tachyon (see [Build Tachyon Master Branch](Building-Tachyon-Master-Branch.html)
about how to build Tachyon manually). Further, the prerequisite for the `tfs`, `loadufs`, `runTest`
and `runTests` commands is that you have a running Tachyon system.
