---
layout: global
title: Developer Tips
nickname: Developer Tips
group: Resources
---

* Table of Contents
{:toc}

This page is a collection of tips and howtos geared towards developers of Alluxio.

### Change a Thrift RPC definition

Alluxio uses thrift for RPC communication between clients and servers. The `.thrift`
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

Alluxio uses protocol buffers to read and write journal messages. The `.proto` files
defined in `core/server/src/proto/journal/` are used to auto-generate Java definitions for
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

### Full list of the commands in bin/tachyon

Most commands in `bin/tachyon` are for developers. The following table explains the description and
the syntax of each command.

<table class="table table-striped">
<tr><th>Command</th><th>Args</th><th>Description</th></tr>
<tr>
  <td>format</td>
  <td>[-s]</td>
  <td>Format Alluxio Master and all Workers. The option [-s] indicates that the command should only
  format when the underfs is local and doesn't already exist.</td>
</tr>
<tr>
  <td>formatWorker</td>
  <td>None</td>
  <td>Format Alluxio Worker storage on this local node.</td>
</tr>
<tr>
  <td>bootstrap-conf</td>
  <td>&lt;TACHYON_MASTER_HOSTNAME&gt;</td>
  <td>Generate the bootstrap config file <code>tachyon-env.sh</code> with the specified
  <code>TACHYON_MASTER_HOSTNAME</code>, if the config file doesn't exist.</td>
</tr>
<tr>
  <td>tfs</td>
  <td>[tfs-commands]</td>
  <td>Interact with Alluxio in command line style for basic file system operations.
  See <a href="Command-Line-Interface.html">Command Line</a> for more information.</td>
</tr>
<tr>
  <td>loadufs</td>
  <td>&lt;AlluxioPath&gt; &lt;UfsPath&gt; [ExcludePathPrefixes]</td>
  <td>Loads files under <code>UfsPath</code> to the given <code>AlluxioPath</code>.
  <code>ExcludePathPrefixes</code> can be a set of prefixes which are separated by ';'.
  The paths with a prefix in <code>ExcludePathPrefixes</code> will not be loaded.</td>
</tr>
<tr>
  <td>runTest</td>
  <td>&lt;Example&gt; &lt;ReadType&gt; &lt;WriteType&gt;</td>
  <td>Run an end-to-end test on a Alluxio cluster. <code>Example</code> should be "Basic" or
  "BasicNonByteBuffer". <code>ReadType</code> should be "CACHE_PROMOTE", "CACHE",
  or "NO_CACHE". <code>WriteType</code> should be "MUST_CACHE", "CACHE_THROUGH" or "THROUGH".</td>
</tr>
<tr>
  <td>runTests</td>
  <td>None</td>
  <td>Run all end-to-end tests on a Alluxio cluster. That is, execute the <code>runTest</code> command
  with all the possible args.</td>
</tr>
<tr>
  <td>journalCrashTest</td>
  <td>[-creates &lt;arg&gt;] [-deletes &lt;arg&gt;] [-renames &lt;arg&gt;] [-maxAlive &lt;arg&gt;]
  [-testDir &lt;arg&gt;] [-totalTime &lt;arg&gt;] [-help]</td>
  <td>Test the Master Journal System in a crash scenario. Try <code>tachyon journalCrashTest -help</code>
  to see the meanings of each argument in detail, or you can run it without args by default.</td>
</tr>
<tr>
  <td>readJournal</td>
  <td>[-help] [-noTimeout]</td>
  <td>Read a Alluxio journal file from stdin and write a human-readable version of it to stdout. You
  can run this on the journal file as <code>tachyon readJournal < journal/FileSystemMaster/log.out</code>.</td>
</tr>
<tr>
  <td>killAll</td>
  <td>&lt;WORD&gt;</td>
  <td>Kill processes whose pid or command contains the <code>WORD</code> specified by the user.</td>
</tr>
<tr>
  <td>copyDir</td>
  <td>&lt;PATH&gt;</td>
  <td>Copy the <code>PATH</code> to all worker nodes.</td>
</tr>
<tr>
  <td>clearCache</td>
  <td>None</td>
  <td>Clear OS buffer cache of the machine. This command needs the root permission.</td>
</tr>
<tr>
  <td>thriftGen</td>
  <td>None</td>
  <td>Generate all thrift code. See <a href="#change-a-thrift-rpc-definition">Change a Thrift RPC
  definition</a>.</td>
</tr>
<tr>
  <td>protoGen</td>
  <td>None</td>
  <td>Generate all protocol buffer code. See <a href="#change-a-protocol-buffer-message">Change a
  Protocol Buffer Message</a>.</td>
</tr>
<tr>
  <td>version</td>
  <td>None</td>
  <td>Print Alluxio version.</td>
</tr>
<tr>
  <td>validateConf</td>
  <td>None</td>
  <td>Validate Alluxio conf.</td>
</tr>
</table>

In addition, these commands have different prerequisites. The prerequisite for the `format`,
`formatWorker`, `journalCrashTest`, `readJournal`, `version` and `validateConf` commands is that you
have already built Alluxio (see [Build Alluxio Master Branch](Building-Alluxio-Master-Branch.html)
about how to build Alluxio manually). Further, the prerequisite for the `tfs`, `loadufs`, `runTest`
and `runTests` commands is that you have a running Alluxio system.
