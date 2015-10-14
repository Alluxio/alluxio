---
layout: global
title: Command Line Interface
group: Features
priority: 0
---

Tachyon's command line interface provides users with basic file system operations. You can invoke 
the command line utility using:

```bash
$ ./bin/tachyon tfs
```

All "path" variables in tfs commands should start with

    tachyon://<master node address>:<master node port>/<path>

Or, if no header is provided, the default hostname and port (set in the env file) will be used.

    /<path>

# List of Operations

<table class="table">
  <tr><th>Operation</th><th>Syntax</th><th>Description</th></tr>
  <tr>
    <td>cat</td>
    <td>cat "path"</td>
    <td>Print the content of the file to the console.</td>
  </tr>
  <tr>
    <td>copyFromLocal</td>
    <td>copyFromLocal "source path" "remote path"</td>
    <td>Copy the specified file specified by "source path" to the path specified by "remote path".
    This command will fail if "remote path" already exists.</td>
  </tr>
  <tr>
    <td>copyToLocal</td>
    <td>copyToLocal "remote path" "local path"</td>
    <td>Copy the specified file from the path specified by "remote source" to a local
    destination.</td>
  </tr>
  <tr>
    <td>count</td>
    <td>count "path"</td>
    <td>Display the number of folders and files matching the specified prefix in "path".</td>
  </tr>
  <tr>
    <td>du</td>
    <td>du "path"</td>
    <td>Display the size of a file or a directory specified by the input path.</td>
  </tr>
  <tr>
    <td>fileinfo</td>
    <td>fileinfo "path"</td>
    <td>Print the information of the blocks of a specified file.</td>
  </tr>
  <tr>
    <td>free</td>
    <td>free "path"</td>
    <td>Free a file or all files under a directory from Tachyon. If the file/directory is also 
    in under storage, it will still be available there.</td>
  </tr>
  <tr>
    <td>getCapacityBytes</td>
    <td>getCapacityBytes</td>
    <td>Get the capacity of the TachyonFS.</td>
  </tr>
  <tr>
    <td>getUsedBytes</td>
    <td>getUsedBytes</td>
    <td>Get number of bytes used in the TachyonFS.</td>
  </tr>
  <tr>
    <td>load</td>
    <td>load "path"</td>
    <td>Load the data of a file or a directory from under storage into Tachyon.</td>
  </tr>
  <tr>
    <td>loadMetadata</td>
    <td>loadMetadata "path"</td>
    <td>Load the metadata of a file or a directory from under storage into Tachyon.</td>
  </tr>
  <tr>
    <td>location</td>
    <td>location "path"</td>
    <td>Display a list of hosts that have the file data.</td>
  </tr>
  <tr>
    <td>ls</td>
    <td>ls "path"</td>
    <td>List all the files and directories directly under the given path with information such as
    size.</td>
  </tr>
  <tr>
    <td>lsr</td>
    <td>lsr "path"</td>
    <td>Recursively list all the files and directories under the given path with information such
    as size.</td>
  </tr>
  <tr>
    <td>mkdir</td>
    <td>mkdir "path"</td>
    <td>Create a directory under the given path, along with any necessary parent directories. This
    command will fail if the given path already exists.</td>
  </tr>
  <tr>
    <td>mount</td>
    <td>mount "path" "uri"</td>
    <td>Mount the underlying file system path "uri" into the Tachyon namespace as "path". The "path"
    is assumed not to exist and is created by the operation. No data or metadata is loaded from under
    storage into Tachyon. After a path is mounted, operations on objects under the mounted path are
    mirror to the mounted under storage.</td>
  </tr>
  <tr>
    <td>mv</td>
    <td>mv "source" "destination"</td>
    <td>Move a file or directory specified by "source" to a new location "destination". This command
    will fail if "destination" already exists.</td>
  </tr>
  <tr>
    <td>pin</td>
    <td>pin "path"</td>
    <td>Pin the given file to avoid evicting it from memory. If the given path is a directory, it
    recursively pins all the files contained and any new files created within this directory.</td>
  </tr>
  <tr>
    <td>report</td>
    <td>report "path"</td>
    <td>Report to the master that a file is lost.</td>
  </tr>
  <tr>
    <td>request</td>
    <td>request "path" "dependency ID"</td>
    <td>Request the file for a given dependency ID.</td>
  </tr>
  <tr>
    <td>rm</td>
    <td>rm "path"</td>
    <td>Remove a file. This command will fail if the given path is a directory rather than a
    file.</td>
  </tr>
  <tr>
    <td>rmr</td>
    <td>rmr "path"</td>
    <td>Remove a file, or a directory with all the files and sub-directories that this directory
    contains.</td>
  </tr>
  <tr>
    <td>tail</td>
    <td>tail "path"</td>
    <td>Print the last 1KB of the specified file to the console.</td>
  </tr>
  <tr>
    <td>touch</td>
    <td>touch "path"</td>
    <td>Create a 0-byte file at the specified location.</td>
  </tr>
  <tr>
    <td>unmount</td>
    <td>unmount "path"</td>
    <td>Unmount the underlying file system path mounted in the Tachyon namespace as "path". Tachyon 
    objects under "path" are removed from Tachyon, but they still exist in the previously mounted 
    under storage.</td>
  </tr>
  <tr>
    <td>unpin</td>
    <td>unpin "path"</td>
    <td>Unpin the given file to allow Tachyon to evict this file again. If the given path is a
    directory, it recursively unpins all files contained and any new files created within this
    directory.</td>
  </tr>
</table>
