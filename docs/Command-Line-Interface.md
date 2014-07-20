---
layout: global
title: Command Line Interface
---

Tachyon's command line interface gives users access to basic file system operations. Invoke the
command line utility with the script:

    ./bin/tachyon tfs

All "path" variables in tfs commands should start with

    tachyon://<master node address>:<master node port>/<path>

Or, if no header is provided, the default hostname and port (set in the env file) will be used.

    /<path>

#List of Operations

<table class="table">
  <tr><th>Operation</th><th>Syntax</th><th>Description</th></tr>
  <tr>
    <td>cat</td>
    <td>cat "path"</td>
    <td>Print the content of the file to the console. </td>
  </tr>
  <tr>
    <td>count</td>
    <td>count "path"</td>
    <td>Display the number of folders and files matching the specified prefix in "path".</td>
  </tr>
  <tr>
    <td>ls</td>
    <td>ls "path"</td>
    <td>List all the files and directories directly under the given path with information such as size.</td>
  </tr>
  <tr>
    <td>lsr</td>
    <td>lsr "path"</td>
    <td>Recursively list all the files and directories directly under the given path with information such as size.</td>
  </tr>
  <tr>
    <td>mkdir</td>
    <td>mkdir "path"</td>
    <td>Create a directory under the given path, along with any necessary parent directories. Will fail if the path already exists.</td>
  </tr>
  <tr>
    <td>rm</td>
    <td>rm "path"</td>
    <td>Remove a file or directory and all folders and files under that directory.</td>
  </tr>
  <tr>
    <td>tail</td>
    <td>tail "path"</td>
    <td>Print the last 1KB of the specified file to the console. </td>
  </tr>
  <tr>
    <td>touch</td>
    <td>touch "path"</td>
    <td>Create a 0 byte file at the specified location.</td>
  </tr>
  <tr>
    <td>mv</td>
    <td>mv "source" "destination"</td>
    <td>Move a file or directory specified by "source" to a new path "destination". Will fail if "destination" already exists.</td>
  </tr>
  <tr>
    <td>copyFromLocal</td>
    <td>copyFromLocal "source path" "remote path"</td>
    <td>Copy the specified file to the path specified by "remote path". Will fail if "remote path" already exists.</td>
  </tr>
  <tr>
    <td>copyToLocal</td>
    <td>copyToLocal "remote path" "local path"</td>
    <td>Copy the specified file from the path specified by "remote source" to a local destination.</td>
  </tr>
  <tr>
    <td>fileinfo</td>
    <td>fileinfo "path"</td>
    <td>Print the information of the blocks of a specified file.</td>
  </tr>
  <tr>
    <td>pin</td>
    <td>pin "path"</td>
    <td>Pins the given file, such that Tachyon will never evict it from memory. If called on a folder, it recursively pins all contained files and any new files created within this folder.</td>
  </tr>
  <tr>
    <td>unpin</td>
    <td>unpin "path"</td>
    <td>Unpins the given file to allow Tachyon to start evicting it again. If called on a folder, it recursively unpins all contained files and any new files created within this folder.</td>
  </tr>
</table>
