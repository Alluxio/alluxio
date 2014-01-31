---
layout: global
title: Command Line Interface
---

Tachyon's command line interface gives users access to basic file system operations. Invoke the
command line utility with the script:

    bin/tachyon

Currently, the only command is tfs, TachyonFileSystem, for file system operations. All "path"
variables should start with

    tachyon://<master node address>:<master node port>/<path>

Or, if no header is provided, the default hostname and port (set in env file) will be used.

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
    <td>copyFromLocal</td>
    <td>copyFromLocal "source path" "remote path"</td>
    <td>Copy the specified file to the path specified by "remote path". Will fail if "remote path" is already in use.</td>
  </tr>
  <tr>
    <td>copyToLocal</td>
    <td>copyToLocal "remote path" "local path"</td>
    <td>Copy the specified file from the path specified by "remote source" to a local destination.</td>
  </tr>
  <tr>
    <td>count</td>
    <td>count "path"</td>
    <td>Display the number of folders and files matching the specified prefix in "path".</td>
  </tr>
  <tr>
    <td>fileinfo</td>
    <td>fileinfo "path"</td>
    <td>Print the information of the blocks of a specified file.</td>
  </tr>
  <tr>
    <td>location</td>
    <td>location "path"</td>
    <td>List the nodes where the file specified by "path" can be found.</td>
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
    <td>Create a directory under the given path, and creates any necessary parent directories. Will fail if the path is already in use.</td>
  </tr>
  <tr>
    <td>mv</td>
    <td>mv "source" "destination"</td>
    <td>Move a file or directory specified by "source" to a new path "destination". Will fail if "destination" is already in use.</td>
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
</table>
