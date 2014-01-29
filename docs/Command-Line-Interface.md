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

<table class="table">
<tr><th>Operation</th><th>Syntax</th><th>Description</th></tr>
<tr>
  <td>ls</td>
  <td>ls "path"</td>
  <td>Lists all the files and directories directly under the given path with information such as size</td>
</tr>
<tr>
  <td>mkdir</td>
  <td>mkdir "path"</td>
  <td>Creates a directory under the given path, and creates any necessary parent directories. Will fail if the path is already in use.</td>
</tr>
<tr>
  <td>rm</td>
  <td>rm "path"</td>
  <td>Removes a file or directory and all folders and files under that directory.</td>
</tr>
<tr>
  <td>mv</td>
  <td>mv "source" "destination"</td>
  <td>Moves a file or directory specified by "source" to a new path "destination". Will fail if "destination" is already in use.</td>
</tr>
<tr>
  <td>copyFromLocal</td>
  <td>copyFromLocal "source" "remote destination"</td>
  <td>Copies the specified file to the path specified by "remote destination". Will fail if "remote destination" is already in use.</td>
</tr>
<tr>
  <td>copyToLocal</td>
  <td>copyToLocal "remote source" "destination"</td>
  <td>Copies the specified file from the path specified by "remote source" to a local destination.</td>
</tr>
<tr>
  <td>location</td>
  <td>location "path"</td>
  <td>Lists the nodes where the file specified by "path" can be found.</td>
</tr>
</table>
