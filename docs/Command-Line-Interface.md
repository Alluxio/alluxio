---
layout: global
title: Command-Line-Interface
---

Tachyon's command line interface gives users access to basic file system
operations. Invoke the command line utility with the script:

    bin/tachyon

Currently, the only command is tfs, TachyonFileSystem, for file system
operations. All "path" variables should start with

    tachyon://<master node address>:<master node port>/<path>

Or, if no header is provided, the default hostname and port (set in env
file) will be used.

    /<path>

  Operation       Syntax                                        Description
  --------------- --------------------------------------------- ----------------------------------------------------------------------------------------------------------------------------------
  ls              ls "path"                                     Lists all the files and directories directly under the given path with information such as size
  mkdir           mkdir "path"                                  Creates a directory under the given path, and creates any necessary parent directories. Will fail if the path is already in use.
  rm              rm "path"                                     Removes a file or directory and all folders and files under that directory.
  mv              mv "source" "destination"                     Moves a file or directory specified by "source" to a new path "destination". Will fail if "destination" is already in use.
  copyFromLocal   copyFromLocal "source" "remote destination"   Copies the specified file to the path specified by "remote destination". Will fail if "remote destination" is already in use.
  copyToLocal     copyToLocal "remote source" "destination"     Copies the specified file from the path specified by "remote source" to a local destination.
  location        location "path"                               Lists the nodes where the file specified by "path" can be found.



