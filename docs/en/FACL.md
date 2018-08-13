---
layout: global
title: Access Control List
nickname: Access Control List
group: Features
priority: 1
---

* Table of Contents
{:toc}

This document describes the Access Control List feature related to file and directory permissions in Alluxio. 

1. [ACL Permission Model](#ACL Permission Model): Alluxio implements an ACL model similar to those found in Linux and HDFS. This section describes some background on what this permission model looks like. 
1. [Command Line Interface](#Command Line Interface): The users can interact with Alluxio's ACL model by using the command line interface tools such as `setfacl` and `getfacl`. 

## ACL Permission Model
Alluxio already implements a file and directory permission model similar to traditional Unix permission bits. For each directory and file, there are three types of permissions: read, write and execute. These permissions can be assigned to three different user clases: owner, group and other. 

The permission bits model is sufficient for most cases. However, it is not flexible enough to express more complicated security policies found in today's computing environment. Access Control List (ACL) is a feature which allows users to mnanage any user or group's rights to any file system objects.

In Alluxio's ACL model, a file or directory's ACL consists of many ACL entries. Each entry specifies a particular user or group's permission to read, write and execute. Each ACL entry consists of a type, which can be one of user, group or mask, an optional name and a permission string similar to the linux permission bits. For example, `user::rw-` is an ACL entry. This entry has the type `user`, with an unspecified name, which means the owner of the file. `rw-` means the owner of the file has `read` and `write` permissions but no `execute` permission. Another example is `group:interns:r--`, this means the interns group has read-only access to the file. 

What we described above is the file or the directory's access ACL. It controls whether a particular type of access to a file or directory is permitted. There is another type of ACL, called default ACL. Default ACLs only apply to directories. Any new file or directory created within a directory with a default ACL will inherit the default ACL as its access ACL. 

Default ACLs also consists of ACL entries. These entries are similar to those found in access ACLs. However, they are prefixed with a `default` keyword. For example, `default:user:alluxiouser:rwx` is a valid default ACL entry. 

Now we use an example to explain how default ACL works. We have a directory called `documents`. We can set its default ACL to `default:user:alluxiouser:rwx`. This action does not give the user `alluxiouser` any additional permission to the directory, but the user`alluxiouser` will have full access to any new files created in the `documents` directory. In other words, any new files created in the `documents` directory will have an access ACL entry `user:alluxiouser:rwx`. 

## Command Line Interface
There are two command line interface (CLI) tools to manipulate the ACL permissions, `setfacl` and `getfacl`, with the following syntax:

 `setfacl [-d] [-R] [--set | -m | -x <acl_entries> <path>] | [-b | -k <path>]`

 `-d` All operations apply to the Default ACL. Any ACL entries will be converted into a default ACL entry.

 `-R` Apply operations to all files and directories recursively.

 `--set` Fully replace the ACL while discarding existing entries. New ACL must be a comma separated list of entries, and must include user, group, and other for compatibility with permission bits.

 `-m` Modify the ACL by adding/overwriting new entries.

 `-x` Remove specific ACL entries

 `-b` Remove all ACL entries, except for the base entries.

 `-k` Remove all the default ACL entries.

 `getfacl <path>`


 