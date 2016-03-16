---
layout: global
title: Security (alpha)
nickname: Security
group: Features
priority: 1
---

* Table of Contents
{:toc}

Secure Alluxio has two features currently. This document describes the concepts and usage of them.

1. [Authentication](#authentication): If enabled, Alluxio file system can recognize and verify
the user accessing it. It is the basis for other security features such as authorization and
encryption.
2. [Authorization](#authorization): If enabled, Alluxio file system can control the user's access.
POSIX permission model is used in Alluxio to assign permissions and
control access rights.

By default Alluxio runs in non-secure mode in which no authentication and authorization are
required.
See [Security specific configuration](Configuration-Settings.html#security-configuration) to
enable and use security features.

# Authentication

Alluxio provides file system service through Thrift RPC. The client side (representing a user)
and the server side (such as master) should build an authenticated connection for communication.
If authentication succeeds, the connection will be built. If fails,
the connection can not be built and an exception will be thrown to client.

Three authentication modes are supported: NOSASL (default mode), SIMPLE, and CUSTOM.

## User Accounts

The communication entities in Alluxio consist of master, worker, and client. Each of them needs
to know its user who are running it, also called as the login user. JAAS (Java Authentication and
Authorization Service) is used to determine who is currently executing the process.

When authentication is enabled, a login user for the component (master, worker, or client)
can be obtained by following steps:

1. Login by configurable user. If property 'alluxio.security.login.username' is set by
application, its value will be the login user.
2. If its value is empty, login by OS account.

If login fails, an exception will be thrown. If succeeds,

1. For master, the login user is the super user of Alluxio file system. It is also the owner of
root directory.
2. For worker and client, the login user is the user who contacts with master for accessing file.
It is passed to master through RPC connection for authentication.

## NOSASL

Authentication is disabled. Alluxio file system behaviors as before.
SASL (Simple Authentication and Security Layer) is a framework to define the authentication
between client and server applications, which used in Alluxio to implement authentication feature
. So NOSASL is used to represent disabled case.

## SIMPLE

Authentication is enabled. Alluxio file system can know the user accessing it,
and simply believe the user is the one he/she claims.

After a user creates directories/files, the user name is added into metadata. This user info
could be read and shown in CLI and UI.

## CUSTOM

Authentication is enabled. Alluxio file system can know the user accessing it,
and use customized `AuthenticationProvider` to verify the user is the one he/she claims.

Experimental. This mode is only used in tests currently.

# Authorization

Alluxio file system implements a permissions model for directories and files,
which is similar as the POSIX permission model.

Each file and directory is associated with:

1. an owner, which is the user of the client process to create the file or directory.
2. a group, which is the group fetched from user-groups-mapping service. See [User group
mapping](#user-group-mapping).
3. permissions

The permissions has three parts:

1. owner permission defines the access privileges of the file owner
2. group permission defines the access privileges of the owning group
3. other permission defines the access privileges of all users that are not in any of above two
classes

Each permission has three actions:

1. read (r)
2. write (w)
3. execute (x)

For files, the r permission is required to read the file, and the w permission is required to write
the file. For directories, the r permission is required to list the contents of the directory,
the w permission is required to create, rename or delete files or directories under it,
and the x permission is required to access a child of the directory.

For example, the output of the shell command `ls -R` when authorization is enabled is:

{% include Security/lsr.md %}

## User group mapping

When user is determined, the list of groups is determined by a group mapping service, configured by
'alluxio.security.group.mapping.class'. The default implementation is 'alluxio.security.group
.provider.ShellBasedUnixGroupsMapping', which executes the 'groups' shell
command to fetch the group memberships of a given user.

Property 'alluxio.security.authorization.permission.supergroup' defines a super group. Any users
belong to this group are also super users.

## Initialized directory and file permissions

The initial creation permission is 777, and the difference between directory and file is 111.
For default umask value 022, the created directory has permission 755 and file has permission 644.
The umask can be set by property 'alluxio.security.authorization.permission.umask'.

## Update directory and file permission model

The owner, group, and permissions can be changed by two ways:

1. User application invokes the setAttribute(...) method of `FileSystem API` or `Hadoop API`. See
[File System API](File-System-API.html).
2. CLI command in shell. See
[chown, chgrp, chmod](Command-Line-Interface.html#list-of-operations).

The owner can only be changed by super user.
The group and permission can only be changed by super user and file owner.

# Deployment

It is recommended to start Alluxio master and workers by one same user. Alluxio cluster service
composes of master and workers. Every worker needs to RPC with master for some file operations.
If the user of a worker is not the same as the master's, the file operations may fail because of
permission check.
