---
layout: global
title: Security
nickname: Security
group: Features
priority: 1
---

* Table of Contents
{:toc}

This document describes the following security related features in Alluxio.

1. [Authentication](#authentication): If `alluxio.security.authentication.type=SIMPLE` (by default),
Alluxio file system recognizes the user accessing the service.
Having `SIMPLE` authentication is required to use other security features such as authorization.
Alluxio also supports other authentication modes like `NOSASL` and `CUSTOM`.
2. [Authorization](#authorization): If `alluxio.security.authorization.permission.enabled=true`
(by default), Alluxio file system will grant or deny user access based on the requesting user and
the POSIX permission model of the files or directories to access.
Note that, authentication cannot be `NOSASL` as authorization requires user information.
3. [Auditing](#auditing): If `alluxio.master.audit.logging.enabled=true`, Alluxio file system
maintains an audit log for user accesses to file metadata.

See [Security specific configuration](Configuration-Settings.html#security-configuration) for
different security properties.

## Authentication

### SIMPLE

When `alluxio.security.authentication.type` is set to `SIMPLE`, authentication is enabled.
Before an Alluxio client accesses the service, this client retrieves the user information to report
to Alluxio service in the following order:

1. If property `alluxio.security.login.username` is set on the client, its value will be used as
the login user of this client.
2. Otherwise, the login user is inferred from the operating system.

After the client retrieves the user information, it will use this user information to connect to
the service. After a client creates directories/files, the user information is added into metadata
and can be retrieved in CLI and UI.

### NOSASL

When `alluxio.security.authentication.type` is `NOSASL`, authentication is disabled. Alluxio
service will ignore the user of the client and no information will be associated to the files or
directories created by this user.

### CUSTOM

When `alluxio.security.authentication.type` is `CUSTOM`, authentication is enabled. Alluxio clients
checks `alluxio.security.authentication.custom.provider.class` which is name of a class
implementing `alluxio.security.authentication.AuthenticationProvider` to retrieve the user.

This mode is currently experimental and should only be used in tests.

## Authorization

Alluxio file system implements a permissions model for directories and files similar to the POSIX
 permission model.

Each file and directory is associated with:

1. an owner, which is the user of the client process to create the file or directory.
2. a group, which is the group fetched from user-groups-mapping service. See [User group
mapping](#user-group-mapping).
3. permissions

The permissions have three parts:

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

For example, the output of the shell command `ls` when authorization is enabled is:

```bash
$ ./bin/alluxio fs ls /
drwxr-xr-x jack           staff                       24       PERSISTED 11-20-2017 13:24:15:649  DIR /default_tests_files
-rw-r--r-- jack           staff                       80   NOT_PERSISTED 11-20-2017 13:24:15:649 100% /default_tests_files/BASIC_CACHE_PROMOTE_MUST_CACHE
```

### User group mapping

When user is determined, the list of groups is determined by a group mapping service, configured by
`alluxio.security.group.mapping.class`. The default implementation is
`alluxio.security.group.provider.ShellBasedUnixGroupsMapping`, which executes the `groups` shell
command to fetch the group memberships of a given user. There is a caching mechanism for user group
mapping, the mapping data will be cached for 60 seconds by default, this value can be configured by
`alluxio.security.group.mapping.cache.timeout`, if the value is '0', the cached will be disabled.

Property `alluxio.security.authorization.permission.supergroup` defines a super group. Any users
belong to this group are also super users.

### Initialized directory and file permissions

The initial creation permission is 777, and the difference between directory and file is 111.
For default umask value 022, the created directory has permission 755 and file has permission 644.
The umask can be set by property `alluxio.security.authorization.permission.umask`.

### Update directory and file permission model

The owner, group, and permissions can be changed by two ways:

1. User application invokes the setAttribute(...) method of `FileSystem API` or `Hadoop API`. See
[File System API](File-System-API.html).
2. CLI command in shell. See
[chown](Command-Line-Interface.html#chown),
[chgrp](Command-Line-Interface.html#chgrp),
[chmod](Command-Line-Interface.html#chmod).

The owner can only be changed by super user.
The group and permission can only be changed by super user and file owner.

## Impersonation
Alluxio supports user impersonation in order for a user to access Alluxio on the behalf of another user.
This can be useful if an Alluxio client is part of a service which provides access to Alluxio for many
different users. In this scenario, the Alluxio client can be configured to connect to Alluxio servers
with a particular user (the connection user), but act on behalf of other users (impersonation users).
In order to configure Alluxio for impersonation, client and master configuration are required.

### Master Configuration
In order to enable a particular user to impersonate other users, the Alluxio master must be configured
to allow that ability. 
The master configuration properties are: `alluxio.master.security.impersonation.<USERNAME>.users` and
`alluxio.master.security.impersonation.<USERNAME>.groups`.

For `alluxio.master.security.impersonation.<USERNAME>.users`, you can specify the comma-separated list
of users that the `<USERNAME>` is allowed to impersonate. The wildcard `*` can be used to indicate that
the user can impersonate any other user. Here are some examples.

- `alluxio.master.security.impersonation.alluxio_user.users=user1,user2`
    - This means the Alluxio user `alluxio_user` is allowed to impersonate the users `user1` and `user2`.
- `alluxio.master.security.impersonation.client.users=*`
    - This means the Alluxio user `client` is allowed to impersonate any user.

For `alluxio.master.security.impersonation.<USERNAME>.users`, you can specify the comma-separated groups
of users that the `<USERNAME>` is allowed to impersonate. The wildcard `*` can be used to indicate that
the user can impersonate any other user. Here are some examples.

- `alluxio.master.security.impersonation.alluxio_user.groups=group1,group2`
    - This means the Alluxio user `alluxio_user` is allowed to impersonate any users from groups `group1` and `group2`.
- `alluxio.master.security.impersonation.client.groups=*`
    - This means the Alluxio user `client` is allowed to impersonate any user.

In order to enable impersonation for some user `alluxio_user`, at least 1 of
`alluxio.master.security.impersonation.<USERNAME>.users` and `alluxio.master.security.impersonation.<USERNAME>.groups`
must be set (replace `<USERNAME>` with `alluxio_user`). Both parameters are allowed to be set for the same user.

### Client Configuration
If the master enables impersonation for particular users, the client must also be configured to
impersonate other users. This is configured with the parameter: `alluxio.security.login.impersonation.username` .
This informs the Alluxio client to connect as usual, but impersonate as a different user. The
parameter can set to the following values:

- empty
  - Alluxio client impersonation is not used
- `_NONE_`
  - Alluxio client impersonation is not used
- `_HDFS_USER_`
  - the Alluxio client will impersonate as the same use as the HDFS client (when using the Hadoop compatible client.)

## Auditing
Alluxio supports audit logging to allow system administrators to track users' access to file metadata.

The audit log file (`master_audit.log`) contains multiple audit log entries, each of which corresponds to an access to file metadata.
The format of Alluxio audit log entry is shown in the table below.

<table class="table table-striped">
<tr><th>key</th><th>value</th></tr>
<tr>
  <td>succeeded</td>
  <td>True if the command has succeeded. To succeed, it must also have been allowed. </td>
</tr>
<tr>
  <td>allowed</td>
  <td>True if the command has been allowed. Note that a command can still fail even if it has been allowed. </td>
</tr>
<tr>
  <td>ugi</td>
  <td>User group information, including username, primary group, and authentication type. </td>
</tr>
<tr>
  <td>ip</td>
  <td>Client IP address. </td>
</tr>
<tr>
  <td>cmd</td>
  <td>Command issued by the user. </td>
</tr>
<tr>
  <td>src</td>
  <td>Path of the source file or directory. </td>
</tr>
<tr>
  <td>dst</td>
  <td>Path of the destination file or directory. If not applicable, the value is null. </td>
</tr>
<tr>
  <td>perm</td>
  <td>User:group:mask or null if not applicable. </td>
</tr>
</table>

It is similar to the format of HDFS audit log [wiki](https://wiki.apache.org/hadoop/HowToConfigure).

To enable Alluxio audit logging, you need to set the JVM property
`alluxio.master.audit.logging.enabled` to `true`, see
[Configuration settings](Configuration-Settings.html).

## Encryption

Service level encryption is not supported yet, user could encrypt sensitive data at application
level, or enable encryption feature at under file system, e.g. HDFS transparent encryption, Linux
disk encryption.

## Deployment

It is recommended to start Alluxio master and workers by one same user. Alluxio cluster service
composes of master and workers. Every worker needs to communicate with the master via RPC to perform some file operations.
If the user of a worker is not the same as the master's, the file operations may fail because of
permission check.
