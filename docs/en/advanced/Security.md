---
layout: global
title: Security
nickname: Security
group: Advanced
priority: 0
---

* Table of Contents
{:toc}

This document describes the following security related features in Alluxio.

1. [User Authentication](#authentication): 
Alluxio filesystem will differentiate users accessing the service
when authentication mode is `SIMPLE` (i.e., `alluxio.security.authentication.type=SIMPLE`).
Alluxio also supports other authentication modes like `NOSASL` which ignores the difference among users.
Having authentication mode to be `SIMPLE` is required for authorization.
1. [User Authorization](#authorization): 
Alluxio filesystem will grant or deny user access based on the requesting user and
the POSIX permissions model of the files or directories to access,
when `alluxio.security.authorization.permission.enabled=true`.
Note that, authentication cannot be `NOSASL` as authorization requires user information.
1. [Access Control Lists](#Access-Control-Lists): In addition to the POSIX permission model, Alluxio
implements an Access Control List(ACL) model similar to those found in Linux and HDFS. The ACL model
is more flexible and allows administrators to manage any user or group's permissions to any file
system object. 
1. [Impersonation](#impersonation): Alluxio supports user impersonation so one user can access
Alluxio on the behalf of another user. This can be useful if an Alluxio client is part of a service
which provides access to Alluxio for many different users.
1. [Auditing](#auditing): If `alluxio.master.audit.logging.enabled=true`, Alluxio file system
maintains an audit log for user accesses to file metadata.

See [Security specific configuration]({{site.baseurl}}{% link en/reference/Properties-List.md %}#security-configuration)
for different security properties.

## Authentication

The authentication protocol is determined by the configuration property `alluxio.security.authentication.type`,
with a default value of `SIMPLE`.

### SIMPLE

Authentication is **enabled** when the authentication type is `SIMPLE`.

A client must identify itself with a username to the Alluxio service.
If the property `alluxio.security.login.username` is set on the client, its value will be used as
the login user, otherwise, the login user is inferred from the operating system.
The provided user information is attached to the corresponding metadata when the client creates
directories or files.

### NOSASL

Authentication is **disabled** when the authentication type is `NOSASL`.

Alluxio service will ignore the user of the client and no user information will be attached to the
corresponding metadata when the client creates directories or files. 

### CUSTOM

Authentication is **enabled** when the authentication type is `CUSTOM`.

Alluxio clients retrieves user information via the class provided by the
`alluxio.security.authentication.custom.provider.class` property.
The specified class must implement `alluxio.security.authentication.AuthenticationProvider`.

This mode is currently experimental and should only be used in tests.

## Authorization

The Alluxio file system implements a permissions model similar to the POSIX permissions model.

Each file and directory is associated with:

- An owner, which is the user of the client process to create the file or directory.
- A group, which is the group fetched from user-groups-mapping service. See [User group
mapping](#user-group-mapping).
- Permissions, which consist of three parts:
  - Owner permission defines the access privileges of the file owner
  - Group permission defines the access privileges of the owning group
  - Other permission defines the access privileges of all users that are not in any of above two
classes

Each permission has three actions:

1. read (r)
2. write (w)
3. execute (x)

For files:
- Read permissions are required to read files
- Write permission are required to write files

For directories:
- Read permissions are required to list its contents
- Write permissions are required to create, rename, or delete files or directories under it
- Execute permissions are required to access a child of the directory

The output of the `ls` shell command when authorization is enabled is:

```bash
$ ./bin/alluxio fs ls /
drwxr-xr-x jack           staff                       24       PERSISTED 11-20-2017 13:24:15:649  DIR /default_tests_files
-rw-r--r-- jack           staff                       80   NOT_PERSISTED 11-20-2017 13:24:15:649 100% /default_tests_files/BASIC_CACHE_PROMOTE_MUST_CACHE
```

### User group mapping

For a given user, the list of groups is determined by a group mapping service, configured by
the `alluxio.security.group.mapping.class` property, with a default implementation of
`alluxio.security.group.provider.ShellBasedUnixGroupsMapping`.
This implementation executes the `groups` shell command to fetch the group memberships of the given user.
The user group mapping is cached, with an expiration period configured by the
`alluxio.security.group.mapping.cache.timeout` property, with a default value of `60s`.
If set to a value of `0`, caching is disabled.

Alluxio has super user, a user with special privileges typically needed to administer and maintain the system.
The `alluxio.security.authorization.permission.supergroup` property defines a super group.
Any users belong to this group are also super users.

### Initialized directory and file permissions

When a file is created, it is initally assigned fully opened permissions of `666` by default.
Similarly, a directory is initally assigned with `777` permissions.
A umask is applied on the initial permissions; this is configured by the
`alluxio.security.authorization.permission.umask` property, with a default of `022`.
Without any property modifications, files and directories are created with `644` and `755` permissions respectively.

### Update directory and file permission model

The owner, group, and permissions can be changed by two ways:

1. User application invokes the `setAttribute(...)` method of `FileSystem API` or `Hadoop API`.
2. CLI command in shell. See
[chown]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#chown),
[chgrp]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#chgrp),
[chmod]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#chmod).

The owner attribute can only be changed by a super user.
The group and permission attributes can be changed by a super user or the owner.

## Access Control Lists

The POSIX permissions model allows administrators to grant permissions to owners, owning groups and other users.
The permission bits model is sufficient for most cases. 
However, to help administrators express more complicated security policies, Alluxio also supports Access Control Lists (ACLs).
ACLs allow administrators to grant permissions to any user or group. 

A file or directory's Access Control List consists of multiple entries.
The two types of ACL entries are Access ACL entries and Default ACL entries. 

### 1. Access ACL Entries:

This type of ACL entry specifies a particular user or group's permission to read, write and
execute. 

Each ACL entry consists of:
- a type, which can be one of user, group or mask
- an optional name 
- a permission string similar to the POSIX permission bits

The following table shows the different types of ACL entries that can appear in the access ACL: 

|ACL Entry Type| Description|
|:----------------------:|:-----------------------|
|user:userid:permission  | Sets the access ACLs for a user. Empty userid implies the permission is for the owner of the file.|
|group:groupid:permission| Sets the access ACLs for a group. Empty groupid implies the permission is for the owning group of the file.|
|other::permission       | Sets the access ACLs for all users not specified above.|
|mask::permission        | Sets the effective rights mask.  The ACL mask indicates the maximum permissions allowed for all users other than the owner and for groups.|

Notice that ACL entries describing owner's, owning group's and other's permissions already exist in
the standard POSIX permission bits model.
For example, a standard POSIX permission of `755` translates into an ACL list as follows:
`user::rwx, group::r-x, other::r-x`.

These three entries are always present in each file and directory.
When there are entries in addition to these standard entries, the ACL is considered an extended ACL. 

A mask entry is automatically generated when an ACL becomes extended.
Unless specifically set by the user, the mask's value is adjusted to be the union of all permissions affected by the mask entry.
This includes all the user entries other than the owner and all group entries. 
	
For the ACL entry `user::rw-`:
- the type is `user`
- the name is empty, which implies the owner
- the permission string is `rw-`

This culminates to the owner has `read` and `write` permissions, but not `execute`.

For the ACL entry `group:interns:rwx` and mask `mask::r--`:
- the entry grants all permissions to the group `interns`
- the mask only allows `read` permissions

This culminates to the `interns` group having only `read` access because the mask disallows all other permissions.  

### 2. Default ACL Entries:

**Default ACLs only apply to directories.**
Any new file or directory created within a directory with a default ACL will inherit the default ACL as its access ACL. 
Any new directory created within a directory with a default ACL will also inherit the default ACL as its default ACL. 

**Default ACLs also consists of ACL entries, similar to those found in access ACLs.** 
The are distinguished by the `default` keyword as the prefix.
For example, `default:user:alluxiouser:rwx` and `default:other::r-x` are both valid default ACL entries.

Given a `documents` directory, its default ACL can be set to `default:user:alluxiouser:rwx`.
The user `alluxiouser` will have full access to any new files created in the `documents` directory.
These new files will have an access ACL entry of `user:alluxiouser:rwx`.
Note that the ACL does not grant the user `alluxiouser` any additional permissions to the directory.

### Managing ACL entries

ACLs can be managed by two ways:

1. User application invokes the `setFacl(...)` method of `FileSystem API` or `Hadoop API` to change the ACL and invokes the `getFacl(...)` to obtain the current ACL. 
2. CLI command in shell. See
[getfacl]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#getfacl)
[setfacl]({{site.baseurl}}{% link en/basic/Command-Line-Interface.md %}#setfacl),

The ACL of a file or directory can only be changed by super user or its owner.

## Impersonation

Alluxio supports user impersonation in order for a user to access Alluxio on the behalf of another user.
This can be useful if an Alluxio client is part of a service which provides access to Alluxio for different users.
In this scenario, the Alluxio client is configured to connect to Alluxio servers as a connection user, but act on behalf of, or impersonate, other users.
In order to configure Alluxio for impersonation, both client and master configurations are required.

### Master Configuration

To enable a particular user to impersonate other users, set the
`alluxio.master.security.impersonation.<USERNAME>.users` property, where `<USERNAME>` is the name
of the connection user.

The property value is a comma-separated list of users that `<USERNAME>` is allowed to impersonate.
The wildcard value `*` can be used to indicate the user can impersonate any other user.
Some examples:

- `alluxio.master.security.impersonation.alluxio_user.users=user1,user2`
means the connection user `alluxio_user` is allowed to impersonate `user1` and `user2`.
- `alluxio.master.security.impersonation.client.users=*`
means the connection user `client` is allowed to impersonate any user.

To enable a particular user to impersonate other groups, set the
`alluxio.master.security.impersonation.<USERNAME>.groups` property, where again `<USERNAME>` is
the name of the connection user.

Similar to the above, the value is a comma-separated list of groups and the wildcard value `*`
can be used to indicate all groups.
Some examples:

- `alluxio.master.security.impersonation.alluxio_user.groups=group1,group2`
means the connection user `alluxio_user` is allowed to impersonate any users from groups `group1` and `group2`.
- `alluxio.master.security.impersonation.client.groups=*`
means the connection user `client` is allowed to impersonate users from any group.

In summary, impersonation of a given user is only enabled if at least one of the two impersonation
properties are be set; setting both are allowed for the same user.

### Client Configuration

After enabling impersonation on the master for a given user,
the client must indicate which user it wants to impersonate.
This is configured by the `alluxio.security.login.impersonation.username` property.

If the property is set to an empty string or `_NONE_`, impersonation is disabled.
If the property is set to `_HDFS_USER_`, the Alluxio client will impersonate as the same user as
the HDFS client when using the Hadoop compatible client.
This property can also be set to a particular string to indicate a user name.

## Auditing

Alluxio supports audit logging to allow system administrators to track users' access to file metadata.

The audit log file at `master_audit.log` contains entries corresponding to file metadata access operations.
The format of Alluxio audit log entry is shown in the table below:

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

This is similar to the format of HDFS audit log [wiki](https://wiki.apache.org/hadoop/HowToConfigure).

To enable Alluxio audit logging, set the JVM property
`alluxio.master.audit.logging.enabled` to `true` in `alluxio-env.sh`.
See [Configuration settings]({{site.baseurl}}{% link en/basic/Configuration-Settings.md %}).

## Encryption

Service level encryption is not supported yet.
Users can encrypt sensitive data at the application level or enable encryption features in the 
respective under file system, such as HDFS transparent encryption or Linux disk encryption.

## Deployment

It is recommended to start Alluxio master and workers under the same user.
In the case where there is a user mismatch, file operations may fail because of permission checks.
