---
layout: global
title: Mounting Tachyon with FUSE (Beta)
nickname: Tachyon-FUSE
group: Features
priority: 4
---

* Table of Contents
{:toc}

Tachyon-FUSE is a new experimental feature that allows to mount a distributed Tachyon File System
within the local file system hierarchy of a Linux node. Using this feature, standard tools
(for example, `ls`, `cat` or `echo`) and legacy POSIX applications will have basic access to
the distributed Tachyon data store.

Given the intrinsic characteristics of Tachyon, like its write-once/read-many-times file
data model, the mounted file system will not have full POSIX semantics and will have specific
limitations.  Please, read the rest of this document before using this feature to understand
what it can and cannot do for you.

# Requirements
* Linux kernel 2.6.9 or newer
* JDK 1.8 or newer
* libfuse 2.9.3 or newer
  (2.8.3 has been reported to also work - with some warnings)

# Building
tachyon-fuse is only built with Tachyon when the `buildFuse` maven profile is active. This
profile will be automatically activated by maven when it is detected that you are building
Tachyon with a JDK version 8 or newer.

For compatibility with Java 6 and 7, binary tachyon distributions may ship without tachyon-fuse
support, so you will need to build your own Tachyon if you want to use tachyon-fuse on your
deployment.

The best way to do so is to either clone the Tachyon [GitHub
repository](https://github.com/amplab/tachyon) and choose your favourite branch from git, or to
grab a [source distribution](https://github.com/amplab/tachyon/releases) directly. Please, refer to
[this page](http://tachyon-project.org/documentation/master/Building-Tachyon-Master-Branch.html))
for building instructions.

# Usage

## Mount Tachyon-FUSE
After having properly configured and started the tachyon cluster, and from the node where you
wish to mount Tachyon, point a shell to your `$TACHYON_HOME` and run:
```bash
$ bin/tachyon-fuse.sh mount <mount_point>
```
This will spawn a background user-space java process (tachyon-fuse) that will mount the file
system on the specified *<mount_point>*. Note that *<mount_point>* must be an existing and empty
path in your local file system hierarchy and that the user that runs the `tachyon-fuse.sh`
script must own the mount point and have read and write permissions on it. Also note that,
currently, you are limited to have only one Tachyon-FUSE mount per node.

## Unmount Tachyon-FUSE
To umount a previoulsy mounted Tachyon-FUSE file sytem, on the node where the file system is
mounted, point a shell to your `$TACHYON_HOME` and run:
```bash
$ bin/tachyon-fuse.sh umount
```
This will stop the background tachyon-fuse java process and unmount the file system.

## Check if Tachyon-FUSE is running
```bash
$ bin/tachyon-fuse.sh stat
```

## Optional configuration steps
Tachyon-FUSE is based on the standard java tachyon-client to perform its operations. You might
want to customize the behaviour of the tachyon client used by Tachyon-FUSE the same way you
would for any other client application.

One possibility, for example, is to edit `$TACHYON_HOME/bin/tachyon-fuse.sh` and add your
specific tachyon client options in the `TACHYON_JAVA_OPTS` variable.

# Operational assumptions and status
Currently, most basic file system operations are supported. However, due to Tachyon implicit
characteristics, please, be aware that:
* Files can be written only once, only sequentially, and never modified.
* Due to the above, any further access to a file must be read-only.

This translates in the following constraints on the UNIX system calls that will operate on the
file system:

## `open(const char* pathname, int flags, mode_t mode)`
(see also `man 2 open`)

If `pathname` indicates the path of a non-existing regular file in Tachyon, then an open will
only succeed if:
1. The base directory of `pathname` exists in Tachyon;
2. `O_CREAT` and `O_WRONLY` are passed among the `flags` bitfield.

Equivalently, `creat(const char* pathname )` calls will succeed as long as (1) holds and
`pathname` does not exist yet.

If `pathname`, instead, points to an existing regular file in Tachyon, then an open call will
only succeed if:
1. `O_RDONLY` is passed among the `flags` bitfield.

Note that, in either cases, the `mode` parameter is currently ignored by Tachyon-FUSE.

## `read(int fd, void* buf, size_t count)`
(see also `man 2 read`)

A read system call will only succeed when `fd` refers to a Tachyon file that has been previously
opened with the `O_RDONLY` flags.

## `lseek(int fd, off_t off, int whence)`
(see also `man 2 lseek`)

Seeking is supported only on files open for reading, i.e., on files that have been opened with an
`O_RDONLY` flag.

## `write(int fd, const void* buf, size_t count)`
(see also `man 2 write`)

A write system call will only succeed when `fd` refers to a Tachyon file that has been previously
opened  with the `O_WRONLY` flag.

# Performance considerations
Due to the conjunct use of FUSE and JNR, the performance of the mounted file system is expected
to be considerably worse than what you would see by using the `tachyon-client` directly. In other
words, if you are concerned about performance rather then functionality, then Tachyon-FUSE is
not what you are looking for.

Most of the problems come from the fact that there are several memory copies going on for each
call on `read` or `write` operations, and that FUSE caps the maximum granularity of writes to
128KB. This could be probably improved by a large extent by leveraging the FUSE cache write-backs
feature introduced in kernel 3.15 (not supported yet, however, by libfuse 2.x userspace libs).

# Configuration Parameters For Tachyon-FUSE

These are the configuration parameters for Tachyon-FUSE.

<table class="table table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
<tr>
  <td>tachyon.fuse.maxwrite.bytes</td>
  <td>131072</td>
  <td>
  The desired granularity of FUSE write upcalls in bytes. Note that 128K is currently an upper
  bound imposed by the linux kernel.
  </td>
</tr>
<tr>
  <td>tachyon.fuse.debug.enabled</td>
  <td>false</td>
  <td>
  Enable FUSE debug output. This output will be redirected in a `fuse.out` log file inside
  `tachyon.logs.dir`.
  </td>
</tr>
<tr>
  <td>tachyon.fuse.cachedpaths.max</td>
  <td>500</td>
  <td>
  Defines the size of the internal Tachyon-FUSE cache that maintains the most frequently used
  translations between local file system paths and Tachyon file URIs.
  </td>
</tr>
<tr>
  <td>tachyon.fuse.mount.default</td>
  <td>/mnt/tachyon</td>
  <td>
  Default mount point, only used when the user does not specify something else at mount time.
  </td>
</tr>
<tr>
  <td>tachyon.fuse.fs.root</td>
  <td>/mnt</td>
  <td>
  Path, within the Tachyon namespace, that will be used as the root of the FUSE mount.
  </td>
</tr>
<tr>
  <td>tachyon.fuse.fs.name</td>
  <td>tachyon-fuse</td>
  <td>
  Descriptive name used by FUSE to mount the file system.
  </td>
</tr>
</table>

# Acknowledgements
This project uses [jnr-fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.

