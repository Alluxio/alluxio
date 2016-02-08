---
layout: global
title: Mounting Alluxio with FUSE (Beta)
nickname: Alluxio-FUSE
group: Features
priority: 4
---

* Table of Contents
{:toc}

Alluxio-FUSE is a new experimental feature that allows to mount a distributed Alluxio File System
within the local file system hierarchy of a Linux node. Using this feature, standard tools
(for example, `ls`, `cat` or `echo`) and legacy POSIX applications will have basic access to
the distributed Alluxio data store.

Given the intrinsic characteristics of Alluxio, like its write-once/read-many-times file
data model, the mounted file system will not have full POSIX semantics and will have specific
limitations.  Please, read the rest of this document before using this feature to understand
what it can and cannot do for you.

# Requirements

* Linux kernel 2.6.9 or newer
* JDK 1.8 or newer
* libfuse 2.9.3 or newer
  (2.8.3 has been reported to also work - with some warnings)

# Building

alluxio-fuse is only built with Alluxio when the `buildFuse` maven profile is active. This
profile will be automatically activated by maven when it is detected that you are building
Alluxio with a JDK version 8 or newer.

For compatibility with Java 7, binary alluxio distributions may ship without alluxio-fuse
support, so you will need to build your own Alluxio if you want to use alluxio-fuse on your
deployment.

The best way to do so is to either clone the Alluxio [GitHub
repository](https://github.com/amplab/alluxio) and choose your favourite branch from git, or to
grab a [source distribution](https://github.com/amplab/alluxio/releases) directly. Please, refer to
[this page](Building-Alluxio-Master-Branch.html))
for building instructions.

# Usage

## Mount Alluxio-FUSE

After having properly configured and started the alluxio cluster, and from the node where you
wish to mount Alluxio, point a shell to your `$ALLUXIO_HOME` and run:

{% include Mounting-Alluxio-FS-with-FUSE/alluxio-fuse-mount.md %}

This will spawn a background user-space java process (alluxio-fuse) that will mount the file
system on the specified `<mount_point>`. Note that `<mount_point>` must be an existing and empty
path in your local file system hierarchy and that the user that runs the `alluxio-fuse.sh`
script must own the mount point and have read and write permissions on it. Also note that,
currently, you are limited to have only one Alluxio-FUSE mount per node.

## Unmount Alluxio-FUSE

To umount a previoulsy mounted Alluxio-FUSE file sytem, on the node where the file system is
mounted, point a shell to your `$ALLUXIO_HOME` and run:

{% include Mounting-Alluxio-FS-with-FUSE/alluxio-fuse-umount.md %}

This will stop the background alluxio-fuse java process and unmount the file system.

## Check if Alluxio-FUSE is running

{% include Mounting-Alluxio-FS-with-FUSE/alluxio-fuse-stat.md %}

## Optional configuration steps

Alluxio-FUSE is based on the standard java alluxio-core-client to perform its operations. You might
want to customize the behaviour of the alluxio client used by Alluxio-FUSE the same way you
would for any other client application.

One possibility, for example, is to edit `$ALLUXIO_HOME/bin/alluxio-fuse.sh` and add your
specific alluxio client options in the `ALLUXIO_JAVA_OPTS` variable.

# Operational assumptions and status

Currently, most basic file system operations are supported. However, due to Alluxio implicit
characteristics, please, be aware that:

* Files can be written only once, only sequentially, and never modified.
* Due to the above, any further access to a file must be read-only.

This translates in the following constraints on the UNIX system calls that will operate on the
file system:

## `open(const char* pathname, int flags, mode_t mode)`
(see also `man 2 open`)

If `pathname` indicates the path of a non-existing regular file in Alluxio, then an open will
only succeed if:

1. The base directory of `pathname` exists in Alluxio;
2. `O_CREAT` and `O_WRONLY` are passed among the `flags` bitfield.

Equivalently, `creat(const char* pathname )` calls will succeed as long as (1) holds and
`pathname` does not exist yet.

If `pathname`, instead, points to an existing regular file in Alluxio, then an open call will
only succeed if:

1. `O_RDONLY` is passed among the `flags` bitfield.

Note that, in either cases, the `mode` parameter is currently ignored by Alluxio-FUSE.

## `read(int fd, void* buf, size_t count)`
(see also `man 2 read`)

A read system call will only succeed when `fd` refers to a Alluxio file that has been previously
opened with the `O_RDONLY` flags.

## `lseek(int fd, off_t off, int whence)`
(see also `man 2 lseek`)

Seeking is supported only on files open for reading, i.e., on files that have been opened with an
`O_RDONLY` flag.

## `write(int fd, const void* buf, size_t count)`
(see also `man 2 write`)

A write system call will only succeed when `fd` refers to a Alluxio file that has been previously
opened  with the `O_WRONLY` flag.

# Performance considerations

Due to the conjunct use of FUSE and JNR, the performance of the mounted file system is expected
to be considerably worse than what you would see by using the `alluxio-core-client` directly. In other
words, if you are concerned about performance rather then functionality, then Alluxio-FUSE is
not what you are looking for.

Most of the problems come from the fact that there are several memory copies going on for each
call on `read` or `write` operations, and that FUSE caps the maximum granularity of writes to
128KB. This could be probably improved by a large extent by leveraging the FUSE cache write-backs
feature introduced in kernel 3.15 (not supported yet, however, by libfuse 2.x userspace libs).

# Configuration Parameters For Alluxio-FUSE

These are the configuration parameters for Alluxio-FUSE.

<table class="table table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
{% for item in site.data.table.Alluxio-FUSE-parameter %}
  <tr>
    <td>{{ item.parameter }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.Alluxio-FUSE-parameter.[item.parameter] }}</td>
  </tr>
{% endfor %}
</table>

# Acknowledgements

This project uses [jnr-fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.
