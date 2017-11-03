---
layout: global
title: Mounting Alluxio with FUSE
nickname: Alluxio-FUSE
group: Features
priority: 7
---

* Table of Contents
{:toc}

Alluxio-FUSE is a feature that allows mounting the distributed Alluxio File System as a standard file system on moust flavors of Unix. By using this feature, standard tools (for example, `ls`, `cat` or `mkdir`) and POSIX libraries like open, write, read will have basic access to the distributed Alluxio data store.

Alluxio-FUSE is based on the project [Filesystem in Userspace](http://fuse.sourceforge.net/) (FUSE). However, given the intrinsic characteristics of Alluxio, like its write-once/read-many-times file data model, the mounted file system will not have full POSIX semantics and will have specific limitations.  Please read the [section of limitations](#assumptions-and-limitations) for details.

## Requirements

* JDK 1.8 or newer
* [libfuse](https://github.com/libfuse/libfuse) 2.9.3 or newer (2.8.3 has been reported to also work - with some warnings) for Linux, or [osxfuse](https://osxfuse.github.io/) 3.7.1 or newer for MacOS

## Usage

### Mount Alluxio-FUSE

After having properly configured and started the alluxio cluster, and from the node where you wish to mount Alluxio, point a shell to your `$ALLUXIO_HOME` and run:

{% include Mounting-Alluxio-FS-with-FUSE/alluxio-fuse-mount.md %}

This will spawn a background user-space java process (alluxio-fuse) that will mount the file system on the specified `mount_point` to the `alluxio_path`. 

For example, the following command will mount the alluxio path `/people` to the folder `/mnt/people` in the local file system.

```bash
$ integration/fuse/bin/alluxio-fuse.sh mount /mnt/people /people
```

When `alluxio_path` is not given, Alluxio-FUSE defaults it to root (`/`). Note that `mount_point` must be an existing and empty path in your local file system hierarchy and that the user that runs the `alluxio-fuse.sh` script must own the mount point and have read and write permissions on it. You can mount multiple mount points, and all of these alluxio-fuse processes share the same log output at `$ALLUXIO_HOME\logs\fuse.log`.

### Unmount Alluxio-FUSE

To umount a previoulsy mounted Alluxio-FUSE file sytem, on the node where the file system is mounted, point a shell to your `$ALLUXIO_HOME` and run:

{% include Mounting-Alluxio-FS-with-FUSE/alluxio-fuse-umount.md %}

This unmounts the file system at the mounting point and stops the corresponding alluxio-fuse process.

### Check if Alluxio-FUSE is running

{% include Mounting-Alluxio-FS-with-FUSE/alluxio-fuse-stat.md %}

This outputs the `pid, mount_point, alluxio_path` of all the running alluxio-fuse processes.

For example, the output will be like:

```bash
pid	mount_point	alluxio_path
80846	/mnt/people	/people
80847	/mnt/sales	/sales
```

### Optional configuration steps

Alluxio-FUSE is based on the standard java `alluxio-core-client-fs` to perform its operations. You
might want to customize the behaviour of the alluxio client used by Alluxio-FUSE the same way you
would for any other client application.

One possibility, for example, is to edit `$ALLUXIO_HOME/conf/alluxio-site.properties` and set your specific alluxio client options.

## Assumptions and limitations

Currently, most basic file system operations are supported. However, due to Alluxio implicit characteristics, please, be aware that:

* Files can be written only once, only sequentially, and never be modified.
* Due to the above, any further access to a file must be read-only.

This translates in the following constraints on the UNIX system calls that will operate on the file system:

### `open(const char* pathname, int flags, mode_t mode)` (see also `man 2 open`)

If `pathname` indicates the path of a non-existing regular file in Alluxio, then an open will only succeed if:

1. The base directory of `pathname` exists in Alluxio;
2. `O_CREAT` and `O_WRONLY` are passed among the `flags` bitfield.

Equivalently, `creat(const char* pathname )` calls will succeed as long as (1) holds and `pathname` does not exist yet.

If `pathname`, instead, points to an existing regular file in Alluxio, then an open call will only succeed if:

1. `O_RDONLY` is passed among the `flags` bitfield.

Note that, in either cases, the `mode` parameter is currently ignored by Alluxio-FUSE.

### `read(int fd, void* buf, size_t count)` (see also `man 2 read`)

A read system call will only succeed when `fd` refers to an Alluxio file that has been previously opened with the `O_RDONLY` flags.

### `lseek(int fd, off_t off, int whence)` (see also `man 2 lseek`)

Seeking is supported only on files open for reading, i.e., on files that have been opened with an `O_RDONLY` flag.

### `write(int fd, const void* buf, size_t count)` (see also `man 2 write`)

A write system call will only succeed when `fd` refers to an Alluxio file that has been previously
opened  with the `O_WRONLY` flag.

## Performance considerations

Due to the conjunct use of FUSE and JNR, the performance of the mounted file system is expected to be worse than what you would see by using the `alluxio-core-client-fs` directly.

Most of the problems come from the fact that there are several memory copies going on for each call on `read` or `write` operations, and that FUSE caps the maximum granularity of writes to 128KB. This could be probably improved by a large extent by leveraging the FUSE cache write-backs feature introduced in kernel 3.15 (not supported yet, however, by libfuse 2.x userspace libs).

## Configuration Parameters For Alluxio-FUSE

These are the configuration parameters for Alluxio-FUSE.

<table class="table table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
{% for item in site.data.table.Alluxio-FUSE-parameter %}
  <tr>
    <td>{{ item.parameter }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.Alluxio-FUSE-parameter[item.parameter] }}</td>
  </tr>
{% endfor %}
</table>

## Building manually

You can build Alluxio-FUSE with the maven profile `fuse`. This profile will be automatically activated by maven when it is detected that you are building Alluxio with a JDK version 8 or newer. For compatibility with Java 7, binary Alluxio distributions for Java 7 ship without alluxio-fuse support.

The best way to do so is to either clone the Alluxio [GitHub repository](https://github.com/alluxio/alluxio) and choose your favourite branch from git, or to grab a [source distribution](https://github.com/alluxio/alluxio/releases) directly. Please refer to [this page](Building-Alluxio-Master-Branch.html)) for building instructions.

## Acknowledgements

This project uses [jnr-fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.
