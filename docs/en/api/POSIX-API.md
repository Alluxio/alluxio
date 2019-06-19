---
layout: global
title: FUSE-based POSIX API
nickname: POSIX API
group: Client APIs
priority: 3
---

* Table of Contents
{:toc}

Alluxio POSIX API is a feature that allows mounting the distributed Alluxio File System as a standard
file system on most flavors of Unix. By using this feature, standard bash tools (for example, `ls`,
`cat` or `mkdir`) will have basic access to the distributed Alluxio data store. More importantly,
with this POSIX API, applications which can interact with the local filesystem, no matter what languages 
(C, C++, Python, Ruby, Perl, or Java) they are written in, can interact with Alluxio and its under storages
without any Alluxio client integration or set up. 

Note that, different from projects like [s3fs](https://s3fs.readthedocs.io/en/latest/), [mountableHdfs](https://wiki.apache.org/hadoop/MountableHDFS) 
which can mount specific storage service like S3 or HDFS as local filesystem, the Alluxio POSIX API 
is a generic solution for all storage systems supported by Alluxio. The rich data orchestration 
and caching service inherited from Alluxio speeds up the I/O access to frequently used data in Alluxio worker memory space.

<p align="center">
<img src="{{ '/img/stack-posix.png' | relativize_url }}" alt="Alluxio stack with its POSIX API"/>
</p>

Alluxio POSIX API is based on the project [Filesystem in Userspace](http://fuse.sourceforge.net/) (FUSE),
and most basic file system operations are supported. However, given the intrinsic characteristics of
Alluxio, like its write-once/read-many-times file data model, the mounted file system will not have
full POSIX semantics and will have specific limitations.  Please read the [section of limitations
](#assumptions-and-limitations) for details.

## Requirements

* JDK 1.8 or newer
* [libfuse](https://github.com/libfuse/libfuse) 2.9.3 or newer (2.8.3 has been
  reported to also work - with some warnings) for Linux
* [osxfuse](https://osxfuse.github.io/) 3.7.1 or newer for MacOS

## Usage

### Mount Alluxio-FUSE

After having properly configured and started the Alluxio cluster, and from the node where you wish
to mount Alluxio, point a shell to your `$ALLUXIO_HOME` and run:

```bash
integration/fuse/bin/alluxio-fuse mount mount_point [alluxio_path]
```

This will spawn a background user-space java process (alluxio-fuse) that will mount the Alluxio path
specified at `alluxio_path` to the local file system on the specified `mount_point`.

For example, the following command will mount the Alluxio path `/people` to the folder `/mnt/people`
in the local file system.

```bash
sudo mkdir -p /mnt/people
sudo chown $(whoami) /mnt/people
chmod 755 /mnt/people
integration/fuse/bin/alluxio-fuse mount /mnt/people /people
```

When `alluxio_path` is not given, Alluxio-FUSE defaults it to root (`/`). Note that the
`mount_point` must be an existing and empty path in your local file system hierarchy and that the
user that runs the `alluxio-fuse.sh` script must own the mount point and have read and write
permissions on it. You can mount Alluxio to multiple mount points. All of these alluxio-fuse
processes share the same log output at `$ALLUXIO_HOME\logs\fuse.log`, which is useful for
troubleshooting when errors happen on operations under the mounting point.

### Unmount Alluxio-FUSE

To unmount a previously mounted Alluxio-FUSE file system, on the node where the file system is
mounted, point a shell to your `$ALLUXIO_HOME` and run:

```bash
integration/fuse/bin/alluxio-fuse unmount mount_point
```

This unmounts the file system at the mounting point and stops the corresponding alluxio-fuse
process. For example,

```bash
integration/fuse/bin/alluxio-fuse unmount /mnt/people
Unmount fuse at /mnt/people (PID:97626).
```

### Check the Alluxio POSIX API mounting status

To list the mounting points, on the node where the file system is mounted, point a shell to your
`$ALLUXIO_HOME` and run:

```bash
integration/fuse/bin/alluxio-fuse stat
```

This outputs the `pid, mount_point, alluxio_path` of all the running Alluxio-FUSE processes.

For example, the output will be like:

```bash
pid	mount_point	alluxio_path
80846	/mnt/people	/people
80847	/mnt/sales	/sales
```

## Advanced configuration

### Configure Alluxio client options

Alluxio POSIX API is based on the standard Java client API `alluxio-core-client-fs` to perform its
operations. You might want to customize the behaviour of the Alluxio client used by Alluxio POSIX API the
same way you would for any other client application.

One possibility, for example, is to edit `$ALLUXIO_HOME/conf/alluxio-site.properties` and set your
specific Alluxio client options. Note that these changes should be done before the mounting steps.

### Configure mount point options

You can use `-o [mount options]` to set mount options.
If you want to set multiple mount options, you can pass in comma separated mount options as the value of `-o`.
The `-o [mount options]` must follow the `mount` command.

The available Linux mount options are listed [here](http://man7.org/linux/man-pages/man8/mount.fuse.8.html).
The mount options of MacOS with osxfuse are listed [here](https://github.com/osxfuse/osxfuse/wiki/Mount-options) .
Some mount options (e.g. `allow_other` and `allow_root`) need additional set-up
and the set up process may be different according to platforms. 

```bash
integration/fuse/bin/alluxio-fuse mount -o [comma separated mount options] mount_point [alluxio_path]
```

Note that `direct_io` mount option is set by default so that writes and reads bypass the kernel page cache
and go directly to Alluxio.

Note that different versions of libfuse and osxfuse support different mount options.

#### Example: allow_other or allow_root

By default, Alluxio Fuse mount point can only be accessed by the user
mounting the Alluxio namespace to the local filesystem.

For Linux, add the following line to file `/etc/fuse.conf` to allow other users
or allow root to access the mounted folder:

```
user_allow_other
```

This option allow non-root users to specify the `allow_other` or `allow_root` mount options.

For MacOS, follow the [osxfuse allow_other instructions](https://github.com/osxfuse/osxfuse/wiki/Mount-options)
to allow other users to use the `allow_other` and `allow_root` mount options.

After setting up, pass the `allow_other` or `allow_root` mount options when mounting Alluxio-Fuse:

```bash
# All users (including root) can access the files.
integration/fuse/bin/alluxio-fuse mount -o allow_other mount_point [alluxio_path]
# The user mounting the filesystem and root can access the files.
integration/fuse/bin/alluxio-fuse mount -o allow_root mount_point [alluxio_path]
```

Note that only one of the `allow_other` or `allow_root` could be set.

## Assumptions and limitations

Currently, most basic file system operations are supported. However, due to Alluxio implicit
characteristics, please be aware that:

* Files can be written only once, only sequentially, and never be modified. That means overriding a
  file is not allowed, and an explicit combination of delete and then create is needed. For example,
  `cp` command will fail when the destination file exists.
* Alluxio does not have hard-link and soft-link concepts, so the commands like `ln` are not supported,
  neither the hardlinks number is displayed in `ll` output.
* The user and group are mapped to the Unix user and group only when Alluxio POSIX API is configured to use
  shell user group translation service, by setting `alluxio.fuse.user.group.translation.enabled` to `true`
  in `conf/alluxio-site.properties`. Otherwise `chown` and `chgrp` are no-ops, and `ll` will return the
  user and group of the user who started the Alluxio-FUSE process. The translation service
  does not change the actual file permission when running `ll`.

## Performance considerations

Due to the conjunct use of FUSE and JNR, the performance of the mounted file system is expected to
be worse than what you would see by using the
[Alluxio Java client]({{ '/en/api/FS-API.html' | relativize_url }}#java-client) directly.

Most of the overheads come from the fact that there are several memory copies going on for each call
on `read` or `write` operations, and that FUSE caps the maximum granularity of writes to 128KB. This
could be probably improved by a large extent by leveraging the FUSE cache write-backs feature
introduced in kernel 3.15 (supported by libfuse 3.x userspace libs but not supported in jnr-fuse yet).

## Configuration Parameters For Alluxio POSIX API

These are the configuration parameters for Alluxio POSIX API.

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

## Acknowledgements

This project uses [jnr-fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.
