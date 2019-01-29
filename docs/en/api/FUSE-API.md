---
layout: global
title: FUSE-based POSIX API
nickname: POSIX API
group: Client APIs
priority: 3
---

* Table of Contents
{:toc}

Alluxio-FUSE is a feature that allows mounting the distributed Alluxio File System as a standard
file system on most flavors of Unix. By using this feature, standard bash tools (for example, `ls`,
`cat` or `mkdir`) will have basic access to the distributed Alluxio data store. More importantly,
with FUSE, an application written in any language like C, C++, Python, Ruby, Perl, or Java, can
interact with Alluxio by using standard POSIX APIs like `open, write, read`, without any Alluxio
client integration or set up.

Alluxio-FUSE is based on the project [Filesystem in Userspace](http://fuse.sourceforge.net/) (FUSE),
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
$ integration/fuse/bin/alluxio-fuse mount mount_point [alluxio_path]
```

This will spawn a background user-space java process (alluxio-fuse) that will mount the Alluxio path
specified at `alluxio_path` to the local file system on the specified `mount_point`.

For example, the following command will mount the Alluxio path `/people` to the folder `/mnt/people`
in the local file system.

```bash
$ integration/fuse/bin/alluxio-fuse mount /mnt/people /people
Starting alluxio-fuse on local host. Alluxio-fuse mounted at /mnt/people. See /lib/alluxio/logs/fuse.log for logs
```

When `alluxio_path` is not given, Alluxio-FUSE defaults it to root (`/`). Note that the
`mount_point` must be an existing and empty path in your local file system hierarchy and that the
user that runs the `alluxio-fuse.sh` script must own the mount point and have read and write
permissions on it. You can mount Alluxio to multiple mount points. All of these alluxio-fuse
processes share the same log output at `$ALLUXIO_HOME\logs\fuse.log`, which is useful for
troubleshooting when errors happen on operations under the mounting point.

### Unmount Alluxio-FUSE

To umount a previously mounted Alluxio-FUSE file sytem, on the node where the file system is
mounted, point a shell to your `$ALLUXIO_HOME` and run:

```bash
$ integration/fuse/bin/alluxio-fuse umount mount_point
```

This unmounts the file system at the mounting point and stops the corresponding alluxio-fuse
process. For example,

```bash
$ integration/fuse/bin/alluxio-fuse umount /mnt/people
Unmount fuse at /mnt/people (PID:97626).
```

### Check the Alluxio-FUSE mounting status

To list the mounting points, on the node where the file system is mounted, point a shell to your
`$ALLUXIO_HOME` and run:

```bash
$ integration/fuse/bin/alluxio-fuse stat
```

This outputs the `pid, mount_point, alluxio_path` of all the running alluxio-fuse processes.

For example, the output will be like:

```bash
pid	mount_point	alluxio_path
80846	/mnt/people	/people
80847	/mnt/sales	/sales
```

## Advanced configuration

### Configure Alluxio client options

Alluxio-FUSE is based on the standard Java client API `alluxio-core-client-fs` to perform its
operations. You might want to customize the behaviour of the Alluxio client used by Alluxio-FUSE the
same way you would for any other client application.

One possibility, for example, is to edit `$ALLUXIO_HOME/conf/alluxio-site.properties` and set your
specific Alluxio client options. Note that these changes should be before Alluxio-FUSE starts.

## Assumptions and limitations

Currently, most basic file system operations are supported. However, due to Alluxio implicit
characteristics, please be aware that:

* Files can be written only once, only sequentially, and never be modified. That means overriding a
  file is not allowed, and an explicit combination of delete and then create is needed. For example,
  `cp` command will fail when the destination file exists.
* Alluxio does not have hard-link and soft-link concepts, so the commands like `ln` are not supported,
  neither the hardlinks number is displayed in `ll` output.
* The user and group are mapped to the Unix user and group only when Alluxio Fuse is configured to use
  shell user group translation service, by setting `alluxio.fuse.user.group.translation.enabled` to `true` 
  in `conf/alluxio-site.properties`. Otherwise `chown` and `chgrp` are no-ops, and `ll` will return the
  user and group of the user who started the alluxio-fuse process. The translation service
  does not change the actual file permission when running `ll`. 

## Performance considerations

Due to the conjunct use of FUSE and JNR, the performance of the mounted file system is expected to
be worse than what you would see by using the
[Alluxio Java client]({{ '/en/api/FS-API.html' | relativize_url }}#Java-Client) directly.

Most of the overheads come from the fact that there are several memory copies going on for each call
on `read` or `write` operations, and that FUSE caps the maximum granularity of writes to 128KB. This
could be probably improved by a large extent by leveraging the FUSE cache write-backs feature
introduced in kernel 3.15 (supported by libfuse 3.x userspace libs but not supported in jnr-fuse yet).

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

## Acknowledgements

This project uses [jnr-fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.
