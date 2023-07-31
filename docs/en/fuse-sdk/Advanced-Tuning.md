---
layout: global
title: FUSE SDK Advanced Tuning
---


## Select Libfuse Version

Alluxio now supports both libfuse2 and libfuse3. Alluxio FUSE on libfuse2 is more stable and has been tested in production.
Alluxio FUSE on libfuse3 is currently experimental but under active development. Alluxio will focus more on libfuse3 and utilize new features provided.

**libfuse3** is used by default.

Set to use **libfuse2** via:
```shell
$ sudo yum install fuse
$ alluxio-fuse mount under_storage_dataset mount_point -o fuse=2
```

See `logs/fuse.out` for which version is used.
```
INFO  NativeLibraryLoader - Loaded libjnifuse with libfuse version 2(or 3).
```

## FUSE Mount Options

You can use `alluxio-fuse mount -o mount_option_a -o mount_option_b=value` to set mount options when launching the standalone Fuse process.

Different versions of `libfuse` and `osxfuse` may support different mount options.
The available Linux mount options are listed [here](http://man7.org/linux/man-pages/man8/mount.fuse3.8.html).
The mount options of MacOS with osxfuse are listed [here](https://github.com/osxfuse/osxfuse/wiki/Mount-options) .
Some mount options (e.g. `allow_other` and `allow_root`) need additional set-up
and the set-up process may be different depending on the platform.

```shell
$ alluxio-fuse mount <under_storage_dataset> <mount_point> -o mount_option
```

### Example: `allow_other` and `allow_root`

By default, Alluxio-FUSE mount point can only be accessed by the user
mounting the Alluxio namespace to the local filesystem.

For Linux, add the following line to file `/etc/fuse.conf` to allow other users
or allow root to access the mounted directory:
```
user_allow_other
```
Only after this step that non-root users have the permission to specify the `allow_other` or `allow_root` mount options.

For MacOS, follow the [osxfuse allow_other instructions](https://github.com/osxfuse/osxfuse/wiki/Mount-options)
to allow other users to use the `allow_other` and `allow_root` mount options.

After setting up, pass the `allow_other` or `allow_root` mount options when mounting Alluxio-FUSE:
```shell
# All users (including root) can access the files.
$ alluxio-fuse mount <under_storage_dataset> <mount_point> -o allow_other
```
```shell
# The user mounting the filesystem and root can access the files.
$ alluxio-fuse mount <under_storage_dataset> <mount_point> -o allow_root
```
Note that only one of the `allow_other` or `allow_root` could be set.

## Troubleshooting

This section talks about how to troubleshoot issues related to Alluxio POSIX API.

### Out of Direct Memory

When encountering the out of direct memory issue, add the following JVM opts to `${ALLUXIO_HOME}/conf/alluxio-env.sh` to increase the max amount of direct memory.

```sh
ALLUXIO_FUSE_JAVA_OPTS+=" -XX:MaxDirectMemorySize=8G"
```
