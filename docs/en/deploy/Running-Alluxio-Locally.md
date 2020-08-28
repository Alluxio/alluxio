---
layout: global
title: Deploy Alluxio Locally
nickname: Local Machine
group: Install Alluxio
priority: 1
---

This guide goes over how to run and test Alluxio on your local machine.

* Table of Contents
{:toc}

## Requirements

The prerequisite for this part is that you have a version of
[Java 8](https://adoptopenjdk.net/releases.html?variant=openjdk8&jvmVariant=hotspot)
installed.

[Download](https://alluxio.io/download) the binary distribution of Alluxio.

To run in standalone mode, do the following:

* Create `conf/alluxio-site.properties` by copying `conf/alluxio-site.properties.template`
* Set `alluxio.master.hostname` in `conf/alluxio-site.properties` to `localhost` (i.e.,
`alluxio.master.hostname=localhost`).
* Set `alluxio.master.mount.table.root.ufs` in `conf/alluxio-site.properties` to a tmp directory in
  the local filesystem (e.g., `alluxio.master.mount.table.root.ufs=/tmp`).
* Turn on remote login service so that `ssh localhost` can succeed. To avoid the need to
repeatedly input the password, you can add the public SSH key for the host into
`~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html) for more
details.

## Mount RAMFS file system

> Run the below command to mount RAMFS file system.

```console
$ ./bin/alluxio-mount.sh SudoMount
```

## Format Alluxio Filesystem

> NOTE: This step is only required when you run Alluxio for the first time.
> If you run this command for an existing Alluxio cluster,
> all previously stored data and metadata in Alluxio filesystem will be erased.
> However, data in under storage will not be changed.

```console
$ ./bin/alluxio format
```

## Start Alluxio Filesystem Locally

Simply run the following command to start Alluxio filesystem.

```console
# If you have not mounted the ramdisk or want to remount it (ie. to change the size)
$ ./bin/alluxio-start.sh local SudoMount
# OR if you have already mounted the ramdisk
$ ./bin/alluxio-start.sh local
```

> NOTE: On Linux, this command may require to input password to get sudo privileges in order to
> mount the RAMFS.
> If you do not want to type in the password every time, or you do not have sudo privileges, please
> read the alternative approaches in [FAQ](#faq).

## Verify Alluxio is running

To verify that Alluxio is running, visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder.

To run a more comprehensive sanity check:

```console
$ ./bin/alluxio runTests
```

You can stop Alluxio any time by running:

```console
$ ./bin/alluxio-stop.sh local
```


## FAQ

### Why is sudo privilege needed to start Alluxio on Linux?

By default, the Alluxio filesystem uses
[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt) as its
in-memory data storage.
On MacOS, it is fine for Alluxio to mount a RAMFS without being a super user.
However, on Linux, it requires sudo privileges to perform `mount` (and the associated `umount`,
`mkdir` and `chmod` operations).

### Can I still try Alluxio on Linux without sudo privileges?

If you have no sudo privileges on Linux, for Alluxio Filesystem to work, it requires a RAMFS (e.g.,
`/path/to/ramdisk`) already mounted by the system admin and accessible for read/write-operations by
the user.
In this case you have can specify the path in `conf/alluxio-site.properties`:

```properties
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/path/to/ramdisk
```

and then start Alluxio with `NoMount` option to use the above directory as its data storage:

```console
$ ./bin/alluxio-start.sh local NoMount
```

Alternatively, you can also specify Linux [tmpFS](https://en.wikipedia.org/wiki/Tmpfs)
as the data storage.
Tmpfs is a temporary file storage backed by memory (e.g., typically `/dev/shm` on Linux), but may
use SWAP space and therefore provides less performance guarantees compared to ramfs.
Similar to using a pre-mounted RAMFS, you can specify the tempfs path in
`conf/alluxio-site.properties`:

```properties
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/dev/shm
```

followed by:

```console
$ ./bin/alluxio-start.sh local NoMount
```

### How can I avoid typing the password to run `sudo`?

Options:

* Start Alluxio as a super user.
* Add the user who starts Alluxio to the [sudoers](https://help.ubuntu.com/community/Sudoers) file.
* Give limited sudo privileges to the running user (e.g., `alluxio`) by adding the following line to
`/etc/sudoers` on Linux:

```
alluxio ALL=(ALL) NOPASSWD: /bin/mount * /mnt/ramdisk, /bin/umount */mnt/ramdisk, /bin/mkdir * /mnt/ramdisk, /bin/chmod * /mnt/ramdisk
```

This allows Linux user "alluxio" to mount, umount, mkdir and chmod (assume they are in `/bin/`) a
specific path `/mnt/ramdisk` with sudo privileges without typing the password, but nothing else.
See more detailed explanation about [Sudoer User
Specifications](https://help.ubuntu.com/community/Sudoers#User_Specifications).
