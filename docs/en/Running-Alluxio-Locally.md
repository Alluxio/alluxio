---
layout: global
title: Running Alluxio Locally
nickname: Alluxio on Local Machine
group: Deploying Alluxio
priority: 1
---

* Table of Contents
{:toc}

# Requirement

The prerequisite for this part is that you have [Java](Java-Setup.html) (JDK 7 or above).

Download the binary distribution of Alluxio {{site.ALLUXIO_RELEASED_VERSION}}:

{% include Running-Alluxio-Locally/download-Alluxio-binary.md %}

To run in standalone mode, make sure that:

* Set `alluxio.master.hostname` in `conf/alluxio-site.properties` to `localhost` (i.e., `alluxio.master.hostname=localhost`).

* Set `alluxio.underfs.address` in `conf/alluxio-site.properties` to a tmp directory in the local
filesystem (e.g., `alluxio.underfs.address=/tmp`).

* Remote login service is turned on so that `ssh localhost` can succeed. To avoid the need to
repeatedly input the password, you can add the public ssh key for the host into
`~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html) for more details.

# Step 0: Format Alluxio Filesystem

> NOTE: This step is only required when you run Alluxio for the first time.
> If you run this command for an existing Alluxio cluster, 
> all previously stored data and metadata in Alluxio filesystem will be erased.
> However, data in under storage will not be changed.

```bash
$ ./bin/alluxio format
```

# Step 1: Start Alluxio Filesystem Locally

Simply run the following command to start Alluxio filesystem.

```bash
$ ./bin/alluxio-start.sh local
```

> NOTE: On Linux, this command may require to input password to get sudo privileges 
> in order to setup RAMFS. If you do not want to type in the password every time, or you do 
> not even have sudo privileges, please read the alternative approaches in [FAQ](#faq).

## Verify Alluxio is running

To verify that Alluxio is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder. 

To run a more comprehensive sanity check:

{% include Running-Alluxio-Locally/run-tests.md %}

You can stop Alluxio any time by running:

{% include Running-Alluxio-Locally/Alluxio-stop.md %}


# FAQ

## Why is sudo privilege needed to start Alluxio on Linux?

By default, Alluxio filesystem uses [RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt) as its in-memory data storage. It turns out on MacOS, it is fine for Alluxio to mount a RAMFS without being a super user. However, on Linux, it requires sudo privileges to perform "mount" (and the followed "umount", "mkdir" and "chmod" operations). 

## Can I still try Alluxio on Linux without sudo privileges?

If you have no sudo privileges on Linux, for Alluxio Filesystem to work, it requires a RAMFS (e.g., `/path/to/ramdisk`) already mounted
by the system admin and accessible for read/write-operations by the user. In this case you have can specify the path in
`conf/alluxio-site.properties`:

```
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/path/to/ramdisk
```

and then start Alluxio with `NoMount` option to use the above directory as its data storage:

```bash
$ ./bin/alluxio-start.sh local NoMount
```

Alternatively, you can also specify Linux [tmpFS](https://en.wikipedia.org/wiki/Tmpfs)  
as the data storage. Tmpfs is a temporary file storage backed by memory (e.g., typically `/dev/shm` on Linux), but may use SWAP space and
therefore provides less performance guarantees compared to ramfs. Similar to using a pre-mounted RAMFS, you can specify the tempfs path in
`conf/alluxio-site.properties`:

```
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/dev/shm
```

followed by:

```bash
$ ./bin/alluxio-start.sh local NoMount
```

## How can I avoid typing the password to run sudo? 

Options:

* Start Alluxio as a superuser.
* Add the user to start Alluxio in [suderors](https://help.ubuntu.com/community/Sudoers).
* Give limited sudo privileges to the running user (e.g., "alluxio") by adding the following line to `/etc/sudoers` on Linux:
`alluxio ALL=(ALL) NOPASSWD: /bin/mount * /mnt/ramdisk, /bin/umount * /mnt/ramdisk, /bin/mkdir * /mnt/ramdisk, /bin/chmod * /mnt/ramdisk`
This allows Linux user "alluxio" to mount, umount, mkdir and chmod (assume they are in `/bin/`) a specific path `/mnt/ramdisk`
with sudo privileges without typing the password, but nothing else.
See more detailed explanation about [Sudoer User Specifications](https://help.ubuntu.com/community/Sudoers#User_Specifications).
