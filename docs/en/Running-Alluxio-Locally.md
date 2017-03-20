---
layout: global
title: Running Alluxio Locally
nickname: Alluxio on Local Machine
group: Deploying Alluxio
priority: 1
---

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

> This step is only required when you run Alluxio for the first time.
> All previously stored data and metadata in an Alluxio filesystem on this server will be erased.

```bash
$ ./bin/alluxio format
```

# Step 1: Start Alluxio Filesystem Locally

## Option1: if you have sudo privileges

By default, on startup Alluxio will create a
[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt) as its in-memory data storage.
This step requires sudo privileges to perform "mount", "umount", "mkdir" and "chmod" operations. There are two approaches to achieve this:

* Start Alluxio by a superuser, or
* Give limited sudo privileges to the running user (e.g., "alluxio") by adding the following line to `/etc/sudoers` on Linux:
`alluxio ALL=(ALL) NOPASSWD: /bin/mount * /mnt/ramdisk, /bin/umount * /mnt/ramdisk, /bin/mkdir * /mnt/ramdisk, /bin/chmod * /mnt/ramdisk`
This allows Linux user "alluxio" to mount, umount, mkdir and chmod (assume they are in `/bin/`) a specific path `/mnt/ramdisk`
with sudo privileges without typing the password, but nothing else.
See more detailed explanation about [Sudoer User Specifications](https://help.ubuntu.com/community/Sudoers#User_Specifications).

With the proper user, run the following command to start Alluxio filesystem.

```bash
$ ./bin/alluxio-start.sh local
```

## Option 2: If you do not have sudo privileges and a RAMFS (e.g., `/path/to/ramdisk`) is already mounted by the system admin

In this case you can specify the path in
`conf/alluxio-site.properties`:

```
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/path/to/ramdisk
```

and start Alluxio without requiring sudo privileges:

```bash
$ ./bin/alluxio-start.sh local NoMount
```

## Verify Alluxio is running

To verify that Alluxio is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder. You can
also run a sample program:

{% include Running-Alluxio-Locally/run-sample.md %}

For the first sample program, you should be able to see something similar to the following:

{% include Running-Alluxio-Locally/first-sample-output.md %}

And you can visit Alluxio web UI at **[http://localhost:19999](http://localhost:19999)** again.
Click `Browse` in the navigation bar and you should see the files written to Alluxio by
the above test.

To run a more comprehensive sanity check:

{% include Running-Alluxio-Locally/run-tests.md %}

You can stop Alluxio any time by running:

{% include Running-Alluxio-Locally/Alluxio-stop.md %}
