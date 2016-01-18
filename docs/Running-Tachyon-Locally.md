---
layout: global
title: Running Tachyon Locally
nickname: Tachyon on Local Machine
group: User Guide
priority: 1
---

# Run Tachyon Standalone on a Single Machine.

The prerequisite for this part is that you have [Java](Java-Setup.html) (JDK 7 or above).

Download the binary distribution of Tachyon {{site.TACHYON_RELEASED_VERSION}}:

```bash
$ wget http://tachyon-project.org/downloads/files/{{site.TACHYON_RELEASED_VERSION}}/tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
$ tar xvfz tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
$ cd tachyon-{{site.TACHYON_RELEASED_VERSION}}
```

Before executing Tachyon run scripts, requisite environment variables must be specified in
`conf/tachyon-env.sh`, which can be copied from the included template file:

```bash
$ cp conf/tachyon-env.sh.template conf/tachyon-env.sh
```

To run in standalone mode, make sure that:

* `TACHYON_UNDERFS_ADDRESS` in `conf/tachyon-env.sh` is set to a tmp directory in the local
filesystem (e.g., `export TACHYON_UNDERFS_ADDRESS=/tmp`).

* Remote login service is turned on so that `ssh localhost` can succeed.

Then, you can format Tachyon FileSystem and start it. *Note: since Tachyon needs to setup
[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt), starting a
local system requires users to input their root password for Linux based users. To avoid the need to
repeatedly input the root password, you can add the public ssh key for the host into 
`~/.ssh/authorized_keys`.*

```bash
$ ./bin/tachyon format
$ ./bin/tachyon-start.sh local
```

To verify that Tachyon is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder. You can
also run a sample program:

```bash
$ ./bin/tachyon runTest Basic CACHE THROUGH
```

For the first sample program, you should be able to see something similar to the following:

```bash
2015-11-20 08:32:22,271 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.2) is trying to connect with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-11-20 08:32:22,294 INFO   (ClientBase.java:connect) - Client registered with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-11-20 08:32:22,387 INFO   (BasicOperations.java:createFile) - createFile with fileId 33554431 took 127 ms.
2015-11-20 08:32:22,552 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.2) is trying to connect with BlockMaster master @ localhost/127.0.0.1:19998
2015-11-20 08:32:22,553 INFO   (ClientBase.java:connect) - Client registered with BlockMaster master @ localhost/127.0.0.1:19998
2015-11-20 08:32:22,604 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.2.15:29998
2015-11-20 08:32:22,698 INFO   (BasicOperations.java:writeFile) - writeFile to file /default_tests_files/BasicFile_CACHE_THROUGH took 311 ms.
2015-11-20 08:32:22,759 INFO   (FileUtils.java:createStorageDirPath) - Folder /Volumes/ramdisk/tachyonworker/7226211928567857329 was created!
2015-11-20 08:32:22,809 INFO   (LocalBlockOutStream.java:<init>) - LocalBlockOutStream created new file block, block path: /Volumes/ramdisk/tachyonworker/7226211928567857329/16777216
2015-11-20 08:32:22,886 INFO   (BasicOperations.java:readFile) - readFile file /default_tests_files/BasicFile_CACHE_THROUGH took 187 ms.
Passed the test!
```

And you can visit Tachyon web UI at **[http://localhost:19999](http://localhost:19999)** again.
Click `Browse File System` in the navigation bar and you should see the files written to Tachyon by
the above test.

To run a more comprehensive sanity check:

```bash
$ ./bin/tachyon runTests
```

You can stop Tachyon any time by running:

```bash
$ ./bin/tachyon-stop.sh all
```
