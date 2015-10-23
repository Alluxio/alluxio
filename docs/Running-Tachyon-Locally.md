---
layout: global
title: Running Tachyon Locally
nickname: Tachyon on Local Machine
group: User Guide
priority: 1
---

# Run Tachyon Standalone on a Single Machine.

The prerequisite for this part is that you have [Java](Java-Setup.html) (JDK 6 or above).

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
$ ./bin/tachyon runTest Basic STORE SYNC_PERSIST
```

For the first sample program, you should be able to see something similar to the following:

```bash
/default_tests_files/BasicFile_STORE_SYNC_PERSIST has been removed
2015-10-20 23:02:54,403 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,422 INFO   (ClientBase.java:connect) - Client registered with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,460 INFO   (BasicOperations.java:createFile) - createFile with fileId 1476395007 took 65 ms.
2015-10-20 23:02:54,557 INFO   (ClientBase.java:connect) - Tachyon client (version 0.8.0-SNAPSHOT) is trying to connect with BlockMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,558 INFO   (ClientBase.java:connect) - Client registered with BlockMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,590 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.31.242:29998
2015-10-20 23:02:54,654 INFO   (FileUtils.java:createStorageDirPath) - Folder /Volumes/ramdisk/tachyonworker/6601007274872912185 was created!
2015-10-20 23:02:54,657 INFO   (LocalBlockOutStream.java:<init>) - LocalBlockOutStream created new file block, block path: /Volumes/ramdisk/tachyonworker/6601007274872912185/1459617792
2015-10-20 23:02:54,658 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.31.242:29998
2015-10-20 23:02:54,754 INFO   (BasicOperations.java:writeFile) - writeFile to file /default_tests_files/BasicFile_STORE_SYNC_PERSIST took 294 ms.
2015-10-20 23:02:54,803 INFO   (BasicOperations.java:readFile) - readFile file /default_tests_files/BasicFile_STORE_SYNC_PERSIST took 47 ms.
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
$ ./bin/tachyon-stop.sh
```
