---
layout: global
title: Running Alluxio Locally
nickname: Alluxio on Local Machine
group: User Guide
priority: 1
---

# Run Alluxio Standalone on a Single Machine.

The prerequisite for this part is that you have [Java](Java-Setup.html) (JDK 7 or above).

Download the binary distribution of Alluxio {{site.ALLUXIO_RELEASED_VERSION}}:

{% include Running-Alluxio-Locally/download-Alluxio-binary.md %}

Before executing Alluxio run scripts, the Alluxio environment configuration `conf/alluxio-env.sh`
needs to be created. The default configuration can be bootstrapped using:

{% include Running-Alluxio-Locally/bootstrap.md %}

To run in standalone mode, make sure that:

* `ALLUXIO_UNDERFS_ADDRESS` in `conf/alluxio-env.sh` is set to a tmp directory in the local
filesystem (e.g., `export ALLUXIO_UNDERFS_ADDRESS=/tmp`).

* Remote login service is turned on so that `ssh localhost` can succeed.

Then, you can format Alluxio FileSystem and start it. *Note: since Alluxio needs to setup
[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt), starting a
local system requires users to input their root password for Linux based users. To avoid the need to
repeatedly input the root password, you can add the public ssh key for the host into
`~/.ssh/authorized_keys`. See [this tutorial](http://www.linuxproblem.org/art_9.html) for more
details.*

{% include Running-Alluxio-Locally/Alluxio-format-start.md %}

To verify that Alluxio is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder. You can
also run a sample program:

{% include Running-Alluxio-Locally/run-sample.md %}

For the first sample program, you should be able to see something similar to the following:

{% include Running-Alluxio-Locally/first-sample-output.md %}

And you can visit Alluxio web UI at **[http://localhost:19999](http://localhost:19999)** again.
Click `Browse File System` in the navigation bar and you should see the files written to Alluxio by
the above test.

To run a more comprehensive sanity check:

{% include Running-Alluxio-Locally/run-tests.md %}

You can stop Alluxio any time by running:

{% include Running-Alluxio-Locally/Alluxio-stop.md %}
