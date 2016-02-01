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

{% include Running-Tachyon-Locally/download-Tachyon-binary.md %}

Before executing Tachyon run scripts, requisite environment variables must be specified in
`conf/tachyon-env.sh`, which can be copied from the included template file:

{% include Running-Tachyon-Locally/copy-template.md %}

To run in standalone mode, make sure that:

* `TACHYON_UNDERFS_ADDRESS` in `conf/tachyon-env.sh` is set to a tmp directory in the local
filesystem (e.g., `export TACHYON_UNDERFS_ADDRESS=/tmp`).

* Remote login service is turned on so that `ssh localhost` can succeed.

Then, you can format Tachyon FileSystem and start it. *Note: since Tachyon needs to setup
[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt), starting a
local system requires users to input their root password for Linux based users. To avoid the need to
repeatedly input the root password, you can add the public ssh key for the host into 
`~/.ssh/authorized_keys`.*

{% include Running-Tachyon-Locally/Tachyon-format-start.md %}

To verify that Tachyon is running, you can visit
**[http://localhost:19999](http://localhost:19999)**, or see the log in the `logs` folder. You can
also run a sample program:

{% include Running-Tachyon-Locally/run-sample.md %}

For the first sample program, you should be able to see something similar to the following:

{% include Running-Tachyon-Locally/first-sample-output.md %}

And you can visit Tachyon web UI at **[http://localhost:19999](http://localhost:19999)** again.
Click `Browse File System` in the navigation bar and you should see the files written to Tachyon by
the above test.

To run a more comprehensive sanity check:

{% include Running-Tachyon-Locally/run-tests.md %}

You can stop Tachyon any time by running:

{% include Running-Tachyon-Locally/tachyon-stop.md %}
