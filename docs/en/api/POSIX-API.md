---
layout: global
title: FUSE-based POSIX API
nickname: POSIX API
group: Client APIs
priority: 3
---

* Table of Contents
{:toc}

The Alluxio POSIX API is a feature that allows mounting an Alluxio File System as a standard file system
on most flavors of Unix.
By using this feature, standard tools (for example, `ls`, `cat` or `mkdir`) will have basic access
to the Alluxio namespace.
More importantly, with the POSIX API integration applications can interact with the Alluxio no
matter what language (C, C++, Python, Ruby, Perl, or Java) they are written in without any Alluxio
library integrations.

Note that Alluxio-FUSE is different from projects like [s3fs](https://s3fs.readthedocs.io/en/latest/),
[mountableHdfs](https://cwiki.apache.org/confluence/display/HADOOP2/MountableHDFS) which mount
specific storage services like S3 or HDFS to the local filesystem.
The Alluxio POSIX API is a generic solution for the many storage systems supported by Alluxio.
Data orchestration and caching features from Alluxio speed up I/O access to frequently used data.

<p align="center">
<img src="{{ '/img/stack-posix.png' | relativize_url }}" alt="Alluxio stack with its POSIX API"/>
</p>

The Alluxio POSIX API is based on the [Filesystem in Userspace](http://fuse.sourceforge.net/)
(FUSE) project.
Most basic file system operations are supported.
However, given the intrinsic characteristics of Alluxio, like its write-once/read-many-times file
data model, the mounted file system does not have full POSIX semantics and contains some
limitations.
Please read the [section of limitations](#assumptions-and-limitations) for details.

## Choose Fuse Implementation

The Alluxio POSIX API has two implementations for users to choose from:
* [Alluxio JNR-Fuse](#jnr-fuse)
Alluxio's default Fuse implementation that uses [jnr-fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.
JNR-Fuse targets for low concurrency scenarios and has some known limitations.
* [Alluxio JNI-Fuse](#jni-fuse) (Experimental)
Alluxio JNI-Fuse is an in-house FUSE implementation which targets for challenging AI scenarios.

To choose between the default JNR-Fuse and JNI-Fuse, here are some aspects to consider:

* Platforms: JNR-Fuse support both Linux and MacOS while JNI-Fuse can only be run in Linux environment.
* Ease of usage: JNR-Fuse is enabled by default while JNI-Fuse has more dependencies requirements.
* Training Jobs: JNI-Fuse introduces less overhead, has more concurrency improvements, and 
generally provides better performance in high concurrency machine learning jobs.
* Maintenance: JNI-Fuse is experimental but is the direction to go. Alluxio will focus more on developing JNI-Fuse.

## Requirements

{% accordion requirements %}
  {% collapsible JNR-Fuse %}
- JDK 1.8 or newer
- [libfuse](https://github.com/libfuse/libfuse) 2.9.3 or newer (2.8.3 has been
reported to also work - with some warnings) for Linux
- [osxfuse](https://osxfuse.github.io/) 3.7.1 or newer for MacOS
  {% endcollapsible %}
  {% collapsible JNI-Fuse %}
- [libfuse](https://github.com/libfuse/libfuse) 2.9.X.
Libfuse 3.X and MacOS are not supported.
- Add pre-compiled `libjnifuse.so` to `java.library.path`
Add the `libjnifuse.so` under `${ALLUXIO_HOME}/integration/docker` to your existing `java.library.path` (e.g. `/usr/lib`) 
or expose it by adding `ALLUXIO_FUSE_JAVA_OPTS+=" -Djava.library.path=${ALLUXIO_HOME}/integration/docker"` 
to `${ALLUXIO_HOME}/conf/alluxio-env.sh`. 
- Enabled JNI-Fuse
```
$ echo "alluxio.fuse.jnifuse.enabled=true" >> ${ALLUXIO_HOME}/conf/alluxio-site.properties
```
  {% endcollapsible %}
{% endaccordion %}  

## Usage

### Mount Alluxio-FUSE

After properly configuring and starting an Alluxio cluster; Run the following command on the node
where you want to create the mount point:

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse mount <mount_point> [<alluxio_path>]
```

This will spawn a background user-space java process (`alluxio-fuse`) that will mount the Alluxio
path specified at `<alluxio_path>` to the local file system on the specified `<mount_point>`.

For example, running the following commands from the `${ALLUXIO_HOME}` directory will mount the
Alluxio path `/people` to the folder `/mnt/people` on the local file system.

```console
$ ./bin/alluxio fs mkdir /people
$ sudo mkdir -p /mnt/people
$ sudo chown $(whoami) /mnt/people
$ chmod 755 /mnt/people
$ integration/fuse/bin/alluxio-fuse mount /mnt/people /people
```

When `<alluxio_path>` is not given, the value defaults to the root (`/`).
Note that the `<mount_point>` must be an existing and empty path in your local file system hierarchy
and that the user that runs the `integration/fuse/bin/alluxio-fuse` script must own the mount point
and have read and write permissions on it.
Multiple Alluxio FUSE mount points can be created in the same node.
All the `AlluxioFuse` processes share the same log output at `$ALLUXIO_HOME\logs\fuse.log`, which is
useful for troubleshooting when errors happen on operations under the filesystem.

### Unmount Alluxio-FUSE

To unmount a previously mounted Alluxio-FUSE file system, on the node where the file system is
mounted run:

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse unmount mount_point
```

This unmounts the file system at the mount point and stops the corresponding Alluxio-FUSE
process. For example,

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse unmount /mnt/people
Unmount fuse at /mnt/people (PID:97626).
```

### Check the Alluxio POSIX API mounting status

To list the mount points; on the node where the file system is mounted run:

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse stat
```

This outputs the `pid, mount_point, alluxio_path` of all the running Alluxio-FUSE processes.

For example, the output could be:

```
pid	mount_point	alluxio_path
80846	/mnt/people	/people
80847	/mnt/sales	/sales
```

## Advanced configuration

### Configure Alluxio fuse options

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

### Configure Alluxio client options

Alluxio POSIX API is based on the standard Java client API `alluxio-core-client-fs` to perform its
operations.
You might want to customize the behaviour of the Alluxio client used by Alluxio POSIX API the same
way you would for any other client application.

One possibility, for example, is to edit `${ALLUXIO_HOME}/conf/alluxio-site.properties` and set your
specific Alluxio client options.
Note that these changes should be done before the mounting steps.

JNR-Fuse targets low concurrency workloads, so the default client configuration
normally is enough. JNI-Fuse requires fine-tuning when running under multi-threads concurrency
AI training environment.
{% accordion clientOptions %}
  {% collapsible JNI-Fuse Alluxio client options %}  
- Fuse client side metadata cache configuration
Alluxio JNI-FUSE can cache file metadata locally to reduce the overhead of 
repeatedly requesting metadata of the same file from Alluxio Master.
```
alluxio.user.metadata.cache.enabled=true
alluxio.user.metadata.cache.max.size=1000000
alluxio.user.metadata.cache.expiration.time=1h
```
- `alluxio.user.client.cache.enabled=true`
If this is enabled, data will be cached on Alluxio client.
Recommend increasing direct byte buffer memory by adding 
`ALLUXIO_USER_JAVA_OPTS+=" -XX:MaxDirectMemorySize=<size>"` 
to `${ALLUXIO_HOME}/conf/alluxio-env.sh` when enabling this property.
- `alluxio.user.unsafe.direct.local.io.enabled=true`
This property helps reduce the block location validation with Alluxio Worker 
when doing short-circuit reads.
Note this optimization is only safe when the workload is read-only,  
and the worker has only one tier and one storage directory in this tier.
- User client pool size configuration
`alluxio.user.block.worker.client.pool.max` limits the number of block worker clients
for Alluxio JNI-Fuse to read data from remote worker or validate block locations.
Some AI training jobs don't release the block worker clients immediately and may stuck
in waiting for any available.
Enlarge this value based on the thread number and blocks number of the AI training job.
`alluxio.user.block.master.client.pool.size.max` limits the number of block master client
for Alluxio JNI-Fuse to get block information.
`alluxio.user.file.master.client.pool.size.max` limits the number of file master client
for Alluxio JNI-Fuse to get or update file metadata. 
- `alluxio.user.metrics.collection.enabled=true`
Enable the collection of fuse side metrics like short-circuit read/write information
to show on the Alluxio Web UI.
- `alluxio.user.update.file.accesstime.disabled=true` (Experimental)
If this is enabled, the clients doesn't update file access time which may cause issues for some applications.
- `alluxio.user.logging.threshold`
Logging a client RPC when it takes more time than the threshold.
  {% endcollapsible %}
{% endaccordion %}  

### Configure mount point options

You can use `-o [mount options]` to set mount options.
If you want to set multiple mount options, you can pass in comma separated mount options as the
value of `-o`.
The `-o [mount options]` must follow the `mount` command.

The available Linux mount options are listed [here](http://man7.org/linux/man-pages/man8/mount.fuse.8.html).
The mount options of MacOS with osxfuse are listed [here](https://github.com/osxfuse/osxfuse/wiki/Mount-options) .
Some mount options (e.g. `allow_other` and `allow_root`) need additional set-up
and the set up process may be different depending on the platform.

```console
$ ${ALLUXIO_HOME}integration/fuse/bin/alluxio-fuse mount \
  -o [comma separated mount options] mount_point [alluxio_path]
```

Some commonly used mount options are listed here:
{% accordion mountOptions %}
  {% collapsible JNR-Fuse %}
`direct_io` mount option is set by default in JNR-Fuse so that writes and reads bypass the kernel
page cache and go directly to Alluxio.

Different versions of `libfuse` and `osxfuse` support different mount options.
    
Example: `allow_other` and `allow_root`
By default, Alluxio-FUSE mount point can only be accessed by the user
mounting the Alluxio namespace to the local filesystem.

For Linux, add the following line to file `/etc/fuse.conf` to allow other users
or allow root to access the mounted folder:

```
user_allow_other
```

This option allow non-root users to specify the `allow_other` or `allow_root` mount options.

For MacOS, follow the [osxfuse allow_other instructions](https://github.com/osxfuse/osxfuse/wiki/Mount-options)
to allow other users to use the `allow_other` and `allow_root` mount options.

After setting up, pass the `allow_other` or `allow_root` mount options when mounting Alluxio-FUSE:

```console
# All users (including root) can access the files.
$ integration/fuse/bin/alluxio-fuse mount -o allow_other mount_point [alluxio_path]
# The user mounting the filesystem and root can access the files.
$ integration/fuse/bin/alluxio-fuse mount -o allow_root mount_point [alluxio_path]
```

Note that only one of the `allow_other` or `allow_root` could be set.
  {% endcollapsible %}
  {% collapsible JNI-Fuse %}
- `kernel_cache`
Unlike JNR-Fuse with `direct_io` enabled by default, JNI-Fuse can have different patterns based on specific use cases.
This should only be enabled on filesystems, where the file data is never changed externally (not through the mounted FUSE filesystem).
- `big_writes`
This option prevents fuse from splitting write buffers into 4K chunks, enabling big write buffers to be transferred from the
application in a single step.
- `attr_timeout=T`
The timeout in seconds for which file/directory attributes are cached. The default is 1 second.
Recommend set to a larger value in real training.
- `entry_timeout=T`
The timeout in seconds for which name lookups will be cached. The default is 1 second. 
Recommend set to a larger value in real training.
  {% endcollapsible %}
{% endaccordion %}  

## Assumptions and limitations

Currently, most basic file system operations are supported. However, due to Alluxio implicit
characteristics, please be aware that:

* Files can be written only once, only sequentially, and never be modified.
  That means overriding a file is not allowed, and an explicit combination of delete and then create
  is needed.
  For example, the `cp` command will fail when the destination file exists.
  `vi` and `vim` commands will succeed because the underlying system do create, delete, and rename
  operation combinations.
* Alluxio does not have hard-links or soft-links, so commands like `ln` are not supported.
  The hardlinks number is not displayed in `ll` output.
* The user and group are mapped to the Unix user and group only when Alluxio POSIX API is configured
  to use shell user group translation service, by setting
  `alluxio.fuse.user.group.translation.enabled` to `true`.
  Otherwise `chown` and `chgrp` are no-ops, and `ll` will return the user and group of the user who
  started the Alluxio-FUSE process.
  The translation service does not change the actual file permission when running `ll`.

## Performance considerations

Due to the conjunct use of FUSE and JNR/JNI, the performance of the mounted file system is expected to
be worse than what you would see by using the
[Alluxio Java client]({{ '/en/api/FS-API.html' | relativize_url }}#java-client) directly.

Most of the overheads come from the fact that there are several memory copies going on for each call
for `read` or `write` operations. FUSE caps the maximum granularity of writes to 128KB.
This could be probably improved by a large extent by leveraging the FUSE cache write-backs feature
introduced in the 3.15 Linux Kernel (supported by libfuse 3.x but not yet supported in jnr-fuse/jni-fuse).
