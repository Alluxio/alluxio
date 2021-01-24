---
layout: global
title: FUSE-based POSIX API
nickname: POSIX API
group: Client APIs
priority: 3
---

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

* Table of Contents
{:toc}

## Choose POSIX API Implementation

The Alluxio POSIX API has two implementations for users to choose from:
* Alluxio JNR-Fuse
Alluxio's default Fuse implementation that uses [JNR-Fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.
JNR-Fuse targets for low concurrency scenarios and has some known limitations.
* Alluxio JNI-Fuse (Experimental)
Alluxio JNI-Fuse is an in-house FUSE implementation based on JNI (Java Native Interface) which targets for challenging AI scenarios.
JNI-Fuse is experimental but is the direction to go. Alluxio community is continuously solving the compatibility issues and improve stability and performance of JNI-Fuse.

To choose between the default JNR-Fuse and JNI-Fuse, here are some aspects to consider:

* Platforms: JNR-Fuse support both Linux and MacOS while JNI-Fuse can only be run in Linux environment.
* Ease of usage: JNR-Fuse is enabled by default while JNI-Fuse has more dependencies.
* Workload: JNR-Fuse supports both read and write workloads. JNI-Fuse supports read only during experimental stage.
Alluxio JNI-Fuse generally provides better performance in high concurrency deep learning workloads.
* Maintenance: JNI-Fuse is experimental but is the direction to go. Alluxio will focus more on developing JNI-Fuse.

## Requirements

The followings are the basic requirements of running Alluxio POSIX API. 
[Docker]({{ '/en/deploy/Running-Alluxio-On-Docker.html' | relativize_url}}#enable-posix-api-access)
and [Kubernetes]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url}}#posix-api)
can help solve the setup pain points.

{% navtabs requirements %}
{% navtab JNR-Fuse %}
- JDK 1.8 or newer
- [libfuse](https://github.com/libfuse/libfuse) 2.9.3 or newer (2.8.3 has been
reported to also work - with some warnings) for Linux
- [osxfuse](https://osxfuse.github.io/) 3.7.1 or newer for MacOS
{% endnavtab %}
{% navtab JNI-Fuse %}
- JDK 1.8 or newer
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
{% endnavtab %}
{% endnavtabs %}

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

JNR-Fuse targets low concurrency workloads, so the default client configuration normally is enough. 
JNI-Fuse requires fine-tuning when running under highly concurrent deep learning workloads.

The following client options are found useful when running deep learning workloads against Alluxio JNI-Fuse.
If you find other options useful, please share with us via [Alluxio community slack channel]((https://slackin.alluxio.io/)) 
or [pull request]({{ '/en/contributor/Contributor-Getting-Started.html' | relativize_url}}).

{% accordion clientOptions %}
  {% collapsible Alluxio client side data cache %}  
With JNI-Fuse, multiple threads are allowed to visit the same Alluxio mount point concurrently.
When `kernel_cache` option is enabled, kernel can prefetch data to fulfill future read requests.
Alluxio has the assumption that most reads are sequential and has a relatively expensive `seek()` call
for access file at desired position. With multi-threading and `kernel_cache`, Alluxio JNI-Fuse may 
issue much more `seek()` requests than expected and thus significantly decrease read performance. 
Client side data cache mechanism is introduced to solve the excessive `seek()` issue.

Client-side data cache will introduce extra resource consumption and is suggested to be configured under heavy I/O scenarios.

The following configuration are related to client side data cache:
<table class="table table-striped">
    <tr>
        <td>Configuration</td>
        <td>Default Value</td>
        <td>Description</td>
    </tr>
    <tr>
        <td>alluxio.user.client.cache.enabled</td>
        <td>false</td>
        <td>If this is enabled, data will be cached on Alluxio client.</td>
    </tr>
    <tr>
        <td>alluxio.user.client.cache.store.type</td>
        <td>LOCAL</td>
        <td>The type of page store to use for client-side cache. Can be either `LOCAL` or `ROCKS`. The `LOCAL` page store stores all pages in a directory, the `ROCKS` page store utilizes rocksDB to persist the data.</td>
    </tr>
    <tr>
        <td>alluxio.user.client.cache.dir</td>
        <td>/tmp/alluxio_cache</td>
        <td>The directory where client-side cache is stored. Recommended to provide the ramfs path to speed up cache read/write performance.</td>
    </tr>
    <tr>
        <td>alluxio.user.client.cache.page.size</td>
        <td>1MB</td>
        <td>Size of each page in client-side cache. Users can try 2MB, 4MB and 8MB first in performance tunning. Note that, if using memory to cache and the page size is too big, JVM may GC frequently. If the value is too small, the cache hit ratio may be low.</td>
    </tr>
    <tr>
        <td>alluxio.user.client.cache.size</td>
        <td>512MB</td>
        <td>The maximum size of the client-side cache.</td>
    </tr>
</table>

An example configuration of deep learning tasks is adding the following configuration to `${ALLUXIO_HOME}/conf/alluxio-site.properties`:
```
alluxio.user.client.cache.enabled=true
alluxio.user.client.cache.store.type=LOCAL
alluxio.user.client.cache.dir=/mnt/ramdisk
alluxio.user.client.cache.page.size=2MB
alluxio.user.client.cache.size=2000MB
```
If the cache size is big, users may need to tune the JVM as well by adding the following java opts to `${ALLUXIO_HOME}/conf/alluxio-env.sh`:
```
ALLUXIO_FUSE_JAVA_OPTS+=" -Xmx10G -Xms10G -XX:MaxDirectMemorySize=8g"
```
Users are recommended to monitor the cache hit ratio (via `${ALLUXIO_HOME}/logs/fuse.log`) and JVM GC status to adjust the page size and cache size.
  {% endcollapsible %}
  {% collapsible Alluxio client side metadata cache %}  
Alluxio JNI-FUSE can cache file metadata locally to reduce the overhead of 
repeatedly requesting metadata of the same file from Alluxio Master.
Enable when the workload repeatedly getting information of numerous files.

<table class="table table-striped">
    <tr>
        <td>Configuration</td>
        <td>Default Value</td>
        <td>Description</td>
    </tr>
    <tr>
        <td>alluxio.user.metadata.cache.enabled</td>
        <td>false</td>
        <td>If this is enabled, metadata of paths will be cached. The cached metadata will be evicted when it expires after alluxio.user.metadata.cache.expiration.time or the cache size is over the limit of alluxio.user.metadata.cache.max.size.</td>
    </tr>
    <tr>
        <td>alluxio.user.metadata.cache.max.size</td>
        <td>100000</td>
        <td>Maximum number of paths with cached metadata. Only valid if the filesystem is alluxio.client.file.MetadataCachingBaseFileSystem.</td>
    </tr>
    <tr>
        <td>alluxio.user.metadata.cache.expiration.time</td>
        <td>10min</td>
        <td>Metadata will expire and be evicted after being cached for this time period. Only valid if the filesystem is alluxio.client.file.MetadataCachingBaseFileSystem.</td>
    </tr>
</table>

For example, a workload that repeatedly gets information of 1 million files and runs for 50 minutes can set the following configuration:
```
alluxio.user.metadata.cache.enabled=true
alluxio.user.metadata.cache.max.size=1000000
alluxio.user.metadata.cache.expiration.time=1h
```
The metadata size of 1 million files usually is around 25MB to 100MB.
Enable metadata cache may also introduce some overhead, but may not be as big as client data cache.

  {% endcollapsible %}
  {% collapsible Other client options %}  
The following client options may affect the training performance or provides more training information.

<table class="table table-striped">
    <tr>
        <td>Configuration</td>
        <td>Default Value</td>
        <td>Description</td>
    </tr>
    <tr>
        <td>alluxio.user.metrics.collection.enabled</td>
        <td>false</td>
        <td>Enable the collection of fuse client side metrics like short-circuit read/write information to show on the Alluxio Web UI.</td>
    </tr>
    <tr>
        <td>alluxio.user.logging.threshold</td>
        <td>10s</td>
        <td>Logging a client RPC when it takes more time than the threshold.</td>
    </tr>
    <tr>
        <td>alluxio.user.unsafe.direct.local.io.enabled</td>
        <td>false</td>
        <td>(Experimental) If this is enabled, clients will read from local worker directly without invoking extra RPCs to worker to require locations. Note this optimization is only safe when the workload is read only and the worker has only one tier and one storage directory in this tier.</td>
    </tr>
    <tr>
        <td>alluxio.user.update.file.accesstime.disabled</td>
        <td>false</td>
        <td>(Experimental) If this is enabled, the clients doesn't update file access time which may cause issues for some applications.</td>
    </tr>
    <tr>
        <td>alluxio.user.block.worker.client.pool.max</td>
        <td>false</td>
        <td>Limits the number of block worker clients for Alluxio JNI-Fuse to read data from remote worker or validate block locations. Some deep training jobs don't release the block worker clients immediately and may stuck in waiting for any available.</td>
    </tr>
    <tr>
        <td>alluxio.user.block.master.client.pool.size.max</td>
        <td>false</td>
        <td>Limits the number of block master client for Alluxio JNI-Fuse to get block information.</td>
    </tr>
    <tr>
        <td>alluxio.user.file.master.client.pool.size.max</td>
        <td>false</td>
        <td>Limits the number of file master client or Alluxio JNI-Fuse to get or update file metadata. </td>
    </tr>
</table>
  {% endcollapsible %}
{% endaccordion %}  

### Configure mount point options

You can use `-o [mount options]` to set mount options.
If you want to set multiple mount options, you can pass in comma separated mount options as the
value of `-o`.
The `-o [mount options]` must follow the `mount` command.

Different versions of `libfuse` and `osxfuse` may support different mount options.
The available Linux mount options are listed [here](http://man7.org/linux/man-pages/man8/mount.fuse.8.html).
The mount options of MacOS with osxfuse are listed [here](https://github.com/osxfuse/osxfuse/wiki/Mount-options) .
Some mount options (e.g. `allow_other` and `allow_root`) need additional set-up
and the set up process may be different depending on the platform.

```console
$ ${ALLUXIO_HOME}integration/fuse/bin/alluxio-fuse mount \
  -o [comma separated mount options] mount_point [alluxio_path]
```

{% accordion mount %}
  {% collapsible Tuning mount options %} 

<table class="table table-striped">
    <tr>
        <td>Mount option</td>
        <td>Default value</td>
        <td>Tuning suggestion</td>
        <td>Description</td>
    </tr>
    <tr>
        <td>direct_io</td>
        <td>set by default in JNR-Fuse</td>
        <td>don't set in JNI-Fuse</td>
        <td>When `direct_io` is enabled, kernel will not cache data and read-ahead. `direct_io` is enabled by default in JNR-Fuse but is recommended not to be set in JNI-Fuse cause it may have stability issue under high I/O load.</td>
    </tr>
    <tr>
        <td>kernel_cache</td>
        <td></td>
        <td>Unable to set in JNR-Fuse, recommend to set in JNI-Fuse based on workloads</td>
        <td>`kernel_cache` utilizes kernel system caching and improves read performance. This should only be enabled on filesystems, where the file data is never changed externally (not through the mounted FUSE filesystem).</td>
    </tr>
    <tr>
        <td>attr_timeout=N</td>
        <td>1.0</td>
        <td>7200</td>
        <td>The timeout in seconds for which file/directory attributes are cached. The default is 1 second. Recommend set to a larger value to reduce the time to retrieve file metadata operations from Alluxio master and improve performance.</td>
    </tr>
    <tr>
        <td>big_writes</td>
        <td></td>
        <td>Set</td>
        <td>Stop Fuse from splitting I/O into small chunks and speed up write.</td>
    </tr>
    <tr>
        <td>entry_timeout=N</td>
        <td>1.0</td>
        <td>7200</td>
        <td>The timeout in seconds for which name lookups will be cached. The default is 1 second. 
            Recommend set to a larger value to reduce the file metadata operations in Alluxio-Fuse and improve performance.</td>
    </tr>
    <tr>
        <td>`max_read=N`</td>
        <td>131072</td>
        <td>Use default value</td>
        <td>Define the maximum size of data can be read in a single Fuse request. The default is infinite. Note that the size of read requests is limited anyway to 32 pages (which is 128kbyte on i386).</td>
    </tr>
</table>

A special mount option is the `max_idle_threads=N` which defines the maximum number of idle fuse daemon threads allowed.
If the value is too small, FUSE may frequently create and destroy threads which will introduce extra performance overhead. 
Note that, libfuse introduce this mount option in 3.2 while JNI-Fuse supports 2.9.X during experimental stage.
The Alluxio Fuse docker image [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}-fuse](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}-fuse/) 
enables this property by modifying the [libfuse source code](https://github.com/cheyang/libfuse/tree/fuse_2_9_5_customize_multi_threads_v2).

If you are using alluxio fuse docker image, set the `MAX_IDLE_THREADS` via environment variable:
```console
$ docker run --rm \
    ...
    --env MAX_IDLE_THREADS=64 \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}-fuse fuse
```
  {% endcollapsible %}
{% endaccordion %}  

{% accordion example %}
  {% collapsible Example: `allow_other` and `allow_root` %}  
By default, Alluxio-FUSE mount point can only be accessed by the user
mounting the Alluxio namespace to the local filesystem.

For Linux, add the following line to file `/etc/fuse.conf` to allow other users
or allow root to access the mounted folder:

```
user_allow_other
```

This option allow non-root users to specify the `allow_other` or `allow_root` mount options.

For MacOS (Only supported by JNR-Fuse), follow the [osxfuse allow_other instructions](https://github.com/osxfuse/osxfuse/wiki/Mount-options)
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
