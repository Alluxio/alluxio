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

## Choose POSIX API Implementation

The Alluxio POSIX API has two implementations:
* Alluxio JNR-Fuse:
Alluxio's first generation Fuse implementation that uses [JNR-Fuse](https://github.com/SerCeMan/jnr-fuse) for FUSE on Java.
JNR-Fuse targets for low concurrency scenarios and has some known limitations in performance.
* Alluxio JNI-Fuse:
Alluxio's default in-house implementation based on JNI (Java Native Interface) which targets more performance-sensitve applications (like model training workloads) and initiated by researchers from Nanjing University and engineers from Alibaba Inc.

Here is a guideline to choose between the JNR-Fuse and JNI-Fuse:

* Workloads: If your data access is highly concurrent (e.g., deep learning training), JNI-Fuse is better and more stable.
* Maintenance: JNI-Fuse is under active development (checkout our [developer meeting notes](https://github.com/Alluxio/alluxio/wiki/Alluxio-Community-Dev-Sync-Meeting-Notes)). Alluxio community will focus more on developing JNI-Fuse and deprecate Alluxio JNR-Fuse eventually.

JNI-Fuse is enabled by default for better performance. 
If JNR-Fuse is needed for legacy reasons, set `alluxio.fuse.jnifuse.enabled` to `false` in `${ALLUXIO_HOME}/conf/alluxio-site.properties`:

```
alluxio.fuse.jnifuse.enabled=false
```

## Choose Deployment Mode

There are two approaches to deploy Alluxio POSIX integration:

* Serving POSIX API by Standalone FUSE process:
Alluxio POSIX integration can be launched as a standalone process, independent from existing running Alluxio clusters.
Each process is essentially a long-running Alluxio client, serving a file system mount point that maps an Alluxio path to a local path.
This approach is flexible so that users can enable or disable POSIX integration on hosts regardless Alluxio servers are running locally.
However, the FUSE process needs to communicate with Alluxio service through network.

* Enabling FUSE on worker:
Alluxio POSIX integration can also be provided by a running Alluxio worker process.
This integration provides better performance because the FUSE service can communicate with the Alluxio worker without invoking RPCs,
which help improve the read/write throughput on local cache hit.

Here is a guideline to choose between them:

* Workloads: If your workload is estimated to have a good hit ratio on local cache, and there are a lot of read/writes of small files, embedded FUSE on the worker process can achieve higher performance with less resource overhead.
* Deployment: If you want to enable multiple local mount points on a single host, choose standalone process. Otherwise, you can reduce one process to deploy with FUSE on worker.

## Requirements

The followings are the basic requirements running Alluxio FUSE integration to support POSIX API in a standalone way.
Installing Alluxio using [Docker]({{ '/en/deploy/Running-Alluxio-On-Docker.html' | relativize_url}}#enable-posix-api-access)
and [Kubernetes]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url}}#posix-api)
can further simplify the setup.

- Install JDK 1.8 or newer
- Install libfuse
    - On Linux, we support libfuse both version 2 and 3
        - To use with libfuse2, install [libfuse](https://github.com/libfuse/libfuse) 2.9.3 or newer (2.8.3 has been reported to also work with some warnings). For example on a Redhat, run `yum install fuse fuse-devel`
        - To use with libfuse3, install [libfuse](https://github.com/libfuse/libfuse) 3.2.6 or newer (We are currently testing against 3.2.6). For example on a Redhat, run `yum install fuse3 fuse3-devel`
        - See [Select which libfuse version to use](#select-which-libfuse-version-to-use) to learn more about libfuse version used by alluxio
    - On MacOS, install [osxfuse](https://osxfuse.github.io/) 3.7.1 or newer. For example, run `brew install osxfuse`

## Basic Setup

The basic setup deploys the standalone process.
After reading the basic setup section, checkout fuse in worker setup [here](#fuse-on-worker-process) if it suits your needs.

### Mount Alluxio as a FUSE mount point

After properly configuring and starting an Alluxio cluster; Run the following command on the node
where you want to create the mount point:

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse mount \
  <mount_point> [<alluxio_path>]
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
All the `AlluxioFuse` processes share the same log output at `${ALLUXIO_HOME}/logs/fuse.log`, which is
useful for troubleshooting when errors happen on operations under the filesystem.

### Unmount Alluxio from FUSE

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

By default, the `unmount` operation will wait for 120 seconds for any in-progress read/write operations to finish. If read/write operations haven't finished after 120 seconds, the fuse process will be forcibly killed which may cause read/write operations to fail. You can add `-s` to avoid the fuse process being killed if there are remaining in-progress read/write operations after the timeout. For example,

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse unmount -s /mnt/people
```

### Check the Alluxio POSIX API mounting status

To list the mount points; on the node where the file system is mounted run:

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse stat
```

This outputs the `pid, mount_point, alluxio_path` of all the running Alluxio-FUSE processes.

For example, the output could be:

```
pid mount_point alluxio_path
80846 /mnt/people /people
80847 /mnt/sales  /sales
```

## Advanced Setup

### Select which libfuse version to use

Alluxio now supports both libfuse2 and libfuse3. libfuse2 is more stable and more tested in production. libfuse3 support is now experimental. However, libfuse3 is newer and actively developed, and alluxio is working on utilizing the new features provided by libfuse3.

If only one version of libfuse is installed, that version is used. In most distros, libfuse2 and libfuse3 can coexist. If both versions are installed, **libfuse2** will be used by default (for backward compatibility). 

To set the version explicitly, add the following configuration in `${ALLUXIO_HOME}/conf/alluxio-site.properties`. 

```
alluxio.fuse.jnifuse.libfuse.version=3
```

Valid values are `2` (use libfuse2 only), `3` (use libfuse3 only) or other integer value (load libfuse2 first, and if failed, load libfuse3). 

See `logs/fuse.out` for which version is used.

```
INFO  NativeLibraryLoader - Loaded libjnifuse with libfuse version 2(or 3).
```

### Fuse on worker process

Unlike standalone Fuse which you can mount at any time without Alluxio worker involves,
the embedded Fuse has the exact same life cycle as the worker process it embeds into.
When the worker starts, the Fuse is mounted based on worker configuration.
When the worker ends, the embedded Fuse is unmounted automatically.
If you want to modify your Fuse mount, change the configuration and restart the worker process.

Enable FUSE on worker by setting `alluxio.worker.fuse.enabled` to `true` in the `${ALLUXIO_HOME}/conf/alluxio-site.properties`:

```
alluxio.worker.fuse.enabled=true
```

By default, Fuse on worker will mount the Alluxio root path `/` to default local mount point `/mnt/alluxio-fuse` with no extra mount options.
You can change the alluxio path, mount point, and mount options through Alluxio configuration:

```
alluxio.worker.fuse.mount.alluxio.path=<alluxio_path>
alluxio.worker.fuse.mount.point=<mount_point>
alluxio.worker.fuse.mount.options=<list of mount options separated by comma>
```

For example, one can mount Alluxio path `/people` to local path `/mnt/people`
with `kernel_cache,entry_timeout=7200,attr_timeout=7200` mount options when starting the Alluxio worker process:

```
alluxio.worker.fuse.enabled=true
alluxio.worker.fuse.mount.alluxio.path=/people
alluxio.worker.fuse.mount.point=/mnt/people
alluxio.worker.fuse.mount.options=kernel_cache,entry_timeout=7200,attr_timeout=7200
```

Fuse on worker also uses `alluxio.fuse.jnifuse.libfuse.version` configuration to determine which libfuse version to use. 

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

### Configure mount point options

You can use `-o [mount options]` to set mount options.
If you want to set multiple mount options, you can pass in comma separated mount options as the
value of `-o`.
The `-o [mount options]` must follow the `mount` command.

Different versions of `libfuse` and `osxfuse` may support different mount options.
The available Linux mount options are listed [here](http://man7.org/linux/man-pages/man8/mount.fuse3.8.html).
The mount options of MacOS with osxfuse are listed [here](https://github.com/osxfuse/osxfuse/wiki/Mount-options) .
Some mount options (e.g. `allow_other` and `allow_root`) need additional set-up
and the set up process may be different depending on the platform.

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse mount \
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
        <td>auto_cache</td>
        <td></td>
        <td>This option is an alternative to `kernel_cache`. Unable to set in JNR-Fuse.</td>
        <td>`auto_cache` utilizes kernel system caching and improves read performance. Instead of unconditionally keeping cached data, the cached data is invalidated if the modification time or the size of the file has changed since it was last opened. See [libfuse documentation](https://libfuse.github.io/doxygen/structfuse__config.html#a9db154b1f75284dd4fccc0248be71f66) for more info. </td>
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
        <td>Stop Fuse from splitting I/O into small chunks and speed up write. [Not supported in libfuse3](https://github.com/libfuse/libfuse/blob/master/ChangeLog.rst#libfuse-300-2016-12-08). Will be ignored if libfuse3 is used.</td>
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
The Alluxio docker image [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}/)
enables this property by modifying the [libfuse source code](https://github.com/cheyang/libfuse/tree/fuse_2_9_5_customize_multi_threads_v2).

In alluxio docker image, the default value for `MAX_IDLE_THREADS` is 64. If you want to use another value in your container,
you could set it via environment variable at container start time:
```console
$ docker run -d --rm \
    ...
    --env MAX_IDLE_THREADS=128 \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} fuse
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

Only after this step that non-root users have the permisson to specify the `allow_other` or `allow_root` mount options.

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
{% endaccordion %}

## Assumptions and Limitations

Currently, most basic file system operations are supported. However, due to Alluxio implicit
characteristics, please be aware that:

* Files can be written only once, only sequentially, and never be modified.
  That means overriding a file is not allowed, and an explicit combination of delete and then create
  is needed.
  For example, the `cp` command would fail when the destination file exists.
  `vi` and `vim` commands will only succeed modifying files if the underlying operating system deletes 
  the original file first and then creates a new file with modified content beneath.
* Alluxio does not have hard-links or soft-links, so commands like `ln` are not supported.
  The hardlinks number is not displayed in `ll` output.
* The user and group are mapped to the Unix user and group only when Alluxio POSIX API is configured
  to use shell user group translation service, by setting
  `alluxio.fuse.user.group.translation.enabled` to `true`.
  Otherwise `chown` and `chgrp` are no-ops, and `ll` will return the user and group of the user who
  started the Alluxio-FUSE process.
  The translation service does not change the actual file permission when running `ll`.

## Performance Optimization

Due to the conjunct use of FUSE, the performance of the mounted file system is expected to be lower compared to using the [Alluxio Java client]({{ '/en/api/FS-API.html' | relativize_url }}#java-client) directly.

Most of the overheads come from the fact that there are several memory copies going on for each call
for `read` or `write` operations. FUSE caps the maximum granularity of writes to 128KB.
This could be probably improved by a large extent by leveraging the FUSE cache write-backs feature
introduced in the 3.15 Linux Kernel (supported by libfuse 3.x but not yet supported in jnr-fuse/jni-fuse).

The following client options are useful when running deep learning workloads against Alluxio JNI-Fuse  based on our experience.
If you find other options useful, please share with us via [Alluxio community slack channel](https://alluxio.io/slack)
or [pull request]({{ '/en/contributor/Contributor-Getting-Started.html' | relativize_url}}).
Note that these changes should be done before the mounting steps.

### Enable Metadata Caching

Alluxio Fuse process can cache file metadata locally to reduce the overhead of
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
        <td>Maximum number of paths with cached metadata. Only valid if alluxio.user.metadata.cache.enabled is set to true.</td>
    </tr>
    <tr>
        <td>alluxio.user.metadata.cache.expiration.time</td>
        <td>10min</td>
        <td>Metadata will expire and be evicted after being cached for this time period. Only valid if alluxio.user.metadata.cache.enabled is set to true.</td>
    </tr>
</table>

For example, a workload that repeatedly gets information of 1 million files and runs for 50 minutes can set the following configuration:
```
alluxio.user.metadata.cache.enabled=true
alluxio.user.metadata.cache.max.size=1000000
alluxio.user.metadata.cache.expiration.time=1h
```
The metadata size of 1 million files is usually between 25MB and 100MB.
Enable metadata cache may also introduce some overhead, but may not be as big as client data cache.

### Other Performance or Debugging Tips

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
        <td>(Experimental) By default, a master RPC will be issued to Alluxio Master to update the file access time whenever a user accesses it. If this is enabled, the client doesn't update file access time which may improve the file access performance but cause issues for some applications.</td>
    </tr>
    <tr>
        <td>alluxio.user.block.worker.client.pool.max</td>
        <td>1024</td>
        <td>Limits the number of block worker clients for Alluxio JNI-Fuse to read data from remote worker or validate block locations. Some deep training jobs don't release the block worker clients immediately and may stuck in waiting for any available.</td>
    </tr>
    <tr>
        <td>alluxio.user.block.master.client.pool.size.max</td>
        <td>1024</td>
        <td>Limits the number of block master client for Alluxio JNI-Fuse to get block information.</td>
    </tr>
    <tr>
        <td>alluxio.user.file.master.client.pool.size.max</td>
        <td>1024</td>
        <td>Limits the number of file master client or Alluxio JNI-Fuse to get or update file metadata. </td>
    </tr>
</table>

### Increase Direct Memory Size

When encountering the out of direct memory issue, add the following JVM opts to `${ALLUXIO_HOME}/conf/alluxio-env.sh` to increase the max amount of direct memory.

```bash
ALLUXIO_FUSE_JAVA_OPTS+=" -XX:MaxDirectMemorySize=8G"
```

## Troubleshooting

This section talks about how to troubleshoot issues related to Alluxio POSIX API.
Note that the errors or problems of Alluxio POSIX API may come from the underlying Alluxio system.
For general guideline in troubleshooting, please refer to [troubleshooting documentation]({{ '/en/operation/Troubleshooting.html' | relativize_url }})

### Input/output error

Unlike Alluxio CLI which may show more detailed error messages, user operations via Alluxio Fuse mount point will only receive error code on failures with the pre-defined error code message by FUSE.
For exmaple, once an error happens, it is common to see:

```console
$ ls /mnt/alluxio-fuse/try.txt
ls: /mnt/alluxio-fuse/try.txt: Input/output error
```

In this case, check Alluxio Fuse logs for the actual error message.
The logs are in `logs/fuse.log` (deployed via standalone fuse process) or `logs/worker.log` (deployed via fuse in worker process).
```
2021-08-30 12:07:52,489 ERROR AlluxioJniFuseFileSystem - Failed to getattr /:
alluxio.exception.status.UnavailableException: Failed to connect to master (localhost:19998) after 44 attempts.Please check if Alluxio master is currently running on "localhost:19998". Service="FileSystemMasterClient"
        at alluxio.AbstractClient.connect(AbstractClient.java:279)
```

### Check FUSE operations in Debug Log

Each I/O operation by users can be translated into a sequence of Fuse operations.
Sometimes the error comes from unexpected Fuse operation combinations.
In this case, enabling debug logging in FUSE operations helps understand the sequence and shows time elapsed of each Fuse operation.

For example, a typical flow to write a file seen by FUSE is an initial `Fuse.create` which creates a file,
followed by a sequence of `Fuse.write` to write data to that file,
and lastly a `Fuse.release` to close file to commit a file written to Alluxio file system.

To understand this sequence seen and executed by FUSE, 
one can modify `${ALLUXIO_HOME}/conf/log4j.properties` to customize logging levels and restart corresponding server processes.
For example, set `alluxio.fuse.AlluxioJniFuseFileSystem` to `DEBUG`
```
alluxio.fuse.AlluxioJniFuseFileSystem=DEBUG
```
Then you will see the detailed Fuse operation sequence shown in debug logs.

If Fuse is deployed in the worker process, one can modify server logging at runtime.
For example, you can update the log level of all classes in `alluxio.fuse` package in all workers to `DEBUG` with the following command:
```console
$ ./bin/alluxio logLevel --logName=alluxio.fuse --target=workers --level=DEBUG
```

For more information about logging, please check out [this page]({{ '/en/operation/Basic-Logging.html' | relativize_url }}).

### Fuse metrics

Depending on the Fuse deployment type, Fuse metrics can be exposed as worker metrics (Fuse on worker process) or client metrics (Standalone FUSE process).
Check out the [metrics introduction doc]({{ '/en/operation/Metrics-System.html' | relativize_url }}) for how to get Fuse metrics.

Fuse metrics include Fuse specific metrics and general client metrics.
Check out the [Fuse metrics list]({{ '/en/reference/Metrics-List.html' | relativize_url }}#fuse-metrics) about more details of
what metrics are recorded and how to use those metrics.

## Performance Tuning

The following diagram shows the stack when using Alluxio POSIX API:
![Fuse components]({{ '/img/fuse.png' | relativize_url }})

Essentially, Alluxio POSIX API is implemented as as FUSE integration which is simply a long-running Alluxio client.
In the following stack, the performance overhead can be introduced in one or more components among 
 
- Application
- Fuse library
- Alluxio related components

### Application Level

It is very helpful to understand the following questions with respect to how the applications interact with Alluxio POSIX API:

- How is the applications accessing Alluxio POSIX API? Is it mostly read or write or a mixed workload?
- Is the access heavy in data or metadata?
- Is the concurrency level sufficient to sustain high throughput?
- Is there any lock contention?

### Fuse Level

Fuse, especially the libfuse and FUSE kernel code, may also introduce performance overhead.

#### libfuse worker threads

The concurrency on Alluxio POSIX API is the joint effort of
- The concurrency of application operations interacting with Fuse kernel code and libfuse
- The concurrency of libfuse worker threads interacting with Alluxio POSIX API limited by `MAX_IDLE_THREADS` [libfuse configuration](#configure-mount-point-options).

Enlarge the `MAX_IDLE_THRAEDS` to make sure it's not the performance bottleneck. One can use `jstack` or `visualvm` to see how many libfuse threads exist
and whether the libfuse threads keep being created/destroyed.


### Alluxio Level

[Alluxio general performance tuning]({{ '/en/operation/Performance-Tuning.html' | relativize_url }}) provides
more information about how to investigate and tune the performance of Alluxio Java client and servers.

#### Clock time tracing

Tracing is a good method to understand which operation consumes most of the clock time.

From the `Fuse.<FUSE_OPERATION_NAME>` metrics documented in the [Fuse metrics doc]({{ '/en/reference/Metrics-List.html' | relativize_url }}#fuse-metrics),
we can know how long each operation consumes and which operation(s) dominate the time spent in Alluxio.
For example, if the application is metadata heavy, `Fuse.getattr` or `Fuse.readdir` may have much longer total duration compared to other operations.
If the application is data heavy, `Fuse.read` or `Fuse.write` may consume most of the clock time.
Fuse metrics help us to narrow down the performance investigation target.

If `Fuse.read` consumes most of the clock time, enables the Alluxio property `alluxio.user.block.read.metrics.enabled=true` and Alluxio metric `Client.BlockReadChunkRemote` will be recorded.
This metric shows the duration statistics of reading data from remote workers via gRPC.

If the application spends relatively long time in RPC calls, try enlarging the client pool sizes Alluxio properties based on the workload.
```
# How many concurrent gRPC threads allowed to communicate from client to worker for data operations
alluxio.user.block.worker.client.pool.max
# How many concurrent gRPC threads allowed to communicate from client to master for block metadata operations
alluxio.user.block.master.client.pool.size.max
# How many concurrent gRPC threads allowed to communicate from client to master for file metadata operations
alluxio.user.file.master.client.pool.size.max
# How many concurrent gRPC threads allowed to communicate from worker to master for block metadata operations
alluxio.worker.block.master.client.pool.size
```
If thread pool size is not the limitation, try enlarging the CPU/memory resources. GRPC threads consume CPU resources.

One can follow the [Alluxio opentelemetry doc](https://github.com/Alluxio/alluxio/blob/ea36bb385d24769e079248015c8e490b6e46e6ed/integration/metrics/README.md)
to trace the gRPC calls. If some gRPC calls take extremely long time and only a small amount of time is used to do actual work, there may be too many concurrent gRPC calls or high resource contention.
If a long time is spent in fulfilling the gRPC requests, we can jump to the server side to see where the slowness come from.

#### CPU/memory/lock tracing

[Async Profiler](https://github.com/jvm-profiling-tools/async-profiler) can trace the following kinds of events:
- CPU cycles
- Allocations in Java Heap
- Contented lock attempts, including both Java object monitors and ReentrantLocks

Install async profiler and run the following commands to get the information of target Alluxio process

```console
$ cd async-profiler && ./profiler.sh -e alloc -d 30 -f mem.svg `jps | grep AlluxioWorker | awk '{print $1}'`
$ cd async-profiler && ./profiler.sh -e cpu -d 30 -f cpu.svg `jps | grep AlluxiWorker | awk '{print $1}'`
$ cd async-profiler && ./profiler.sh -e lock -d 30 -f lock.txt `jps | grep AlluxioWorker | awk '{print $1}'`
```
- `-d` define the duration. Try to cover the whole POSIX API testing duration
- `-e` define the profiling target
- `-f` define the file name to dump the profile information to

## Fuse Shell Tool

The Alluxio JNI-Fuse client provides a useful shell tool to perform some internal operations, such as clearing the client metadata cache. 
If our Alluxio-Fuse mount point is `/mnt/alluxio-fuse`, the command patten of Fuse Shell is:
```console
$ ls -l /mnt/alluxio-fuse/.alluxiocli.[COMMAND].[SUBCOMMAND]
```
Among them, the `/.alluxiocli` is the identification string of Fuse Shell, `COMMAND` is the main command (such as `metadatacache`), and `SUBCOMMAND` is the subcommand (such as `drop, size, dropAll`).
Currently, Fuse Shell only supports `metadatacache` command to clear cache or get cache size, and we will expand more commands and interactive methods in the future.
To use the Fuse shell tool, `alluxio.fuse.special.command.enabled` needs to be set to true in `${ALLUXIO_HOME}/conf/alluxio-site.properties` before launching the Fuse applications:
```console
$ alluxio.fuse.special.command.enabled=true
```

### Metadatacache Command

Client-side metadata cache can be enabled by setting `alluxio.user.metadata.cache.enabled=true` to reduce the latency of metadata cache operations and improve FUSE performance in many workloads. 
For example, in a scenario that reads a large number of small files such as AI, enabling client metadata caching can relieve Alluxio Master's metadata pressure and improve read performance.
When the data in Alluxio is updated, the metadata cache of the client needs to be updated. Usually, you need to wait for the timeout configured by `alluxio.user.metadata.cache.expiration.time` to invalidate the metadata cache.
This means that there is a time window that the cached metadata is outdated. In this case, it is recommended to use the `metadatacache` command of Fuse Shell to manually clean up the client metadata cache. The format of `metadatacache` command is：
```console
$ ls -l /mnt/alluxio-fuse/.alluxiocli.metadatacache.[dropAll|drop|size]
```

#### Usage Example

- Clean up all metadata caches：
```console
$ ls -l /mnt/alluxio-fuse/.alluxiocli.metadatacache.dropAll
```
- Clear the cache of a path and all parent directory under the mount point：
```console
$ ls -l /mnt/alluxio-fuse/dir/dir1/.alluxiocli.metadatacache.drop
```
The above command will clear the metadata of `/mnt/alluxio-fuse/dir/dir1` and all parent directory metadata cache.
- Get the client metadata size
```console
$ ls -l /mnt/alluxio-fuse/.alluxiocli.metadatacache.size
```
You will get metadata cache size in file size field, as in the output below:
```
---------- 1 root root 13 Jan  1  1970 /mnt/alluxio-fuse/.alluxiocli.metadatacache.size
```
