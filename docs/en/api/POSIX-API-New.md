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
<img src="{{ '/img/posix-stack.png' | relativize_url }}" alt="Alluxio stack with its POSIX API"/>
</p>

The Alluxio POSIX API is based on the [Filesystem in Userspace](http://fuse.sourceforge.net/)
(FUSE) project.
Most basic file system operations are supported.
However, given the intrinsic characteristics of Alluxio, like its write-once/read-many-times file
data model, the mounted file system does not have full POSIX semantics and contains some
limitations.
Please read the [functionalities and limitations](#functionalities-and-limitations) for details.

## Quick Start Example

This example shows you how to mount the whole Alluxio cluster to a local folder and run operations against the folder.

### Prerequisites

The followings are the basic requirements running ALLUXIO POSIX API.
Installing Alluxio POSIX API using [Docker]({{ '/en/deploy/Running-Alluxio-On-Docker.html' | relativize_url}}#enable-posix-api-access)
and [Kubernetes]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url}}#posix-api)
can further simplify the setup.

- Install JDK 11, or newer
    - JDK 8 has been reported to have some bugs that may crash the FUSE applications, see [issue](https://github.com/Alluxio/alluxio/issues/15015) for more details.
- Install libfuse
    - On Linux, we support libfuse both version 2 and 3
        - To use with libfuse2, install [libfuse](https://github.com/libfuse/libfuse) 2.9.3 or newer (2.8.3 has been reported to also work with some warnings). For example on a Redhat, run `yum install fuse fuse-devel`
        - To use with libfuse3, install [libfuse](https://github.com/libfuse/libfuse) 3.2.6 or newer (We are currently testing against 3.2.6). For example on a Redhat, run `yum install fuse3 fuse3-devel`
        - See [Select which libfuse version to use](#select-libfuse-version) to learn more about the libfuse version used by alluxio
    - On MacOS, install [osxfuse](https://osxfuse.github.io/) 3.7.1 or newer. For example, run `brew install osxfuse`
- Have a running Alluxio cluster

### Mount Alluxio as a FUSE Mount Point

After properly configuring and starting an Alluxio cluster; Run the following command on the node
where you want to create the mount point:

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse mount \
  [<mount_point>] [<alluxio_path>]
```

This will spawn a background user-space java process (`AlluxioFuse`) that will mount the Alluxio
path specified at `<alluxio_path>` to the local file system on the specified `<mount_point>`.

For example, running the following commands from the `${ALLUXIO_HOME}` directory will mount the
Alluxio path `/people` to the folder `/mnt/people` on the local file system.

```console
# Create the alluxio folder to be mounted
$ ${ALLUXIO_HOME}/bin/alluxio fs mkdir /people
# Prepare the local folder to mount Alluxio to
$ sudo mkdir -p /mnt/people
$ sudo chown $(whoami) /mnt/people
$ chmod 755 /mnt/people
# Mount alluxio folder to local folder
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse mount /mnt/people /people
```

Note that the `<mount_point>` must be an existing and empty path in your local file system hierarchy
and that the user that runs the `integration/fuse/bin/alluxio-fuse` script must own the mount point
and have read and write permissions on it.

Multiple Alluxio FUSE mount points can be created in the same node.
All the `AlluxioFuse` processes share the same log output at `${ALLUXIO_HOME}/logs/fuse.log`, which is
useful for troubleshooting when errors happen on operations under the filesystem.

### Check the Alluxio POSIX API Mounting Status

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

### Unmount Alluxio from FUSE

To unmount a previously mounted Alluxio-FUSE file system, on the node where the file system is
mounted run:

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse unmount mount_point
```

For example,

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse unmount /mnt/people
Unmount fuse at /mnt/people (PID:97626).
```

## Functionalities and Limitations

Currently, most basic file system operations are supported. However, due to Alluxio implicit
characteristics, some operations are not fully supported.

<table class="table table-striped">
    <tr>
        <td>Category</td>
        <td>Supported Operations</td>
        <td>Not Supported Operations</td>
    </tr>
    <tr>
        <td>Metadata Write</td>
        <td>Create file, delete file, create directory, delete directory, rename, change owner, change group, change mode</td>
        <td>Symlink, link, FIFO special file type, sticky bit, change access/modification time (utimens), change special file attributes (chattr)</td>
    </tr>
    <tr>
        <td>Metadata Read</td>
        <td>Get file status, get directory status, list directory status</td>
        <td></td>
    </tr>
    <tr>
        <td>Data Write</td>
        <td>Sequential write</td>
        <td>Append write, random write, overwrite, truncate</td>
    </tr>
    <tr>
        <td>Data Read</td>
        <td>Sequential read, random read</td>
        <td></td>
    </tr>
    <tr>
        <td>Combinations</td>
        <td></td>
        <td>Rename when writing the source file, read and write on the same file</td>
    </tr>
</table>

Note that all file/dir permissions are checked against the user launching the AlluxioFuse process instead of the end user running the operations.

## Configuration

### General Training Configuration

The following configurations are validated in training production workloads to help improve the training performance or system efficiency.

`<ALLUXIO_HOME>/conf/alluxio-env.sh`:
```config
# Enable Java 11 + G1GC for all Alluxio processes including Alluxio master, worker and fuse processes.
# Different from analytics workloads, training workloads generally have higher concurrency and more files involved.
# Likely that much more RPCs are issues between processes which results in a higher memory consumption and more intense GC activities.
# Enabling Java 11 + G1GC has been proved to improve GC activities in training workloads.
ALLUXIO_MASTER_JAVA_OPTS="-Xmx128G -Xms128G -XX:+UseG1GC"
ALLUXIO_WORKER_JAVA_OPTS="-Xmx32G -Xms32G -XX:MaxDirectMemorySize=32G -XX:+UseG1GC"
ALLUXIO_FUSE_JAVA_OPTS="-Xmx16G -Xms16G -XX:MaxDirectMemorySize=16G -XX:+UseG1GC"
```

`<ALLUXIO_HOME>/conf/alluxio-site.properties`:
```config
# By default, a master RPC will be issued to Alluxio Master to update the file access time whenever a user accesses it.
# If disabled, the client doesn't update file access time which may improve the file access performance
alluxio.user.update.file.accesstime.disabled=true
# Most training workloads deploys Alluxio cluster and training cluster separately.
# Alluxio passive cache which helps cache a new copy of data in local worker is not needed in this case
alluxio.user.file.passive.cache.enabled=false
# no need to check replication level if written only once
alluxio.master.replication.check.interval=1hr
```

### Cache Configuration

The FUSE kernel and FUSE userspace process can cache both data and metadata. When an application reads a file using Alluxio POSIX client,
data can be served from FUSE kernel cache or AlluxioFuse process local disk cache if the file resides in cache from a previous read or write operation.
However, if the file has been modified on the Alluxio cluster by a different client or modified on the underlying storage system, the data cached may be stale.
The following illustration shows the layers of cache - FUSE kernel cache, FUSE userspace cache.

<p align="center">
<img src="{{ '/img/posix-cache.png' | relativize_url }}" alt="Alluxio stack with its POSIX API"/>
</p>

Kernel cache and userspace cache both provide caching capability, enable one of them based on your environment and needs.
- Kernel Cache (Recommended): kernel cache provides significantly better performance, scalability, and resource consumption compared to Userspace cache.
However, kernel cache is not controlled by Alluxio and may affect other applications on the same node.
For example, large kernel cache usage in Kubernetes environment may cause the pods on the same node be killed unexpectedly.
- Userspace Cache: userspace cache in contrast has relatively worse performance, scalability, and resource consumption
and requires pre-calculated and pre-allocated cache resources when launching the process. Despite the disadvantages,
users can have more fine-grain control on the cache (e.g. maximum cache size, eviction policy)
and the cache will not affect other applications in containerized environment unexpectedly.

#### Metadata Cache

Metadata can be cached on FUSE kernel cache and/or in Alluxio FUSE process userspace cache. When the same file is accessed from multiple clients,
file metadata modification by one client may not be seen by other clients. The metadata cached on the FUSE side (kernel or userspace) may be stale.
For example, the file or directory metadata such as size, or modification timestamp cached on Node A might be stale if the file is being modified concurrently by an application on Node B.

Metadata cache may significantly improve the read training performance especially when loading a large amount of small files repeatedly.
FUSE kernel issues extra metadata read operations (sometimes can be 3 - 7 times more) compared to [Alluxio Native Java client]({{ '/en/api/FS-API.html' | relativize_url }}#java-client)
when applications are doing metadata operations or even data operations.
Even a `1min` temporary metadata cache may double metadata read speed or small file data loading speed.

{% navtabs metadataCache %}
{% navtab Kernel Metadata Cache Configuration %}

Kernel metadata cache is defined by the following FUSE mount options:
and [entry_timeout](https://manpages.debian.org/testing/fuse/mount.fuse.8.en.html#entry_timeout=T).

- [attr_timeout](https://manpages.debian.org/testing/fuse/mount.fuse.8.en.html#attr_timeout=T): Specifies the timeout in seconds for which file/directory metadata are cached. The default is 1.0 second.
- [entry_timeout](https://manpages.debian.org/testing/fuse/mount.fuse.8.en.html#entry_timeout=T): Specifies the timeout in seconds for which directory listing results are cached. The default is 1.0 second.

Enable via Fuse mount command:
```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse mount -o attr_timeout=600,entry_timeout=600 /mnt/people /people
```

Enable via Alluxio configuration before mounting, edit the `${ALLUXIO_HOME}/conf/alluxio-site.properties`:
```config
lluxio.fuse.mount.options=entry_timeout=600,attr_timeout=600
```

If have enough memory resources, recommend set the timeout to a large value to cover the whole training period to avoid refresh cache.
If not, even a 1min to 10min kernel metadata cache may significantly improve the overall metadata read performance and/or data read performance.

Kernel metadata cache for a single file takes around 300 bytes (up to 1KB), 3GB (up to 10GB) for 10 million files.

Recommend to use kernel metadata cache when installing Fuse process in plain machine (not via containerized environment)
or when FUSE container has enough memory resources and thus will not be killed unexpectedly.

{% endnavtab %}
{% navtab Userspace Metadata Cache Configuration %}

Userspace metadata cache can be enabled via setting Alluxio client configuration in `${ALLUXIO_HOME}/conf/alluxio-site.properties`
before mounting:

```config
alluxio.user.metadata.cache.enabled=true
alluxio.user.metadata.cache.expiration.time=2h
alluxio.user.metadata.cache.max.size=2000000
```

The metadata is cached in the AlluxioFuse java process heap so make sure `cache.max.size * 2KB < AlluxioFuse process maximum memory allocated`.
For example, if AlluxioFuse is launched with `-Xmx=16GB` and metadata cache can use up to 10GB memory, then `cache.max.size` should be smaller than 5 million.

{% endnavtab %}
{% endnavtabs %}

#### Data Cache

