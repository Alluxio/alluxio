---
layout: global
title: Running Tensorflow on Alluxio-FUSE
nickname: Tensorflow
group: Compute Integrations
priority: 6
---

This guide describes how to run [Tensorflow](https://www.tensorflow.org/) on top of Alluxio POSIX API.

* Table of Contents
  {:toc}

## Overview

Tensorflow enables developers to quickly and easily get started with deep learning.
This tutorial aims to provide some hands-on examples and tips for running Tensorflow
on top of Alluxio POSIX API.

## Prerequisites

* Setup Java for Java 8 Update 60 or higher (8u60+), 64-bit.
* Alluxio has been set up and is running.
* [Python3](https://www.python.org/downloads/) installed.
* [Numpy](https://numpy.org/install/) installed. This guide uses numpy **1.19.5**.
* [Tensorflow](https://www.tensorflow.org/install/pip) installed. This guide uses Tensorflow **v1.15**.

## Setting up Alluxio POSIX API

In this section, we will follow the instructions in the
[FUSE-SDK-Overview]({{ '/en/fuse-sdk/FUSE-SDK-Overview.html' | relativize_url }}) to set up Alluxio POSIX API
and allow Tensorflow applications to access the data through Alluxio POSIX API.

Run the following command to install FUSE on Linux:

```shell
$ yum install fuse fuse-devel
```

On macOS, download the [osxfuse dmg file](https://github.com/osxfuse/osxfuse/releases/download/osxfuse-3.8.3/osxfuse-3.8.3.dmg) instead and follow the installation instructions.

In this guide, we use /training-data as Alluxio-Fuse's root directory and /mnt/fuse as the mount point of local directory.

Create a folder at the root in Alluxio:

```shell
$ ./bin/alluxio fs mkdir /training-data
```

Create a folder `/mnt/fuse`, change its owner to the current user (`$(whoami)`),
and change its permissions to allow read and write:

```shell
$ sudo mkdir -p /mnt/fuse
$ sudo chown $(whoami) /mnt/fuse
$ chmod 755 /mnt/fuse
```

Configure `conf/alluxio-site.properties`:

```properties
alluxio.fuse.mount.alluxio.path=/training-data
alluxio.fuse.mount.point=/mnt/fuse
```

Alluxio POSIX API allows accessing data from under storage as local directories. 
This is enabled by using the mount command to mount a dataset from under storage to local mount point:

```shell
$ sudo yum install fuse3
$ alluxio-fuse mount <under_storage_dataset> <mount_point> -o option
```

Alluxio POSIX API allows accessing data from under storage as local directories.
This is enabled by using the `mount` command to mount a dataset from under storage to local mount point:
```shell
$ sudo yum install fuse3
$ alluxio-fuse mount <under_storage_dataset> <mount_point> -o option
```
- `under_storage_dataset`: The full under storage dataset address. e.g. `s3://bucket_name/path/to/dataset`, `hdfs://namenode_address:port/path/to/dataset`
- `mount_point`: The local mount point to mount the under storage dataset to.
  Note that the `<mount_point>` must be an existing and empty path in your local file system hierarchy.
  User that runs the `mount` command must own the mount point and have read and write permissions on it.
- `-o option`: All the `alluxio-fuse mount` options are provided using this format. Options include
    - Alluxio property key value pair in `-o alluxio_property_key=value` format
        - Under storage credentials and configuration. Detailed configuration can be found under the `Storage Integrations` tap of the left of the doc page.
    - Local cache configuration. Detailed usage can be found in the [local cache guide]({{ '/en/fuse-sdk/Local-Cache-Overview.html' | relative_url }})
    - Generic mount options. Detailed supported mount options information can be found in the [FUSE mount options section]({{ '/en/fuse-sdk/Advanced-Tuning.html#fuse-mount-options' | relative_url }})

After mounting, `alluxio-fuse` mount can be found locally
```shell
$ mount | grep "alluxio-fuse"
alluxio-fuse on mount_point type fuse.alluxio-fuse (rw,nosuid,nodev,relatime,user_id=1000,group_id=1000)
```

`AlluxioFuse` process will be launched
```shell
$ jps
34884 AlluxioFuse
```

All the fuse logs can be found at `logs/fuse.log` and all the fuse outputs can be found at `logs/fuse.out` which are
useful for troubleshooting when errors happen on operations under the filesystem.

## Example: Image Recognition

### Preparing training data

If the training data is already in a remote data storage, you can mount it as a folder under the Alluxio `/training-data` directory.
This data will be visible to the applications running on local `/mnt/fuse/`.

If the data is not in a remote data storage, you can copy it to Alluxio namespace:

```shell
$ wget http://download.tensorflow.org/models/image/imagenet/inception-2015-12-05.tgz
$ ./bin/alluxio fs mkdir /training-data/imagenet 
$ ./bin/alluxio fs copyFromLocal inception-2015-12-05.tgz /training-data/imagenet 
```

Suppose the ImageNet's data is stored in an S3 bucket `s3://alluxio-tensorflow-imagenet/`,
the following three commands will show the exact same data after the two mount processes:

```shell
aws s3 ls s3://alluxio-tensorflow-imagenet/
# 2019-02-07 03:51:15          0 
# 2019-02-07 03:56:09   88931400 inception-2015-12-05.tgz
bin/alluxio fs ls /training-data/imagenet/
# -rwx---rwx ec2-user       ec2-user              88931400       PERSISTED 02-07-2019 03:56:09:000   0% /training-data/imagenet/inception-2015-12-05.tgz
ls -l /mnt/fuse/imagenet/
# total 0
# -rwx---rwx 0 ec2-user ec2-user 88931400 Feb  7 03:56 inception-2015-12-05.tgz
```

### Run image recognition test

Download the image recognition script and run it with the training data.

```shell
$ curl -o classify_image.py -L https://raw.githubusercontent.com/tensorflow/models/v1.11/tutorials/image/imagenet/classify_image.py
$ python classify_image.py --model_dir /mnt/fuse/imagenet/
```

This will use the input data in `/mnt/fuse/imagenet/` to recognize images,
and if everything works you will see something like this in your command prompt:

```
giant panda, panda, panda bear, coon bear, Ailuropoda melanoleuca (score = 0.89107)
indri, indris, Indri indri, Indri brevicaudatus (score = 0.00779)
lesser panda, red panda, panda, bear cat, cat bear, Ailurus fulgens (score = 0.00296)
custard apple (score = 0.00147)
earthstar (score = 0.00117)
```

## Tips

### Write Tensorflow applications with data location parameter

Running Tensorflow on top of HDFS, S3, and other under storages could require different configurations, making it
difficult to manage and integrate Tensorflow applications with different under storages.
Through Alluxio POSIX API, users only need to mount under storages to Alluxio once and mount the parent folder of those
under storages that contain training data to the local filesystem.
After the initial mounting, the data becomes immediately available through the Alluxio FUSE mount point and can be
transparently accessed in Tensorflow applications.
If a Tensorflow application has the data location parameter set, we only need to pass the data location inside the FUSE mount
point to the Tensorflow application without modifying it.
This greatly simplifies the application development, which otherwise would require different integration setups and
credential configurations for each under storage.

### Co-locating Tensorflow with Alluxio worker

By co-locating Tensorflow applications with an Alluxio Worker, Alluxio caches the remote data locally for future access,
providing data locality.
Without Alluxio, slow remote storage may result in bottleneck on I/O and leave GPU resources underutilized.
When concurrently writing or reading big files, Alluxio POSIX API can provide significantly better performance when
running on an Alluxio Worker node.
Setting up a Worker node with memory space to host all the training data can allow the Alluxio POSIX API to provide
nearly 2X performance improvement.

### Configure Alluxio write type and read type

Many Tensorflow applications generate a lot of small intermediate files during their workflow.
Those intermediate files are only useful for a short time and do not need to be persisted to under storages.
If we directly link Tensorflow with remote storages, all files (regardless of the type - data files, intermediate files,
results, etc.) will be written to and persisted in the remote storage.
With Alluxio -- a cache layer between the Tensorflow applications and remote storage, users can reduce unneeded remote
persistent work and speed up the write/read time.

With `alluxio.user.file.writetype.default` set to `MUST_CACHE`, we can write to the top tier (usually it is the memory
tier) of Alluxio Worker storage.
With `alluxio.user.file.readtype.default` set to `CACHE_PROMOTE`, we can cache the read data in Alluxio for future access.
This will accelerate our Tensorflow workflow by writing to and reading from memory.
If the remote storages are cloud storages like S3, the advantages will be more obvious.