---
layout: global
title: Running Tensorflow on Alluxio-Fuse
nickname: Tensorflow
group: Data Applications
priority: 6
---

This guide describes how to run [Tensorflow](https://www.tensorflow.org/) on top of Alluxio-Fuse.

* Table of Contents
{:toc}

## Overview

Tensorflow enables developers to quickly and easily get started with deep learning. 
In the [deep learning]({{ '/en/compute/Deep-Learning.html' | relativize_url }}) section, 
we already illustrate the data challenges of deep learning and how Alluxio helps solving 
the challenges. In this tutorial, we will provide some hands on examples and tips for running Tensorflow
on top of Alluxio-Fuse.

## Prerequisites

* Setup Java for Java 8 Update 60 or higher (8u60+), 64-bit.
* Alluxio has been set up and is running.

## Setting up Alluxio-Fuse

In this section, we will follow the instructions in the
[FUSE section]({{ '/en/api/FUSE-API.html' | relativize_url }}) to setup FUSE 
and allow deep learning frameworks to access the data through FUSE.

Create a folder at the root in Alluxio

```bash
$ ./bin/alluxio fs mkdir /training-data
```

Note this command takes options to pass the S3 credentials of the bucket. These credentials
are associated with the mounting point so that the future accesses will not require credentials.

Start the Alluxio-FUSE process. Create a folder `/mnt/fuse`, change its
owner to the current user (`ec2-user` in this example), and modify its permissions to allow read and write.

```bash
$ sudo mkdir -p /mnt/fuse
$ sudo chown ec2-user:ec2-user /mnt/fuse
$ chmod 664 /mnt/fuse
```

Run the Alluxio-FUSE shell to mount Alluxio folder training-data to the local folder
/`mnt/fuse`.

```bash
$ ./integration/fuse/bin/alluxio-fuse mount /mnt/fuse /training-data
```

Check the status of the FUSE process with:

```bash
$ ./integration/fuse/bin/alluxio-fuse stat
```

The mounted folder `/mnt/fuse` is ready for the deep learning frameworks to use, which would treat the Alluxio
storage as a local folder. This folder will be used for the Tensorflow training in the next
section.

## Examples: Image Recognition on Alluxio-Fuse

If the trainning data is already in a remote data storage, you can mount it as a folder under 
the Alluxio `/trainning-data` directory. Those data will be visible to the applications running on
local `/mnt/fuse/`.

For example, we suppose the ImageNet data is stored in an S3 bucket `s3a://alluxio-tensorflow-imagenet/` 
We can mount it into path `/training-data/imagenet` by running:

```bash
$ ./bin/alluxio fs mount /training-data/imagenet/ s3a://alluxio-tensorflow-imagenet/ --option aws.accessKeyID=<ACCESS_KEY_ID> --option aws.secretKey=<SECRET_KEY>
```

If the data is not in a remote data storage, you can copy it to Alluxio namespace:

```bash
$ wget http://download.tensorflow.org/models/image/imagenet/inception-2015-12-05.tgz
$ ./bin/alluxio fs mkdir /trainning-data/imagenet 
$ ./bin/alluxio fs copyFromLocal inception-2015-12-05.tgz /trainning-data/imagenet 
```

Browse the data at the mounted folder; it should display the image net training data.

```bash
$ cd /mnt/fuse
$ ls
```

To run the image recognition test, we need to download the 
[image recognition script](https://github.com/tensorflow/models/tree/master/tutorials/image/imagenet)
and run it with our data directory.

```bash
$ python classify_image.py --model_dir /mnt/fuse/imagenet/
```

## Tips

### Write Tensorflow applications with data location parameter

When running tensorflow on top of HDFS, S3, and other under storages, it requires to 
configure Tensorflow and change tensorflow applications. Through Alluxio-Fuse,
users only need to mount their various under storages to Alluxio once, and mount the 
parent folder of those under storages that containing training data to local filesystem.
After mounting, data in various under storages become immediately available through Alluxio
fuse mount point and can be transparently accessed to Tensorflow applications.
If the tensorflow application has the data location parameter, we only need to pass the 
data location inside fuse mount point to the tensorflow application without modifying it.
This greatly simplifies the application development, which otherwise 
would need the integration of each particular storage
system as well as the configurations of the credentials.

### Co-locating Tensorflow with Alluxio worker

By co-locating Tensorflow applications with Alluxio worker, Alluxio caches the remote data
locally for the future access, providing data locality. Without Alluxio, slow remote
storage may result in bottleneck on I/O and leave GPU resources underutilized. 
When concurrently writing or reading big files, Alluxio-Fuse can provide significantly better
performance when running on Alluxio worker node. When the worker node has big enough memory space to 
host all the training data, Alluxio-Fuse on worker node can provide nearly 2X performance improvement.

### Configure Alluxio write type and read type

Many Tensorflow applications generates a lot of small intermediate files during their
workflow. Those intermediate files are only useful for a short time and are not needed to be 
persistent to under storages. If we directly link Tensorflow with remote storages, all the 
files no matter it is data, intermediate files, or results will write to the storage and become 
persistent. It is a waste of resources and the write/read time will be relatively slow.
But with Alluxio, we are adding a cache layer between the Tensorflow applications and remote storages.

The `alluxio.user.file.writetype.default` is by default set to `MUST_CACHE` 
and `alluxio.user.file.readtype.default` is `CACHE_PROMOTE`. 
We can read the data from remote storages and cache them for future usage
and write to the top tier of Alluxio worker storage. This will accelerate our Tensorflow workflow
by writing to and reading from memory.

If the remote storage is s3, the advantages will be more obvious.
