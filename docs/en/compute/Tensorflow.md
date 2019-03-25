---
layout: global
title: Running Tensorflow on Alluxio-FUSE
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
we illustrate the data challenges of deep learning and how Alluxio helps to solve
the challenges. In this tutorial, we will provide some hands-on examples and tips for running Tensorflow
on top of Alluxio-FUSE.

## Prerequisites

* Setup Java for Java 8 Update 60 or higher (8u60+), 64-bit.
* Alluxio has been set up and is running.

## Setting up Alluxio POSIX API

In this section, we will follow the instructions in the
[POSIX API section]({{ '/en/api/POSIX-API.html' | relativize_url }}) to set up Alluxio POSIX API
and allow Tensorflow applications to access the data through Alluxio POSIX API.

Create a folder at the root in Alluxio

```bash
./bin/alluxio fs mkdir /training-data
```

Start the Alluxio-FUSE process. Create a folder `/mnt/fuse`, change its
owner to the current user (`ec2-user` in this example), and modify its permissions to allow read and write.

```bash
sudo mkdir -p /mnt/fuse
sudo chown ec2-user:ec2-user /mnt/fuse
chmod 664 /mnt/fuse
```

Run the Alluxio-FUSE shell to mount Alluxio folder `training-data` to the local folder
`/mnt/fuse`.

```bash
./integration/fuse/bin/alluxio-fuse mount /mnt/fuse /training-data
```

Check the status of the FUSE process with:

```bash
./integration/fuse/bin/alluxio-fuse stat
```

The mounted folder `/mnt/fuse` is ready for the deep learning frameworks to use, which would treat the Alluxio
storage as a local folder. This folder will be used for the Tensorflow training in the next
section.

## Examples: Image Recognition

If the training data is already in a remote data storage, you can mount it as a folder under 
the Alluxio `/training-data` directory. Those data will be visible to the applications running on
local `/mnt/fuse/`.

For example, we suppose the ImageNet data is stored in an S3 bucket `s3a://alluxio-tensorflow-imagenet/` 
We can mount it into path `/training-data/imagenet` by running:

```bash
./bin/alluxio fs mount /training-data/imagenet/ s3a://alluxio-tensorflow-imagenet/ --option aws.accessKeyID=<ACCESS_KEY_ID> --option aws.secretKey=<SECRET_KEY>
```

Note this command takes options to pass the S3 credentials of the bucket. These credentials
are associated with the mounting point so that the future accesses will not require credentials.

If the data is not in a remote data storage, you can copy it to Alluxio namespace:

```bash
wget http://download.tensorflow.org/models/image/imagenet/inception-2015-12-05.tgz
./bin/alluxio fs mkdir /trainning-data/imagenet 
./bin/alluxio fs copyFromLocal inception-2015-12-05.tgz /trainning-data/imagenet 
```

Browse the data at the mounted folder; it should display the image net training data.

```bash
cd /mnt/fuse
ls
```

To run the image recognition test, we need to download the 
[image recognition script](https://github.com/tensorflow/models/tree/master/tutorials/image/imagenet)
and run it with our data directory.

```bash
python classify_image.py --model_dir /mnt/fuse/imagenet/
```

## Examples: Tensorflow benchmark

Mount the ImageNet data stored in an S3 bucket into path `/training-data/imagenet`,
assuming the data is at the S3 path `s3a://alluxio-tensorflow-imagenet/`.

```bash
./bin/alluxio fs mount /training-data/imagenet/ s3a://alluxio-tensorflow-imagenet/ --option aws.accessKeyID=<ACCESS_KEY_ID> --option aws.secretKey=<SECRET_KEY>
```

To access the training data in S3 via Alluxio, with the Alluxio FUSE,
we can pass the path `/mnt/fuse/imagenet` to the parameter `data_dir` of the benchmark
script [tf_cnn_benchmarsk.py](https://github.com/tensorflow/benchmarks/blob/master/scripts/tf_cnn_benchmarks/tf_cnn_benchmarks.py).

## Tips

### Write Tensorflow applications with data location parameter

When running Tensorflow on top of HDFS, S3, and other under storages, it requires to 
configure Tensorflow and modify tensorflow applications. Through Alluxio-FUSE,
users only need to mount their various under storages to Alluxio once and mount the 
parent folder of those under storages that containing training data to the local filesystem.
After mounting, data in various under storages become immediately available through Alluxio
fuse mount point and can be transparently accessed to Tensorflow applications.
If the Tensorflow application has the data location parameter, we only need to pass the 
data location inside fuse mount point to the Tensorflow application without modifying it.
This greatly simplifies the application development, which otherwise 
would need the integration of each particular storage
system as well as the configurations of the credentials.

### Co-locating Tensorflow with Alluxio worker

By co-locating Tensorflow applications with Alluxio worker, Alluxio caches the remote data
locally for the future access, providing data locality. Without Alluxio, slow remote
storage may result in bottleneck on I/O and leave GPU resources underutilized. 
When concurrently writing or reading big files, Alluxio-FUSE can provide significantly better
performance when running on Alluxio worker node. When the worker node has big enough memory space to 
host all the training data, Alluxio-FUSE on worker node can provide nearly 2X performance improvement.

### Configure Alluxio write type and read type

Many Tensorflow applications generate a lot of small intermediate files during their
workflow. Those intermediate files are only useful for a short time and are not needed to be 
persistent to under storages. If we directly link Tensorflow with remote storages, all the 
files no matter they are data, intermediate files, or results will write to the remote storage 
and become persistent. With Alluxio -- a cache layer between the Tensorflow applications and remote storages, 
users can reduce unneeded remote persistent works and speed up the write/read time.

With `alluxio.user.file.writetype.default` set to `MUST_CACHE`, We can write to the top tier (usually it is the memory tier) 
of Alluxio worker storage. With `alluxio.user.file.readtype.default` set to `CACHE_PROMOTE`, we can cache the read data in Alluxio 
for future access. This will accelerate our Tensorflow workflow by writing to and reading from memory. If the remote storages are cloud storages like S3, 
the advantages will be more obvious.
