---
layout: global
title: Running Tensorflow on Alluxio-FUSE
nickname: Tensorflow
group: Data Applications
priority: 6
---

This guide describes how to run [Tensorflow](https://www.tensorflow.org/) on top of Alluxio POSIX API.

* Table of Contents
{:toc}

## Overview

Tensorflow enables developers to quickly and easily get started with deep learning. 
The [deep learning]({{ '/en/compute/Deep-Learning.html' | relativize_url }}) section illustrates the data challenges of deep learning 
and how Alluxio helps to solve those challenges. This tutorial aims to provide some hands-on examples and tips for running Tensorflow
on top of Alluxio POSIX API.

## Prerequisites

* Setup Java for Java 8 Update 60 or higher (8u60+), 64-bit.
* Alluxio has been set up and is running.
* [Tensorflow](https://www.tensorflow.org/install/pip) installed. 

## Setting up Alluxio POSIX API

In this section, we will follow the instructions in the
[POSIX API section]({{ '/en/api/POSIX-API.html' | relativize_url }}) to set up Alluxio POSIX API
and allow Tensorflow applications to access the data through Alluxio POSIX API.

Run the following command to install FUSE on Linux:

```
$ yum install fuse fuse-devel
```

On MacOS, download the [osxfuse dmg file](https://github.com/osxfuse/osxfuse/releases/download/osxfuse-3.8.3/osxfuse-3.8.3.dmg) instead and follow the installation instructions.

Create a folder at the root in Alluxio: 

```bash
./bin/alluxio fs mkdir /training-data
```

Create a folder `/mnt/fuse`, change its owner to the current user (`$(whoami)`), 
and change its permissions to allow read and write:

```bash
sudo mkdir -p /mnt/fuse
sudo chown $(whoami) /mnt/fuse
chmod 755 /mnt/fuse
```

Run the Alluxio-FUSE shell to mount Alluxio folder `training-data` to the local empty folder
just created:

```bash
./integration/fuse/bin/alluxio-fuse mount /mnt/fuse /training-data
```

The above CLI spawns a background user-space java process (`alluxio-fuse`) that mounts the Alluxio path specified at `/training-data` 
to the local file system on the specified mount point `/mnt/alluxio`. Please refer to [POSIX API documentation]({{ '/en/api/POSIX-API.html' | relativize_url }}) 
for details about how to mount Alluxio-FUSE and set up fuse related options. 

Check the status of the FUSE process with:

```bash
./integration/fuse/bin/alluxio-fuse stat
```

The mounted folder `/mnt/fuse` is ready for the deep learning frameworks to use, which would treat the Alluxio
storage as a local folder. This folder will be used for the Tensorflow training in the next
section.

## Examples: Image Recognition

### Preparing training data

If the training data is already in a remote data storage, you can mount it as a folder under 
the Alluxio `/training-data` directory. Those data will be visible to the applications running on
local `/mnt/fuse/`.

Suppose the ImageNet data is stored in a S3 bucket `s3://alluxio-tensorflow-imagenet/`.
Run the following command to mount this S3 bucket to Alluxio path `/training-data/imagenet`:

```bash
./bin/alluxio fs mount /training-data/imagenet/ s3://alluxio-tensorflow-imagenet/ --option aws.accessKeyID=<ACCESS_KEY_ID> --option aws.secretKey=<SECRET_KEY>
```

Note this command takes options to pass the S3 credentials of the bucket. These credentials
are associated with the mounting point so that the future accesses will not require credentials.

If the data is not in a remote data storage, you can copy it to Alluxio namespace:

```bash
wget http://download.tensorflow.org/models/image/imagenet/inception-2015-12-05.tgz
./bin/alluxio fs mkdir /training-data/imagenet 
./bin/alluxio fs copyFromLocal inception-2015-12-05.tgz /training-data/imagenet 
```

Suppose the ImageNet data is stored in an S3 bucket `s3://alluxio-tensorflow-imagenet/`, 
the following three commands will show the exact same data after the two mount processes:

```
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

Download the [image recognition script](https://raw.githubusercontent.com/tensorflow/models/master/tutorials/image/imagenet/classify_image.py)
and run it with the local folder which holds the training data.

```bash
curl -o classify_image.py -L https://raw.githubusercontent.com/tensorflow/models/master/tutorials/image/imagenet/classify_image.py
python classify_image.py --model_dir /mnt/fuse/imagenet/
```

This will use the input data in `/mnt/fuse/imagenet/inception-2015-12-05.tgz` to recognize images,  write some intermediate data to `/mnt/fuse/imagenet` 
and if everything worked successfully you will see in your command prompt:

```
giant panda, panda, panda bear, coon bear, Ailuropoda melanoleuca (score = 0.89107)
indri, indris, Indri indri, Indri brevicaudatus (score = 0.00779)
lesser panda, red panda, panda, bear cat, cat bear, Ailurus fulgens (score = 0.00296)
custard apple (score = 0.00147)
earthstar (score = 0.00117)
```

## Examples: Tensorflow benchmark

Mount the ImageNet data stored in an S3 bucket into path `/training-data/imagenet`,
assuming the data is at the S3 path `s3://alluxio-tensorflow-imagenet/`.

```bash
./bin/alluxio fs mount /training-data/imagenet/ \
s3://alluxio-tensorflow-imagenet/ \
--option aws.accessKeyID=<ACCESS_KEY_ID> \
--option aws.secretKey=<SECRET_KEY>
```

To access the training data in S3 via Alluxio, with the Alluxio POSIX API,
we can pass the path `/mnt/fuse/imagenet` to the parameter `data_dir` of the benchmark
script [tf_cnn_benchmarsk.py](https://github.com/tensorflow/benchmarks/blob/master/scripts/tf_cnn_benchmarks/tf_cnn_benchmarks.py).

## Tips

### Write Tensorflow applications with data location parameter

When running Tensorflow on top of HDFS, S3, and other under storages, it requires to 
configure Tensorflow and modify tensorflow applications. Through Alluxio POSIX API,
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
When concurrently writing or reading big files, Alluxio POSIX API can provide significantly better
performance when running on Alluxio worker node. When the worker node has big enough memory space to 
host all the training data, Alluxio POSIX API on worker node can provide nearly 2X performance improvement.

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
