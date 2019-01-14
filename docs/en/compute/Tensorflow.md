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

Tensorflow enables developers to quickly and easily get started with deep learning. In the deep learning
section, we already illustrate the data challenges of deep learning and how Alluxio helps solving 
the challenges. In this tutorial, we will provide an easy image recognition example.

## Prerequisites

* Setup Java for Java 8 Update 60 or higher (8u60+), 64-bit.
* Alluxio has been set up and is running.

### Setting up Alluxio-Fuse

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

Browse the data at the mounted folder; it should display the training data stored in the cloud.

```bash
$ cd /mnt/fuse
$ ls
```

This folder is ready for the deep learning frameworks to use, which would treat the Alluxio
storage as a local folder. This folder will be used for the Tensorflow training in the next
section.

## Examples: Image Recognition on Alluxio-Fuse

If the data is already in a remote data storage, you can mount it as a folder under 
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

To run the image recognition test, we need to download the 
[image recognition script](https://github.com/tensorflow/models/tree/master/tutorials/image/imagenet)
and run it with our data:

```bash
$ python classify_image.py --model_dir /mnt/fuse/imagenet/
```

### Examples: Tensorflow benchmark



After mounting the under storage once, data in various under storages becomes immediately
available through Alluxio and can be transparently accessed to the benchmark without any
modification to either Tensorflow or the benchmark scripts. This greatly simplifies the
application development, which otherwise would need the integration of each particular storage
system as well as the configurations of the credentials.

Alluxio also brings performance benefits.
The benchmark evaluates the throughput of the training model from the input training images in
units of images/sec. The training involves three stages of utilizing different resources:
 - Data reads (I/O): choose and read image files from source.
 - Image Processing (CPU): Decode image records into images, preprocess, and organize into
 mini-batches.
 - Modeling training (GPU): Calculate and update the parameters in the multiple convolutional
 layers

By co-locating Alluxio worker with the deep learning frameworks, Alluxio caches the remote data
locally for the future access, providing data locality. Without Alluxio, slow remote
storage may result in bottleneck on I/O and leave GPU resources underutilized. For
example, among the benchmark models, we found AlexNet has relatively simpler architecture and
therefore more likely to result in an I/O bottleneck when storage becomes slower. Alluxio
brings nearly 2X performance improvement on an EC2 p2.8xlarge machine.
