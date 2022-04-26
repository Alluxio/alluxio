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
The [deep learning]({{ '/en/compute/Deep-Learning.html' | relativize_url }}) section illustrates the data challenges of deep learning 
and how Alluxio helps to solve those challenges. 
This tutorial aims to provide some hands-on examples and tips for running Tensorflow
on top of Alluxio POSIX API.

## Prerequisites

* Setup Java for Java 8 Update 60 or higher (8u60+), 64-bit.
* Alluxio has been set up and is running.
* [Python3](https://www.python.org/downloads/) installed.
* [Numpy](https://numpy.org/install/) installed. This guide uses numpy **1.19.5**.
* [Tensorflow](https://www.tensorflow.org/install/pip) installed. This guide uses Tensorflow **2.6.2**.

## Setting up Alluxio POSIX API

In this section, we will follow the instructions in the
[POSIX API section]({{ '/en/api/POSIX-API.html' | relativize_url }}) to set up Alluxio POSIX API
and allow Tensorflow applications to access the data through Alluxio POSIX API.

Run the following command to install FUSE on Linux:

```shell
$ yum install fuse fuse-devel
```

On macOS, download the [osxfuse dmg file](https://github.com/osxfuse/osxfuse/releases/download/osxfuse-3.8.3/osxfuse-3.8.3.dmg) instead and follow the installation instructions.

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

Run the Alluxio-FUSE shell to mount Alluxio folder `training-data` to the local empty folder
just created:

```shell
$ ./integration/fuse/bin/alluxio-fuse mount /mnt/fuse /training-data
```

The above CLI spawns a background user-space java process (`AlluxioFuse`) that mounts the Alluxio path specified at `/training-data` 
to the local file system on the specified mount point `/mnt/fuse`.
Please refer to [POSIX API documentation]({{ '/en/api/POSIX-API.html' | relativize_url }}) 
for details about how to mount Alluxio-FUSE and set up fuse related options. 

Check the status of the FUSE process with:

```shell
$ ./integration/fuse/bin/alluxio-fuse stat
```

The mounted folder `/mnt/fuse` is ready for the deep learning frameworks to use, which would treat the Alluxio storage as a local folder. 
This folder will be used for the Tensorflow training in the next section.

## Example: Image Recognition

### Preparing training data

If the training data is already in a remote data storage, you can mount it as a folder under the Alluxio `/training-data` directory. 
This data will be visible to the applications running on local `/mnt/fuse/`.

Suppose the MNIST data is stored in a S3 bucket `s3://alluxio-tensorflow-mnist/`.
Run the following command to mount this S3 bucket to Alluxio path `/training-data/mnist`:

```shell
$ ./bin/alluxio fs mount /training-data/mnist/ s3://alluxio-tensorflow-mnist/ \
  --option s3a.accessKeyID=<ACCESS_KEY_ID> --option s3a.secretKey=<SECRET_KEY>
```

Note this command takes options to pass the S3 credentials of the bucket. 
These credentials are associated with the mounting point so that the future accesses will not require credentials.

If the data is not in a remote data storage, you can copy it to Alluxio namespace:

```shell
$ wget https://storage.googleapis.com/tensorflow/tf-keras-datasets/mnist.npz
$ ./bin/alluxio fs mkdir /training-data/mnist 
$ ./bin/alluxio fs copyFromLocal mnist.npz /training-data/mnist 
```

Suppose the MNIST data is stored in an S3 bucket `s3://alluxio-tensorflow-mnist/`, 
the following three commands will show the exact same data after the two mount processes:

```shell
$ aws s3 ls s3://alluxio-tensorflow-mnist/
2021-11-04 17:43:58    11490434 mnist.npz
$ bin/alluxio fs ls /training-data/mnist/
-rwx---rwx ec2-user       ec2-user              11490434       PERSISTED 11-04-2021 17:45:41:000   0% mnist.npz
$ ls -l /mnt/fuse/mnist/
total 0
-rwx---rwx 0 ec2-user ec2-user 11490434 Nov  4 17:45 mnist.npz
```

### Run image recognition test

Download the [image recognition script](https://github.com/ssz1997/AlluxioFuseTensorflowExample/blob/main/mnist_test.py)
and run it with the training data `/mnt/fuse/mnist.npz`.

```shell
$ curl -o mnist_test.py -L https://github.com/ssz1997/AlluxioFuseTensorflowExample/blob/main/mnist_test.py?raw=true
$ python3 mnist_test.py /mnt/fuse/mnist.npz
```

This will use the input data in `/mnt/fuse/mnist.npz` to recognize images, 
and if everything works you will see something like this in your command prompt:

```
Epoch 1, Loss: 0.1307114064693451, Accuracy: 96.0566635131836, Test Loss: 0.07885940372943878, Test Accuracy: 97.29000091552734
Epoch 2, Loss: 0.03961360827088356, Accuracy: 98.71500396728516, Test Loss: 0.06348009407520294, Test Accuracy: 97.87999725341797
Epoch 3, Loss: 0.0206863172352314, Accuracy: 99.33999633789062, Test Loss: 0.060054901987314224, Test Accuracy: 98.20999908447266
Epoch 4, Loss: 0.011528069153428078, Accuracy: 99.61166381835938, Test Loss: 0.05984818935394287, Test Accuracy: 98.3699951171875
Epoch 5, Loss: 0.008437666110694408, Accuracy: 99.71666717529297, Test Loss: 0.060016192495822906, Test Accuracy: 98.5199966430664
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
