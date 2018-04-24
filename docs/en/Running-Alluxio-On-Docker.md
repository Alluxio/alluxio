---
layout: global
title: Running Alluxio on Docker
nickname: Alluxio on Docker
group: Deploying Alluxio
priority: 3
---

* Table of Contents
{:toc}

Alluxio can be run in a Docker container. This guide demonstrates how to run Alluxio
in Docker using the Dockerfile that comes in the Alluxio Github repository.

# Basic Tutorial

This tutorial walks through a basic dockerized Alluxio setup on a single node.

## Prerequisites

A Linux machine. For the purposes of this guide, we will use a fresh EC2 machine running
Amazon Linux. The machine size doesn't need to be large; we will use t2.small. When
setting up the network security for the instance, allow traffic on ports 19998-19999 and
29998-30000.

## Launch a standalone cluster

All steps below should be executed from your Linux machine.

### Install Docker

```bash
$ sudo yum install -y docker git
$ sudo service docker start
$ # Add the current user to the docker group
$ sudo usermod -a -G docker $(id -u -n)
$ # Log out and log back in again to pick up the group changes
$ exit
```

### Clone the Alluxio repo

```bash
$ git clone https://github.com/Alluxio/alluxio.git
```

### Build the Alluxio Docker image

```bash
$ cd alluxio/integration/docker
$ docker build -t alluxio .
```

### Set up under storage

Create an under storage folder on the host.
```bash
$ mkdir underStorage
```

### Set up ramdisk to enable short-circuit reads

From the host machine:

```bash
$ sudo mkdir /mnt/ramdisk
$ sudo mount -t ramfs -o size=1G ramfs /mnt/ramdisk
$ sudo chmod a+w /mnt/ramdisk
```

Restart Docker so that it is aware of the new mount point.

```bash
$ sudo service docker restart
```

### Run the Alluxio master

```bash
$ docker run -d --net=host \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio master
```
Details:
- `-v $PWD/underStorage:/underStorage`: Share the underStorage folder with the Docker container.
- `-e ALLUXIO_UNDERFS_ADDRESS=/underStorage`: Tell the worker to use /underStorage as the under file storage.

### Run the Alluxio worker

```bash
$ # This gets the public ip of the current EC2 instance
$ export INSTANCE_PUBLIC_IP=$(curl http://169.254.169.254/latest/meta-data/public-ipv4)
$ # Launch an Alluxio worker container and save the container ID for later
$ ALLUXIO_WORKER_CONTAINER_ID=$(docker run -d --net=host \
             -v /mnt/ramdisk:/opt/ramdisk \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP} \
             -e ALLUXIO_RAM_FOLDER=/opt/ramdisk \
             -e ALLUXIO_WORKER_MEMORY_SIZE=1GB \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio worker)
```
Details:
- `-v /mnt/ramdisk:/opt/ramdisk`: Share the host machine's ramdisk with the Docker container.
- `-v $PWD/underStorage:/underStorage`: Share the underStorage folder with the Docker container.
- `-e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP}`: Tell the worker how to contact the master.
- `-e ALLUXIO_RAM_FOLDER=/opt/ramdisk`: Tell the worker where to find the ramdisk.
- `-e ALLUXIO_WORKER_MEMORY_SIZE=1GB`: Tell the worker how much ramdisk space to use.
- `-e ALLUXIO_UNDERFS_ADDRESS=/underStorage`: Tell the worker to use /underStorage as the under file storage.

### Test the cluster

To test the cluster, first enter the worker container.

```bash
$ docker exec -it ${ALLUXIO_WORKER_CONTAINER_ID} /bin/sh
```

Now run Alluxio tests.
```bash
$ cd opt/alluxio
$ bin/alluxio runTests
```

# Building from a specific Alluxio distribution

To build an Alluxio Docker image from a local or remote Alluxio tarball, use `--build-arg`

Local tarball:
```bash
$ docker build -t alluxio --build-arg ALLUXIO_TARBALL=alluxio-snapshot.tar.gz .
```

Remote tarball:
```bash
$ docker build -t alluxio --build-arg ALLUXIO_TARBALL=http://downloads.alluxio.org/downloads/files/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz .
```

# Alluxio Configuration Properties

To set an Alluxio configuration property, convert it to an environment variable by uppercasing
and replacing periods with underscores. For example, `alluxio.master.hostname` converts to
`ALLUXIO_MASTER_HOSTNAME`. You can then set the environment variable for the image with
`-e PROPERTY=value`. Alluxio configuration values will be copied to `conf/alluxio-site.properties`
when the image starts.

# Memory tier: ramdisk vs Docker tmpfs

The tutorial used a ramdisk to enable short-circuit reads. Another option is to use the tmpfs that
comes with Docker containers. This makes setup easier and improves isolation, but comes at the cost
of not being able to perform memory-speed short-circuit reads from local clients. Local clients will
instead need to go over the network to get data from Alluxio workers.

## Using the Docker tmpfs

When `ALLUXIO_RAM_FOLDER` isn't specified, the Docker worker container will use the
tmpfs mounted at `/dev/shm`. To set the size of the worker memory to `1GB`, specify
`--shm-size 1G` at launch time and configure the Alluxio worker with `1GB` memory size.

```bash
$ docker run -d --net=host --shm-size=1G \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP} \
             -e ALLUXIO_WORKER_MEMORY_SIZE=1GB \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio worker
```

To prevent clients from attempting and failing short-circuit reads, the client's hostname must
be set to a value different from the worker's hostname. On the clients, configure `alluxio.user.hostname=dummy`.

# Short-circuit operations

In the tutorial, we set up a shared ramdisk between the host system and the worker container.
This enables clients to perform read and write operations directly against the ramdisk instead
of having to go through the worker process. A limitation of this approach is that it doesn't play
well with setting worker container memory limits. Even though the ramdisk is mounted on the host,
writes to it from the worker container will count towards the container's memory limit. To work
around this limitation, you can share a domain socket instead of the ramdisk itself. The downside
of using domain sockets is that they are computationally more expensive and suffer from lower
write throughput.

## Domain Socket

From the host machine, create a directory for the shared domain socket.
```bash
$ mkdir /tmp/domain
$ chmod a+w /tmp/domain
```

When starting workers and clients, run their docker containers with `-v /tmp/domain:/opt/domain`
to share the domain socket directory. Also set the site property
`alluxio.worker.data.server.domain.socket.address` in the worker by passing
`-e ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS=/opt/domain/d`
when launching the container.

```bash
$ # This gets the public ip of the current EC2 instance
$ export INSTANCE_PUBLIC_IP=$(curl http://169.254.169.254/latest/meta-data/public-ipv4)
$ ALLUXIO_WORKER_CONTAINER_ID=$(docker run -d --net=host --shm-size=1G \
             -v /tmp/domain:/opt/domain \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP} \
             -e ALLUXIO_WORKER_MEMORY_SIZE=1GB \
             -e ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS=/opt/domain/d \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio worker)
```

By default, short-circuit operations between the Alluxio client and worker are enabled if the client
hostname matches the worker hostname. This may not be true if the client is running as part of a container
with virtual networking. In such a scenario, when starting the workers, set the following properties to
use filesystem inspection instead: `ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS=/opt/domain`
and `ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID=true`. Short-circuit writes are then enabled if
the worker UUID is located on the client filesystem.

# FUSE

To use FUSE, you need to build a docker image with FUSE enabled:

```bash
docker build -f Dockerfile.fuse -t alluxio-fuse .
```

There are a couple extra arguments required to run the docker image with FUSE support, 
for example:

```bash
docker run -e ALLUXIO_MASTER_HOSTNAME=alluxio-master --cap-add SYS_ADMIN --device /dev/fuse  alluxio-fuse [master|worker|proxy]
```

Note: running FUSE in docker requires adding [SYS_ADMIN capability](http://man7.org/linux/man-pages/man7/capabilities.7.html)
 to the container. This removes isolation of the container and should be used with caution.

Importantly, in order for the application to access data from Alluxio storage mounted with FUSE, it must run in the same
 container as Alluxio. You can easily extend the docker image to include applications to run on top of Alluxio. For example, 
 to run TensorFlow with Alluxio inside a docker container, just edit Dockerfile.fuse and replace

```bash
FROM ubuntu:16.04
```

with

```bash
FROM tensorflow/tensorflow:1.3.0
```

You can then build the image with the same command for building image with FUSE support and run it. There is a pre-built docker image with TensorFlow at https://hub.docker.com/r/alluxio/alluxio-tensorflow/
