---
layout: global
title: Running Alluxio on Docker
nickname: Alluxio on Docker
group: Deploying Alluxio
priority: 3
---

Alluxio can be run in a Docker container. This guide demonstrates how to run Alluxio
in Docker using the Dockerfile that comes in the Alluxio github repository.

## Prerequisites

A linux machine. For the purposes of this guide, we will use a fresh EC2 machine running
Amazon Linux. The machine size doesn't need to be large; we will use t2.small.

## Launch a standalone cluster

All steps below should be executed from your linux machine.

### Install Docker

```bash
$ sudo yum install -y docker git
$ sudo service docker start
$ # Add the current user to the docker group
$ sudo usermod -a -G docker $(id -u -n)
```

Finally, log out and log back in again to pick up the group changes

### Clone the Alluxio repo

```bash
$ git clone https://github.com/Alluxio/alluxio.git
```

### Build the Alluxio Docker image

```bash
$ cd alluxio/integration/docker
$ docker build -t alluxio .
```

By default, this will build an image for the latest released version of Alluxio. To build
from a local Alluxio tarball or a different downloadable tarball, you can use `--build-arg`

```bash
$ # Build from a local tarball
$ docker build -t alluxio --build-arg ALLUXIO_TARBALL=alluxio-snapshot.tar.gz .
$ # Alternatively, build from a remote tarball
$ docker build -t alluxio --build-arg ALLUXIO_TARBALL=http://downloads.alluxio.org/downloads/files/1.4.0/alluxio-1.4.0-bin.tar.gz .
```

### Set up under storage

To keep this guide simple, we will use the local filesystem for under storage. In a real deployment
you would instead use something like HDFS or S3.

Create an under storage folder on the host
```bash
$ mkdir underStorage
```

When we launch the master and worker containers, we will mount this directory with `-v /underStorage:/underStorage`.

### Set up ramdisk to enable short-circuit reads

When the Alluxio client runs on the same host as an Alluxio worker, a shared ramdisk
can be set up so that short-circuit reads can be used to read data at memory speed
instead of network speed. Another option is to use the tmpfs built into Docker containers.
This option is discussed in more detail below. For the purpose of this tutorial, we will use
a shared ramdisk.

From the host machine:

```bash
$ sudo mkdir /mnt/ramdisk
$ sudo mount -t ramfs -o size=1G ramfs /mnt/ramdisk
$ sudo chmod a+w /mnt/ramdisk
```

After mounting the ramdisk, restart Docker so that it is aware of the new mount point.

```bash
$ sudo service docker restart
```

### Useful Docker run flags

We use these `docker run` flags when launching Alluxio master and worker containers

- `-d` detaches the container from the current shell session
- `--net=host` configures host networking so that the container shares the host's network stack
- `-v` mounts a directory on the host to a directory in the container
- `-e` defines environment variables to pass to the container

### Run the Alluxio master

```bash
$ docker run -d --net=host \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio master
```

### Run the Alluxio worker

We need to tell the worker where to find the master. Below, we set the `ALLUXIO_MASTER_HOSTNAME`
environment variable to the machine's hostname when launching the worker Docker container.
To enable short-circuit reads, we share the ramdisk with `-v /mnt/ramdisk:/opt/ramdisk`, and
specify its location and size to the worker. `-v /mnt/ramdisk:/opt/ramdisk` will mount the
`/mnt/ramdisk` path on the host machine to the `/opt/ramdisk` path in the worker container.
This way, the data written by the Alluxio worker can be directly accessed from outside the
container.

```bash
$ # This gets the public ip of the current EC2 instance
$ export INSTANCE_PUBLIC_IP=$(curl http://169.254.169.254/latest/meta-data/public-ipv4)
$ docker run -d --net=host \
             -v /mnt/ramdisk:/opt/ramdisk \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP} \
             -e ALLUXIO_RAM_FOLDER=/opt/ramdisk \
             -e ALLUXIO_WORKER_MEMORY_SIZE=1GB \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio worker
```

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

### Read/Write to the local worker 

There are two options to read/write to the local worker efficiently. The option that shares ramdisk
with the client requires the memory limit of the client container to be greater than the amount of
data it writes to the local ramdisk. The domain socket approach does not have this
limitation but it consumes more CPU on the client container and the worker container.

#### Share ramdisk with clients
Running the worker with `-v /mnt/ramdisk:/opt/ramdisk` shares the ramdisk between the worker 
(`/opt/ramdisk`) and host (`/mnt/ramdisk`). To make this ramdisk available to clients in 
other containers under `/opt/ramdisk` running on the same host, those containers should also be 
run with `-v /mnt/ramdisk:/opt/ramdisk`.

#### Share domain socket with clients

From the host machine
```bash
$ mkdir /tmp/domain
$ chmod a+w /tmp/domain
$ touch /tmp/domain/d
$ chmod a+w /tmp/domain/d
```
When starting worker and clients, run the docker container with `-v /tmp/domain:/opt/domain`
to share `/tmp/domain` directory on the host machine with the worker and clients under 
`/opt/domain`. And set the site property `alluxio.worker.data.server.domain.socket.address` 
in the worker by passing `-e ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS=/opt/domain/d` 
when running the container.

## Configuration

### Alluxio Configuration Properties

To set an Alluxio configuration property, convert it to an environment variable by uppercasing
and replacing periods with underscores. For example, `alluxio.master.hostname` converts to
`ALLUXIO_MASTER_HOSTNAME`. You can then set the environment variable on the image with
`-e PROPERTY=value`. Alluxio configuration values will be copied to `conf/alluxio-site.properties`
when the image starts.

```bash
$ docker run -d --net=host \
             -v $PWD/underStorage:/underStorage \
             -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_PUBLIC_IP} \
             -e ALLUXIO_UNDERFS_ADDRESS=/underStorage \
             alluxio worker
```

## Memory tier: ramfs vs Docker tmpfs

The tutorial used ramfs with short-circuit reads enabled. Another option is to use the tmpfs that
comes with Docker containers. This makes setup easier, but comes at the cost of not being able to
perform memory-speed short-circuit reads from local clients. Local clients will instead need to go
over the network to get data from Alluxio workers.

### Using the Docker tmpfs

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

To prevent clients from attempting and failing short-circuit reads, the client hostname must
be set to a value different from the worker's hostname. On the clients, configure `alluxio.user.hostname=dummy`.
