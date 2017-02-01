---
layout: global
title: Running Alluxio on Docker
nickname: Alluxio on Docker
group: Deploying Alluxio
priority: 3
---

Alluxio can be run in a Docker container. This guide demonstrates how to run Alluxio
in Docker using the Dockerfile that comes in the Alluxio github repository.

# Prerequisites

A linux machine. For the purposes of this guide, we will use a fresh EC2 machine running
Amazon Linux. The machine size doesn't need to be large; we will use t2.small.

# Launch a standalone cluster

All steps below should be executed from your linux machine.

## Install Docker

```bash
sudo yum install -y docker
sudo service docker start
sudo usermod -a -G docker $(id -u -n)
```

Finally, log out and log back in again to pick up the group changes

## Clone the Alluxio repo

```bash
git clone https://github.com/Alluxio/alluxio.git
```

## Build the Alluxio Docker image

```bash
cd alluxio/integration/docker
docker build -t alluxio .
```

By default, this will build an image for the latest released version of Alluxio. To build
from a local Alluxio tarball instead, you can use `--build-arg`
```bash
docker build -t alluxio --build-arg ALLUXIO_TARBALL=alluxio-snapshot.tar.gz .
```

## Run the Alluxio master

```bash
docker run -d --net=host alluxio master
```

## Run the Alluxio worker

We need to tell the worker where to find the master. Set the `ALLUXIO_MASTER_HOSTNAME`
environment variable to your machine's hostname when launching the worker Docker container.

```bash
docker run -d --net=host -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_HOSTNAME} alluxio worker
```

# Test the cluster

To test the cluster, first enter the worker container.
```bash
docker exec -it ${ALLUXIO_WORKER_CONTAINER_ID} /bin/sh
```

Now run Alluxio tests
```bash
cd opt/alluxio*
bin/alluxio runTests
```

# Configuration

## Alluxio Configuration Properties

To set an Alluxio configuration property, convert it to an environment variable by uppercasing
and replacing periods with underscores. For example, `alluxio.master.hostname` converts to
`ALLUXIO_MASTER_HOSTNAME`. You can then set the environment variable on the image with
`-e PROPERTY=value`. Alluxio configuration values will be copied to `conf/alluxio-site.properties`
when the image starts.

```bash
docker run -d --net=host -e ALLUXIO_MASTER_HOSTNAME=ec2-203-0-113-25.compute-1.amazonaws.com alluxio worker
```

# Setting worker memory size

The Docker worker container will use the tmpfs mounted at `/dev/shm` by default. To set the
size of the worker memory to `50GB`, you can specify `--shm-size 50G` and configure the Alluxio worker to
use `50GB`. The full command would look like

```bash
docker run -d --net=host --shm-size=50GB \
           -e ALLUXIO_MASTER_HOSTNAME=master \
           -e ALLUXIO_WORKER_MEMORY_SIZE=50GB \
           alluxio worker
```
