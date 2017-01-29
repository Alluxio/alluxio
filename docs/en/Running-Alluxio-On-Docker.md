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

A Linux box. For the purposes of this guide, we will use a fresh EC2 machine running
Amazon Linux. The machine size doesn't need to be large; we will use t2.small.

# Launch a standalone cluster

## Install Docker and Git

```bash
sudo yum install -y git docker
sudo service docker start
sudo usermod -a -G docker ec2-user
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
from a local Alluxio tarball, use `--build-arg`
```bash
docker build -t alluxio --build-arg ALLUXIO_TARBALL=alluxio-snapshot.tar.gz .
```

## Run the Alluxio master

```bash
docker run -d --net=host alluxio master
```

## Run the Alluxio worker

We need to tell the worker where to find the master. Set the ALLUXIO_MASTER_HOSTNAME
environment variable to your machine's hostname when launching the worker Docker container.

```bash
docker run -d -e ALLUXIO_MASTER_HOSTNAME=${INSTANCE_HOSTNAME} --net=host alluxio worker
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

Configuration may be passed to the Alluxio containers using the `-e` flag when launching the container.
Any environment variables beginning with `ALLUXIO_` will be saved to `conf/alluxio-site.properties` inside
the container.
