---
layout: global
title: Deploy Alluxio on Docker
nickname: Docker
group: Deploying Alluxio
priority: 3
---

Docker can be used to simplify the deployment and management of Alluxio servers.
Using the [alluxio/alluxio](https://hub.docker.com/r/alluxio/alluxio/) Docker
image available on Dockerhub, you can go from
zero to a running Alluxio cluster in just a couple of `docker run` commands.
This document provides a tutorial for running Dockerized Alluxio on a single node.
We'll also discuss more advanced topics, and how to troubleshoot.

* Table of Contents
{:toc}

## Prerequisites

- A Linux machine with Docker installed.
- Ports 19998, 19999, 29998, 29999, and 30000 are available

If you don't have access to a Linux machine with Docker installed, you can
provision a t2.small EC2 machine (costs about $0.03/hour) to follow along with
the tutorial. When provisioning the instance, set the security group so that
port `19999` is open to your IP address. This will let you view the Alluxio web
UI in your browser.

To set up Docker after provisioning the instance, run

```bash
$ sudo yum install -y docker
$ sudo service docker start
$ # Add the current user to the docker group
$ sudo usermod -a -G docker $(id -u -n)
$ # Log out and log back in again to pick up the group changes
$ exit
```

## Launch Alluxio

These commands use the host machine's `/mnt/data` directory as the under storage for Alluxio.
The `--shm-size=1G` argument will allocate a `1G` tmpfs for the worker to store Alluxio data.

```bash
# Launch the Alluxio master
$ docker run -d --net=host \
    -v /mnt/data:/opt/alluxio/underFSStorage \
    alluxio/alluxio master
# Launch the Alluxio worker
$ docker run -d --net=host \
    --shm-size=1G -e ALLUXIO_WORKER_MEMORY_SIZE=1G \
    -v /mnt/data:/opt/alluxio/underFSStorage \
    -e ALLUXIO_MASTER_HOSTNAME=localhost \
    alluxio/alluxio worker \
```

## Verify the Cluster

To verify that the services came up, check `docker ps`. You should see something like
```bash
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS               NAMES
ef2f3b5be1a3        alluxio:1.8.0       "/entrypoint.sh work…"   6 days ago          Up 6 days                               dazzling_lichterman
8e3c31ed62cc        alluxio:1.8.0       "/entrypoint.sh mast…"   6 days ago          Up 6 days                               eloquent_clarke
```

If you don't see the containers, run `docker logs` on their container ids to see what happened.
The container ids were printed by the `docker run` command, and can also be found in `docker ps -a`.

Next, visit `instance_hostname:19999` to view the Alluxio web UI. You should see one worker connected and providing
`1024MB` of space.

To run tests, first enter the worker container

```bash
$ docker exec -it ${worker_container_id} /bin/bash
```

Then run the tests

```bash
$ cd /opt/alluxio
$ bin/alluxio runTests
```

Congratulations, you've deployed a basic Dockerized Alluxio cluster! Read on to learn more about how to manage the cluster and make is production-ready.

## Operations

### Set server configuration

Configuration changes require stopping the Alluxio Docker images, then re-launching
them with the new configuration.

To set an Alluxio configuration property, convert it to an environment variable by uppercasing
and replacing periods with underscores. For example, `alluxio.master.hostname` converts to
`ALLUXIO_MASTER_HOSTNAME`. You can then set the environment variable for the image with
`-e PROPERTY=value`. Alluxio configuration values will be copied to `conf/alluxio-site.properties`
when the image starts. If you aren't seeing a property take effect, make sure the property in
`conf/alluxio-site.properties` within the container is spelled correctly. You can check the
contents with

```bash
$ docker exec ${container_id} cat /opt/alluxio/conf/alluxio-site.properties
```

### Run in High-Availability Mode

A lone Alluxio master is a single point of failure. To guard against this, a production
cluster should run multiple Alluxio masters and use Zookeeper for leader election. One
of the masters will be elected leader and serve client requests. If it dies, one of the
remaining masters will become leader and pick up where the previous master left off.

With multiple masters, Alluxio needs a shared journal directory that all masters have
access to, usually either NFS or HDFS.

To run in HA mode, launch multiple Alluxio masters, point them to a shared journal,
and set their Zookeeper configuration.

```bash
$ docker run -d --net=host \
             ...
             -e ALLUXIO_MASTER_JOURNAL_FOLDER=hdfs://[namenodeserver]:[namenodeport]/alluxio_journal
             -e ALLUXIO_ZOOKEEPER_ENABLED=true -e ALLUXIO_ZOOKEEPER_ADDRESS=zkhost1:2181,zkhost2:2181,zkhost3:2181 \
             alluxio master
```

Set the same Zookeeper configuration for workers so that they can query Zookeeper
to discover the current leader.

```bash
$ docker run -d --net=host \
             ...
             -e ALLUXIO_ZOOKEEPER_ENABLED=true -e ALLUXIO_ZOOKEEPER_ADDRESS=zkhost1:2181,zkhost2:2181,zkhost3:2181 \
             alluxio worker
```

### Enable short-circuit reads and writes

If your compute applications will run on the same nodes as your Alluxio workers,
you can improve performance by enabling short-circuit reads
and writes. This allows applications to read from and write to their
local Alluxio worker without going over the loopback network. Instead, they will
read and write using [domain sockets](https://en.wikipedia.org/wiki/Unix_domain_socket).

On worker host machines, create a directory for the shared domain socket.
```bash
$ mkdir /tmp/domain
$ chmod a+w /tmp/domain
```

When starting workers and clients, run their docker containers with `-v /tmp/domain:/opt/domain`
to share the domain socket directory. Also set domain socket properties by passing
`-e ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS=/opt/domain` and
`-e ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID=true` when launching worker containers.

```bash
$ docker run -d --net=host \
             ...
             -v /tmp/domain:/opt/domain \
             -e ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS=/opt/domain \
             -e ALLUXIO_WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID=true \
             alluxio worker
```

### Relaunch Alluxio Servers

When relaunching Alluxio masters, use the `--no-format` flag to avoid re-formatting
the journal. The journal should only be formatted the first time the image is run.
Formatting the journal deletes all Alluxio metadata, and starts the cluster in
a fresh state.

## Troubleshooting

Alluxio server logs can be accessed by running `docker logs $container_id`.
Usually the logs will give a good indication of what is wrong. If they are not enough to diagnose
your issue, you can get help on the
[user mailing list](https://groups.google.com/forum/#!forum/alluxio-users).
