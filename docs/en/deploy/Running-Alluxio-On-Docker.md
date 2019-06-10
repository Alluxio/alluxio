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
zero to a running Alluxio cluster with a couple of `docker run` commands.
This document provides a tutorial for running Dockerized Alluxio on a single node.
We'll also discuss more advanced topics and how to troubleshoot.

* Table of Contents
{:toc}

## Prerequisites

- A machine with Docker installed.
- Ports 19998, 19999, 29998, 29999, and 30000 available

If you don't have access to a machine with Docker installed, you can
provision a t2.small EC2 machine (costs about $0.03/hour) to follow along with
the tutorial. When provisioning the instance, set the security group so that
port `19999` is open to your IP address. This will let you view the Alluxio web
UI in your browser.

To set up Docker after provisioning the instance, run

```bash
sudo yum install -y docker
sudo service docker start
# Add the current user to the docker group
sudo usermod -a -G docker $(id -u -n)
# Log out and log back in again to pick up the group changes
exit
```

## Prepare network and UFS volume

Create a network for connecting Alluxio containers, and create a volume for storing ufs data.

```bash
docker network create alluxio_nw
docker volume create ufs
```

## Launch Alluxio

The `--shm-size=1G` argument will allocate a `1G` tmpfs for the worker to store Alluxio data.

```bash
# Launch the Alluxio master
docker run -d \
           -p 19999:19999 \
           --net=alluxio_nw \
           --name=alluxio-master \
           -v ufs:/opt/alluxio/underFSStorage \
           alluxio/alluxio master
# Launch the Alluxio worker
docker run -d \
           --net=alluxio_nw \
           --name=alluxio-worker \
           --shm-size=1G \
           -v ufs:/opt/alluxio/underFSStorage \
           -e ALLUXIO_JAVA_OPTS="-Dalluxio.worker.memory.size=1G -Dalluxio.master.hostname=alluxio-master" \
           alluxio/alluxio worker
```

## Verify the Cluster

To verify that the services came up, check `docker ps`. You should see something like
```bash
docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                      NAMES
1fef7c714d25        alluxio/alluxio     "/entrypoint.sh work…"   39 seconds ago      Up 38 seconds                                  alluxio-worker
27f92f702ac2        alluxio/alluxio     "/entrypoint.sh mast…"   44 seconds ago      Up 43 seconds       0.0.0.0:19999->19999/tcp   alluxio-master
```

If you don't see the containers, run `docker logs` on their container ids to see what happened.
The container ids were printed by the `docker run` command, and can also be found in `docker ps -a`.

Visit `instance-hostname:19999` to view the Alluxio web UI. You should see one worker connected and providing
`1024MB` of space.

To run tests, enter the worker container

```bash
docker exec -it alluxio-worker /bin/bash
```

Run the tests

```bash
cd /opt/alluxio
./bin/alluxio runTests
```

Congratulations, you've deployed a basic Dockerized Alluxio cluster! Read on to learn more about how to manage the cluster and make is production-ready.

## Operations

### Set server configuration

Configuration changes require stopping the Alluxio Docker images, then re-launching
them with the new configuration.

To set an Alluxio configuration property, add it to the alluxio java options environment variable with

```
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property.name=value"
```

Multiple properties should be space-separated.

If a property value contains spaces, you must escape it using single quotes.

```
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property1=value1 -Dalluxio.property2='value2 with spaces'"
```

Alluxio environment variables will be copied to `conf/alluxio-env.sh`
when the image starts. If you aren't seeing a property take effect, make sure the property in
`conf/alluxio-env.sh` within the container is spelled correctly. You can check the
contents with

```bash
docker exec ${container_id} cat /opt/alluxio/conf/alluxio-env.sh
```

### Run in High-Availability Mode

A lone Alluxio master is a single point of failure. To guard against this, a production
cluster should run multiple Alluxio masters and use internal leader election or Zookeeper-based leader election.
One of the masters will be elected leader and serve client requests.
If it dies, one of the remaining masters will become leader and pick up where the previous master left off.

#### Internal leader election

Alluxio uses internal leader election by default.

Provide the master embedded journal addresses and set the hostname of the current master:

```bash
$ docker run -d \
             ...
             -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.embedded.journal.addresses=master-hostname-1:19200,master-hostname-2:19200,master-hostname-3:19200 -Dalluxio.master.hostname=master-hostname-1" \
             alluxio master
```

Set the master rpc addresses for all the workers so that they can query the master nodes find out the leader master.

```bash
$ docker run -d \
             ...
             -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998" \
             alluxio worker
```

#### Zookeeper-based leader election

To run in HA mode with Zookeeper, Alluxio needs a shared journal directory
that all masters have access to, usually either NFS or HDFS.

Point them to a shared journal and set their Zookeeper configuration.

```bash
docker run -d \
           ...
           -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.journal.type=UFS -Dalluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/alluxio_journal -Dalluxio.zookeeper.enabled=true -Dalluxio.zookeeper.address=zkhost1:2181,zkhost2:2181,zkhost3:2181" \
           alluxio master
```

Set the same Zookeeper configuration for workers so that they can query Zookeeper
to discover the current leader.

```bash
docker run -d \
           ...
           -e ALLUXIO_JAVA_OPTS="-Dalluxio.zookeeper.enabled=true -Dalluxio.zookeeper.address=zkhost1:2181,zkhost2:2181,zkhost3:2181" \
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
mkdir /tmp/domain
chmod a+w /tmp/domain
```

When starting workers and clients, run their docker containers with `-v /tmp/domain:/opt/domain`
to share the domain socket directory. Also set domain socket properties by passing
`alluxio.worker.data.server.domain.socket.address=/opt/domain` and
`alluxio.worker.data.server.domain.socket.as.uuid=true` when launching worker containers.

```bash
docker run -d \
           ...
           -v /tmp/domain:/opt/domain \
           -e ALLUXIO_JAVA_OPTS="-Dalluxio.worker.data.server.domain.socket.address=/opt/domain -Dalluxio.worker.data.server.domain.socket.as.uuid=true" \
           alluxio worker
```

### Relaunch Alluxio Servers

When relaunching Alluxio masters, use the `--no-format` flag to avoid re-formatting
the journal. The journal should only be formatted the first time the image is run.
Formatting the journal deletes all Alluxio metadata, and starts the cluster in
a fresh state.

### Enable POSIX API access

Using the [alluxio/alluxio-fuse](https://hub.docker.com/r/alluxio/alluxio-fuse/), you can enable
access to Alluxio using the POSIX API.

Launch the container with [SYS_ADMIN](http://man7.org/linux/man-pages/man7/capabilities.7.html)
capability. This runs the FUSE daemon on a client node that needs to access Alluxio using the POSIX
API with a mount accessible at `/alluxio-fuse`.

```bash
docker run -e \
           ...
           --cap-add SYS_ADMIN
           --device /dev/fuse
           alluxio-fuse fuse
```


## Troubleshooting

Alluxio server logs can be accessed by running `docker logs $container_id`.
Usually the logs will give a good indication of what is wrong. If they are not enough to diagnose
your issue, you can get help on the
[user mailing list](https://groups.google.com/forum/#!forum/alluxio-users).
