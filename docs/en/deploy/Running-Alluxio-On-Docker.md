---
layout: global
title: Deploy Alluxio on Docker
nickname: Docker
group: Install Alluxio
priority: 4
---

Docker can be used to simplify the deployment and management of Alluxio servers.
Using the [alluxio/alluxio](https://hub.docker.com/r/alluxio/alluxio/) Docker
image available on Dockerhub, you can go from
zero to a running Alluxio cluster with a couple of `docker run` commands.
This document provides a tutorial for running Dockerized Alluxio on a single node
with local disk as the under storage.
We'll also discuss more advanced topics and how to troubleshoot.

* Table of Contents
{:toc}

## Prerequisites

- A machine with Docker installed.
- Ports 19998, 19999, 29998, 29999, and 30000 available

If you don't have access to a machine with Docker installed, you can
provision a small AWS EC2 instance (e.g. t2.small)
to follow along with the tutorial. When provisioning the instance, set the security group
so that the following ports are open to  your IP address and the CIDR range of the
Alluxio clients (e.g. remote Spark clusters):

+ 19998 for the CIDR range of your Alluxio servers and clients: Allow the clients and workers to communicate
with Alluxio Master RPC processes.
+ 19999 for the IP address of your browser:  Allow you to access the Alluxio master web UI.
+ 29999 for the CIDR range of your Alluxio and clients: Allow the clients to communicate
with Alluxio Worker RPC processes.
+ 30000 for the IP address of your browser:  Allow you to access the Alluxio worker web UI.

To set up Docker after provisioning the instance, which will be referred as the Docker Host, run

```console
$ sudo yum install -y docker
$ sudo service docker start
# Add the current user to the docker group
$ sudo usermod -a -G docker $(id -u -n)
# Log out and log back in again to pick up the group changes
$ exit
```

## Prepare Docker Volume to Persist Data

By default all files created inside a container are stored on a writable container layer.
The data doesn’t persist when that container no longer exists. [Docker volumes](https://docs.docker.com/storage/volumes/)
are the preferred way to save data outside the containers. The following two types of Docker volumes are used the most:

+ **Host Volume**: You manage where in the Docker host's file system to store and share the
containers data. To create a host volume, run:

  ```console
  $ docker run -v /path/on/host:/path/in/container ...
  ```
  The file or directory is referenced by its full path on the Docker host. It can exist on the Docker host already, or it will be created automatically if it does not yet exist.

+ **Named Volume**: Docker manage where they are located.  It should be be referred to by specific names.
To create a named volume, run:

  ```console
  $ docker volume create volumeName
  $ docker run -v  volumeName:/path/in/container ...
  ```

Either host volume or named volume can be used for Alluxio containers. For purpose of test,
the host volume is recommended, since it is the easiest type of volume
to use and very performant. More importantly, you know where to refer to the data in the host
file system and you can manipulate the files directly and easily outside the containers.

Therefore, we will use the host volume and mount the host directory `/alluxio_ufs` to the
container location `/opt/alluxio/underFSStorage`, which is the default setting for the
Alluxio UFS root mount point in the Alluxio docker image:
  ```console
  $ docker run -v /alluxio_ufs:/opt/alluxio/underFSStorage   ...
  ```
Of course, you can choose to mount a different path instead of `/alluxio_ufs`.
From version 2.1 on, Alluxio Docker image runs as user `alluxio` by default.
It has UID 1000 and GID 1000.
Please make sure it is writable by the user the Docker image is run as.

## Launch Alluxio Containers for Master and Worker

The Alluxio clients (local or remote) need to communicate with
both Alluxio master and workers. Therefore it is important to make sure clients can reach
both of the following services:

+ Master RPC on port 19998
+ Worker RPC on port 29999

Within the Alluxio cluster, please also make sure the master and worker containers can reach
each other on the ports defined in [General requirements]({{ '/en/deploy/Requirements.html#general-requirements' | relativize_url }}).

We are going to launch Alluxio master and worker containers on the same Docker host machine.
In order to make sure this works for either local or remote clients, we have to set up the
Docker network and expose the required ports correctly.

There are two ways to launch Alluxio Docker containers on the Docker host:
+ A: Use [host network](https://docs.docker.com/network/host/) or
+ B: Use [user-defined bridge network](https://docs.docker.com/network/bridge/)

Host network shares ip-address and networking namespace between the container and the Docker host.
User-defined bridge network allows containers connected to communicate,
while providing isolation from containers not connected to that bridge network.
It is recommended to use host network, option A, for testing.

### Option A: Launch Docker Alluxio Containers Using Host Network

```console
# Launch the Alluxio Master
$ docker run -d --rm \
    --net=host \
    --name=alluxio-master \
    -v /alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=$(hostname -i) \
       -Dalluxio.master.mount.table.root.ufs=/opt/alluxio/underFSStorage" \
    alluxio/alluxio master

#Launch the Alluxio Worker
$ docker run -d --rm \
    --net=host \
    --name=alluxio-worker \
    --shm-size=1G \
    -v /alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.worker.memory.size=1G \
       -Dalluxio.master.hostname=$(hostname -i)" \
    alluxio/alluxio worker
```

Notes:

  1. The argument `--net=host ` tells Docker to use the host network.
     Under this setup, the containers are directly using the host's network adapter.  
     All containers will have the same hostname and IP address as the Docker host,
     and all the host's ports are directly mapped to containers. Therefore, all the required container
     ports `19999, 19998, 29999, 30000` are available for the clients via the Docker host.
     You can find more details about this setting [here](https://docs.docker.com/network/host/).
  1. The argument  `-e ALLUXIO_JAVA_OPTS="-Dalluxio.worker.memory.size=1G -Dalluxio.master.hostname=$(hostname -i)"`
     allocates the worker's memory capacity and bind the master address. 
     When using the `host` network driver, the master can't be referenced to by the master container name `alluxio-master` or
     it will throw `"No Alluxio worker available" ` error.
     Instead, it should be referenced to by the host IP address.
     The substitution `$(hostname -i)` uses the docker host's name instead.
  1. The argument  `--shm-size=1G` will allocate a `1G` tmpfs for the worker to store Alluxio data.
  1. The argument `-v /alluxio_ufs:/opt/alluxio/underFSStorage` tells Docker to use the host volume
     and persist the Alluxio UFS root data in the host directory `/alluxio_ufs`, 
     as explained above in the Docker volume section.

### Option B: Launch Docker Alluxio Containers Using User-Defined Network

Using host network is simple, but it has disadvantages. For example

+ The Services running inside the container could potentially conflict with other services in other
  containers which run on the same port.
+ Containers can access to the host's full network stack and bring up potential security risks.

The better way is using the user-defined network, but we need to explicitly expose the required ports
so that the external clients can reach out the containers' services:

```console
# Prepare the network
$ docker network create alluxio_network

# Launch the Alluxio master
$ docker run -d  --rm \
    -p 19999:19999 \
    -p 19998:19998 \
    --net=alluxio_network \
    --name=alluxio-master \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=alluxio-master \
       -Dalluxio.master.mount.table.root.ufs=/opt/alluxio/underFSStorage" \
    -v /alluxio_ufs:/opt/alluxio/underFSStorage \
    alluxio/alluxio master

# Launch the Alluxio worker
$ docker run -d --rm \
    -p 29999:29999 \
    -p 30000:30000 \
    --net=alluxio_network \
    --name=alluxio-worker \
    --shm-size=1G \
    -v /alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.worker.memory.size=1G \
       -Dalluxio.master.hostname=alluxio-master \
       -Dalluxio.worker.hostname=alluxio-worker" \
    alluxio/alluxio worker
```

Notes:

  1. The argument `--net=alluxio_network` tells Docker to use the user-defined bridge network ```alluxio_network```.
     All containers will use their own container IDs as their hostname, and each of them has a different IP
     address within the network's subnet.
     Containers connected to the same user-defined bridge network effectively expose all ports to each other,
     unless firewall policies are defined. 
     You can find more details about the bridge network driver [here](https://docs.docker.com/network/bridge/).
  1. Only the specified ports (`-p` option) are exposed to the outside network, where the client may be run.
     The command `-p <host-port>:<container-port>` maps the container port to a host port. 
     Therefore, you must explicitly expose the two ports 19999 and 19998 for the master container and the port
     29999 and 30000 for the worker container.
     Otherwise, the clients can't communicate with the master and worker.
  1. You can refer to the master either by the container name
     (`alluxio-master` for master container and `alluxio-worker` for worker container)  or
     by the Docker host's IP address `$(hostname -i)`, if all the communication is within the
     docker network (e.g., no external client outside the docker network). Otherwise, you must
     specify the master and worker's docker host IP that client can reach out (e.g., by `-Dalluxio.worker.hostname=$(hostname -i)`).
     This is required for the external communication between master/worker and
     clients outside the docker network. Otherwise, clients can't connect to worker, since
     they do not recognize the worker's container Id. It will throw error like below:
     ```
     Target: 5a1a840d2a98:29999, Error: alluxio.exception.status.UnavailableException: Unable to resolve host 5a1a840d2a98
     ```

## Verify the Cluster

To verify that the services came up, check `docker ps`. You should see something like
```console
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                      NAMES
1fef7c714d25        alluxio/alluxio     "/entrypoint.sh work…"   39 seconds ago      Up 38 seconds                                  alluxio-worker
27f92f702ac2        alluxio/alluxio     "/entrypoint.sh mast…"   44 seconds ago      Up 43 seconds       0.0.0.0:19999->19999/tcp   alluxio-master
```

If you don't see the containers, run `docker logs` on their container ids to see what happened.
The container ids were printed by the `docker run` command, and can also be found in `docker ps -a`.

Visit `instance-hostname:19999` to view the Alluxio web UI. You should see one worker connected and providing
`1024MB` of space.

To run tests, enter the worker container

```console
$ docker exec -it alluxio-worker /bin/bash
```

Run the tests

```console
$ cd /opt/alluxio
$ ./bin/alluxio runTests
```

To test the remote client access, for example, from the Spark cluster (python 3)

```scala
textFile_alluxio_path = "alluxio://{docker_host-ip}:19998/path_to_the_file"
textFile_RDD = sc.textFile (textFile_alluxio_path)

for line in textFile_RDD.collect():
  print (line)
```

Congratulations, you've deployed a basic Dockerized Alluxio cluster! Read on to learn more about how to manage the cluster and make is production-ready.

## Operations

### Set server configuration

Configuration changes require stopping the Alluxio Docker images, then re-launching
them with the new configuration.

To set an Alluxio configuration property, add it to the Alluxio java options environment variable with

```
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property.name=value"
```

Multiple properties should be space-separated.

If a property value contains spaces, you must escape it using single quotes.

```
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property1=value1 -Dalluxio.property2='value2 with spaces'"
```

Alluxio environment variables will be copied to `conf/alluxio-env.sh`
when the image starts. If you are not seeing a property take effect, make sure the property in
`conf/alluxio-env.sh` within the container is spelled correctly. You can check the
contents with

```console
$ docker exec ${container_id} cat /opt/alluxio/conf/alluxio-env.sh
```

### Run in High-Availability Mode

A lone Alluxio master is a single point of failure. To guard against this, a production
cluster should run multiple Alluxio masters in [High Availability mode]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url }}).

#### Option A: Internal Leader Election

Alluxio uses internal leader election by default.

Provide the master embedded journal addresses and set the hostname of the current master:

```console
$ docker run -d \
  ...
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.embedded.journal.addresses=master-hostname-1:19200,master-hostname-2:19200,master-hostname-3:19200 -Dalluxio.master.hostname=master-hostname-1" \
  alluxio master
```

Set the master rpc addresses for all the workers so that they can query the master nodes find out the leader master.

```console
$ docker run -d \
  ...
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998" \
  alluxio worker
```

You can find more on Embedded Journal configuration [here]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html#option1-raft-based-embedded-journal' | relativize_url }}).

#### Option B: Zookeeper and Shared Journal Storage

To run in HA mode with Zookeeper, Alluxio needs a shared journal directory
that all masters have access to, usually either NFS or HDFS.

Point them to a shared journal and set their Zookeeper configuration.

```console
$ docker run -d \
  ...
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.master.journal.type=UFS -Dalluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/alluxio_journal -Dalluxio.zookeeper.enabled=true -Dalluxio.zookeeper.address=zkhost1:2181,zkhost2:2181,zkhost3:2181" \
  alluxio master
```

Set the same Zookeeper configuration for workers so that they can query Zookeeper
to discover the current leader.

```console
$ docker run -d \
  ...
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.zookeeper.enabled=true -Dalluxio.zookeeper.address=zkhost1:2181,zkhost2:2181,zkhost3:2181" \
  alluxio worker
```

You can find more on ZooKeeper and shared journal configuration [here]({{ '/en/deploy/Running-Alluxio-On-a-HA-Cluster.html#option2-zookeeper-and-shared-journal-storage' | relativize_url }}).

### Enable short-circuit reads and writes

If your compute applications will run on the same nodes as your Alluxio workers,
you can improve performance by enabling short-circuit reads
and writes. This allows applications to read from and write to their
local Alluxio worker without going over the loopback network. Instead, they will
read and write using [domain sockets](https://en.wikipedia.org/wiki/Unix_domain_socket).

On worker host machines, create a directory for the shared domain socket.

```console
$ mkdir /tmp/domain
$ chmod a+w /tmp/domain
```

When starting workers and clients, run their docker containers with `-v /tmp/domain:/opt/domain`
to share the domain socket directory. Also set domain socket properties by passing
`alluxio.worker.data.server.domain.socket.address=/opt/domain` and
`alluxio.worker.data.server.domain.socket.as.uuid=true` when launching worker containers.

```console
$ docker run -d \
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

```console
$ docker run -e \
  ... \
  --cap-add SYS_ADMIN \
  --device /dev/fuse \
  alluxio-fuse fuse \
```

## Troubleshooting

Alluxio server logs can be accessed by running `docker logs $container_id`.
Usually the logs will give a good indication of what is wrong. If they are not enough to diagnose
your issue, you can get help on the
[user mailing list](https://groups.google.com/forum/#!forum/alluxio-users).
