---
layout: global
title: Install Alluxio on Docker
---

Docker can be used to simplify the deployment and management of Alluxio servers.
Using the [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}/){:target="_blank"} Docker
image available on Dockerhub, you can go from
zero to a running Alluxio cluster with a couple of `docker run` commands.
This document provides a tutorial for running Dockerized Alluxio on a single node
with local disk as the under storage.
We'll also discuss more advanced topics and how to troubleshoot.

* Table of Contents
{:toc}

## Prerequisites

- A machine with Docker installed.
- Ports 19998, 29999, and 30000 available

If you don't have access to a machine with Docker installed, you can
provision an AWS EC2 instance (e.g. t2.large)
to follow along with the tutorial. When provisioning the instance, set the security group
so that the following ports are open to your IP address and the CIDR range of the
Alluxio clients (e.g. remote Spark clusters):

+ 19998 for the CIDR range of your Alluxio servers and clients: Allow the clients and workers to communicate
with Alluxio Master RPC processes.
+ 29999 for the CIDR range of your Alluxio and clients: Allow the clients to communicate
with Alluxio Worker RPC processes.
+ 30000 for the IP address of your browser: Allow you to access the Alluxio worker metrics exposed through this port.

To set up Docker after provisioning the instance, which will be referred to as the Docker Host, run

```shell
$ sudo yum install -y docker
# Create docker group
$ sudo groupadd docker
# Add the current user to the docker group
$ sudo usermod -a -G docker $(id -u -n)
# Start docker service
$ sudo service docker start
# Log out and log back in again to pick up the group changes
$ exit
```

## Prepare Docker Volume to Persist Data

By default, all files created inside a container are stored on a writable container layer.
The data doesn’t persist when that container no longer exists. [Docker volumes](https://docs.docker.com/storage/volumes/){:target="_blank"}
are the preferred way to save data outside the containers. The following two types of Docker volumes are used the most:

+ **Host Volume**: You manage where in the Docker host's file system to store and share the
containers' data. To create a host volume, include the following when launching your containers:

  ```shell
  $ docker run -v /path/on/host:/path/in/container ...
  ```
  The file or directory is referenced by its full path on the Docker host. It can exist on the Docker host already, or it will be created automatically if it does not yet exist.

+ **Named Volume**: Docker manage where they are located. It should be referred to by specific names.
To create a named volume, first run:

  ```shell
  $ docker volume create volumeName
  ```
  Then include the following when launching your containers:
  ```shell
  $ docker run -v volumeName:/path/in/container ...
  ```

Either host volume or named volume can be used for Alluxio containers. For purpose of test,
the host volume is recommended, since it is the easiest type of volume
to use and very performant. More importantly, you know where to refer to the data in the host
file system, and you can manipulate the files directly and easily outside the containers.

For example, we will use the host volume and mount the host directory `/tmp/alluxio_ufs` to the
container location `/opt/alluxio/underFSStorage`, which is the default setting for the
Alluxio UFS root mount point in the Alluxio docker image:

```shell
$ mkdir -p /tmp/alluxio_ufs
```

Of course, you can choose to mount a different path instead of `/tmp/alluxio_ufs`.
From version 2.1 on, Alluxio Docker image runs as user `alluxio` by default.
It has UID 1000 and GID 1000.
Please make sure the host volume is writable by the user the Docker image is run as.

## Launch Alluxio Containers for Master and Worker

The Alluxio clients (local or remote) need to communicate with
both Alluxio master and workers. Therefore, it is important to make sure clients can reach
both of the following services:

+ Master RPC on port 19998
+ Worker RPC on port 29999

Within the Alluxio cluster, please also make sure the master and worker containers can reach
each other on the ports defined in [General requirements]({{ '/en/deploy/Software-Requirements.html#general-requirements' | relativize_url }}).

We are going to launch Alluxio master and worker containers on the same Docker host machine.
In order to make sure this works for either local or remote clients, we have to set up the
Docker network and expose the required ports correctly.

There are two ways to launch Alluxio Docker containers on the Docker host:
+ Option1: Use [host network](https://docs.docker.com/network/host/){:target="_blank"} or
+ Option2: Use [user-defined bridge network](https://docs.docker.com/network/bridge/){:target="_blank"}

Host network shares ip-address and networking namespace between the container and the Docker host.
User-defined bridge network allows containers connected to communicate,
while providing isolation from containers not connected to that bridge network.
It is recommended to use host network, option 1, for testing.

{% navtabs network %}
{% navtab Using Host Network %}

Launch the Alluxio Master

```shell
$ docker run -d --rm \
    --net=host \
    --name=alluxio-master \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost \
       -Dalluxio.dora.client.read.location.policy.enabled=true \
       -alluxio.user.short.circuit.enabled=false \
       -alluxio.master.worker.register.lease.enabled=false \
       -alluxio.worker.block.store.type=PAGE \
       -alluxio.worker.page.store.type=LOCAL \
       -alluxio.worker.page.store.sizes=1GB \
       -alluxio.worker.page.store.page.size=1MB \
       -alluxio.worker.page.store.dirs=/mnt/ramdisk \
       -alluxio.dora.client.ufs.root=/opt/alluxio/underFSStorage" \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} master
```

Launch the Alluxio Worker

```shell
$ docker run -d --rm \
    --net=host \
    --name=alluxio-worker \
    --shm-size=1G \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost \
       -Dalluxio.dora.client.read.location.policy.enabled=true \
       -alluxio.user.short.circuit.enabled=false \
       -alluxio.master.worker.register.lease.enabled=false \
       -alluxio.worker.block.store.type=PAGE \
       -alluxio.worker.page.store.type=LOCAL \
       -alluxio.worker.page.store.sizes=1GB \
       -alluxio.worker.page.store.page.size=1MB \
       -alluxio.worker.page.store.dirs=/mnt/ramdisk \
       -alluxio.dora.client.ufs.root=/opt/alluxio/underFSStorage" \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} worker
```

Notes:

  1. The argument `--net=host ` tells Docker to use the host network.
     Under this setup, the containers are directly using the host's network adapter.
     All containers will have the same hostname and IP address as the Docker host,
     and all the host's ports are directly mapped to containers. Therefore, all the required container
     ports `19998, 29999, 30000` are available for the clients via the Docker host.
     You can find more details about this setting [here](https://docs.docker.com/network/host/){:target="_blank"}.
  1. The argument  `-e ALLUXIO_JAVA_OPTS="-D..."` sets the required configurations for the Alluxio cluster.
  1. The argument `-v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage` tells Docker to use the host volume
     and persist the Alluxio UFS root data in the host directory `/tmp/alluxio_ufs`,
     as explained above in the Docker volume section.

{% endnavtab %}
{% navtab Using User-Defined Network %}

Using host network is simple, but it has disadvantages. For example

+ The Services running inside the container could potentially conflict with other services in other
  containers which run on the same port.
+ Containers can access to the host's full network stack and bring up potential security risks.

The better way is using the user-defined network, but we need to explicitly expose the required ports
so that the external clients can reach out the containers' services:

Prepare the network

```shell
$ docker network create alluxio_network
```

Launch the Alluxio master

```shell
$ docker run -d  --rm \
    -p 19999:19999 \
    -p 19998:19998 \
    --net=alluxio_network \
    --name=alluxio-master \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost \
       -Dalluxio.dora.client.read.location.policy.enabled=true \
       -alluxio.user.short.circuit.enabled=false \
       -alluxio.master.worker.register.lease.enabled=false \
       -alluxio.worker.block.store.type=PAGE \
       -alluxio.worker.page.store.type=LOCAL \
       -alluxio.worker.page.store.sizes=1GB \
       -alluxio.worker.page.store.page.size=1MB \
       -alluxio.worker.page.store.dirs=/mnt/ramdisk \
       -alluxio.dora.client.ufs.root=/opt/alluxio/underFSStorage" \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} master
```

Launch the Alluxio worker
```shell
$ docker run -d --rm \
    -p 29999:29999 \
    -p 30000:30000 \
    --net=alluxio_network \
    --name=alluxio-worker \
    --shm-size=1G \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost \
       -Dalluxio.dora.client.read.location.policy.enabled=true \
       -alluxio.user.short.circuit.enabled=false \
       -alluxio.master.worker.register.lease.enabled=false \
       -alluxio.worker.block.store.type=PAGE \
       -alluxio.worker.page.store.type=LOCAL \
       -alluxio.worker.page.store.sizes=1GB \
       -alluxio.worker.page.store.page.size=1MB \
       -alluxio.worker.page.store.dirs=/mnt/ramdisk \
       -alluxio.dora.client.ufs.root=/opt/alluxio/underFSStorage" \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} worker
```

Notes:

  1. The argument `--net=alluxio_network` tells Docker to use the user-defined bridge network `alluxio_network`.
     All containers will use their own container IDs as their hostname, and each of them has a different IP
     address within the network's subnet.
     Containers connected to the same user-defined bridge network effectively expose all ports to each other,
     unless firewall policies are defined.
     You can find more details about the bridge network driver [here](https://docs.docker.com/network/bridge/){:target="_blank"}.
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

{% endnavtab %}
{% endnavtabs %}

## Verify the Cluster

To verify that the services came up, check `docker ps`. You should see something like
```shell
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                      NAMES
1fef7c714d25        alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}     "/entrypoint.sh work…"   39 seconds ago      Up 38 seconds                                  alluxio-worker
27f92f702ac2        alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}     "/entrypoint.sh mast…"   44 seconds ago      Up 43 seconds       0.0.0.0:19999->19999/tcp   alluxio-master
```

If you don't see the containers, run `docker logs` on their container ids to see what happened.
The container ids were printed by the `docker run` command, and can also be found in `docker ps -a`.

Visit `instance-hostname:19999` to view the Alluxio web UI. You should see one worker connected and providing
`1024MB` of space.

To run tests, enter the worker container

```shell
$ docker exec -it alluxio-worker /bin/bash
```

Run the tests

```shell
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

## Advanced Setup

### Launch Alluxio with the development image

A separate development image, `alluxio/alluxio-dev`, is available on Dockerhub for development usage. Unlike the default `alluxio/alluxio` image that 
only contains packages needed for Alluxio service to run, the `alluxio-dev` image installs more development tools, including gcc, make, async-profiler, etc., 
making it easier for users to deploy more services in the container along with Alluxio. 

To use the development image, simply replace `alluxio/alluxio` with `alluxio/alluxio-dev` in the container launching process.

### Set server configuration

Configuration changes require stopping the Alluxio Docker images, then re-launching
them with the new configuration.

To set an Alluxio configuration property, add it to the Alluxio java options environment variable with

```properties
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property.name=value"
```

Multiple properties should be space-separated.

If a property value contains spaces, you must escape it using single quotes.

```properties
-e ALLUXIO_JAVA_OPTS="-Dalluxio.property1=value1 -Dalluxio.property2='value2 with spaces'"
```

Alluxio environment variables will be copied to `conf/alluxio-env.sh`
when the image starts. If you are not seeing a property take effect, make sure the property in
`conf/alluxio-env.sh` within the container is spelled correctly. You can check the
contents with

```shell
$ docker exec ${container_id} cat /opt/alluxio/conf/alluxio-env.sh
```

### Enable POSIX API access

Alluxio POSIX access is implemented via FUSE.
To enable POSIX accesses to Alluxio in a docker environment, we will run a standalone Alluxio Fuse container.

First make sure a directory with the right permissions exists on the host to [bind-mount](https://docs.docker.com/storage/bind-mounts/){:target="_blank"} in the Alluxio FUSE container:
```shell
$ mkdir -p /tmp/mnt/alluxio-fuse && sudo chmod -R a+rwx /tmp/mnt/alluxio-fuse
```

The original [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}-fuse](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}-fuse/){:target="_blank"} has been deprecated. Now you can enable access to Alluxio on Docker host using the POSIX API by [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}/){:target="_blank"} Docker image, the same one used for launching Alluxio master and worker.

For example, the following commands run the alluxio-fuse container as a long-running client that presents Alluxio file system through a POSIX interface on the Docker host:

Run the Alluxio FUSE service to create a FUSE mount in the host bind-mounted directory:
```shell
$ docker run -d --rm \
    --net=host \
    --name=alluxio-fuse \
    -v /tmp/mnt:/mnt:rshared \
    -v /tmp/alluxio_ufs:/opt/alluxio/underFSStorage \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost" \
    --cap-add SYS_ADMIN \
    --device /dev/fuse \
    --security-opt apparmor:unconfined \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} fuse \
    /opt/alluxio/underFSStorage /mnt/alluxio-fuse \
    -o allow_other -o entry_timeout=3600 -o attr_timeout=3600
```

Notes
- `-v /tmp/mnt:/mnt:rshared` binds path `/mnt/alluxio-fuse` the default directory to Alluxio through fuse inside the container, to a mount accessible at `/tmp/mnt/alluxio-fuse` on host.
To change this path to `/foo/bar/alluxio-fuse` on host file system, replace `/tmp/mnt` with `/foo/bar`.
- `--cap-add SYS_ADMIN` launches the container with [SYS_ADMIN](http://man7.org/linux/man-pages/man7/capabilities.7.html)
capability.
- `--device /dev/fuse` shares host device `/dev/fuse` with the container.
- The command to start Fuse container takes 4 arguments. Take the command above as an example:
  - "fuse" - indicating launching AlluxioFuse process
  - "/opt/alluxio/underFSStorage" - the under storage of the Alluxio system
  - "/mnt/alluxio-fuse" - the mount point which Alluxio is mounted to inside the container
  - "-o allow_other -o entry_timeout=3600 -o attr_timeout=3600allow_other -o entry_timeout=3600 -o attr_timeout=3600" - AlluxioFuse options

See [Fuse Advanced Tuning]({{ '/en/fuse-sdk/Advanced-Tuning.html' | relativize_url }})
and [Local Cache Tuning]({{ '/en/fuse-sdk/Local-Cache-Tuning.html' | relativize_url }})
for more details about Alluxio Fuse mount options and tuning.

### Set up Alluxio Proxy

To start the Alluxio proxy server inside a Docker container, simply run the following command:

```shell
$ docker run -d \
    --net=host \
    --name=alluxio-proxy \
    --security-opt apparmor:unconfined \
    -e ALLUXIO_JAVA_OPTS=" \
       -Dalluxio.master.hostname=localhost" \
    alluxio/{{site.ALLUXIO_DOCKER_IMAGE}} proxy
```

See [Properties List]({{ '/en/reference/Properties-List.html' | relativize_url }}) for more
configuration options for Alluxio proxy server.


## Troubleshooting

If the Alluxio servers are not able to be launched, remove the `-d` when running the `docker run` command
so that the processes can be launched in the foreground and the console output can provide some helpful information.

If the Alluxio servers are launched, their logs can be accessed by running `docker logs $container_id`.
Usually the logs will give a good indication of what is wrong. If they are not enough to diagnose
your issue, you can get help on the
[user mailing list](https://groups.google.com/forum/#!forum/alluxio-users){:target="_blank"}
or [github issues](https://github.com/Alluxio/alluxio/issues){:target="_blank"}.


## FAQ

### AvailableProcessors: returns 0 in docker container

When you execute `alluxio ls` in the alluxio master container and got the following error.

```bash
bash-4.4$ alluxio fs ls /
Exception in thread "main" java.lang.ExceptionInInitializerError
...
Caused by: java.lang.IllegalArgumentException: availableProcessors: 0 (expected: > 0)
        at io.netty.util.internal.ObjectUtil.checkPositive(ObjectUtil.java:44)
        at io.netty.util.NettyRuntime$AvailableProcessorsHolder.setAvailableProcessors(NettyRuntime.java:44)
        at io.netty.util.NettyRuntime$AvailableProcessorsHolder.availableProcessors(NettyRuntime.java:70)
        at io.netty.util.NettyRuntime.availableProcessors(NettyRuntime.java:98)
        at io.grpc.netty.Utils$DefaultEventLoopGroupResource.<init>(Utils.java:394)
        at io.grpc.netty.Utils.<clinit>(Utils.java:84)
        ... 20 more
```

This error can be fixed by adding `-XX:ActiveProcessorCount=4` as jvm parameter.
