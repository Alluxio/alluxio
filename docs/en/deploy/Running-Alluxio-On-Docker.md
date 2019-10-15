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
provision a small AWS EC2 instance (for example  t2.small, it costs about $0.03/hour) to follow along with
the tutorial. When provisioning the instance, set the security group so that the following ports are open to  your IP address and the CIDR range of the Alluxio clients (e.g. remote Spark clusters): 

+ 19999 for the IP address of your browser:  Allow you to access the Alluxio web UI. 
+ 19998 for the  CIDR range of your Alluxio clients: Allow the clients to communicate with Alluxio Master RPC process.
+ 29999 for the  CIDR range of your Alluxio clients: Allow the clients to communicate with Alluxio Workder RPC processes.

To set up Docker after provisioning the instance, which will be referred as the Docker Host , run

```console
$ sudo yum install -y docker
$ sudo service docker start
# Add the current user to the docker group
$ sudo usermod -a -G docker $(id -u -n)
# Log out and log back in again to pick up the group changes
$ exit
```

## Prepare network and UFS volume (optional)

Create a user-defined bridge network (optional) for connecting Alluxio containers, and create a volume for storing ufs data.

```console
$ docker network create alluxio_network
$ docker volume create ufs   
```

You can decide to use them or not.  For the named volume ```ufs```, you can replace it with the host's absolute path name ```/alluxio_ufs```. Docker will create the directory in the host if it does not exist, or use the directory (including all the data underneath it) if already exist. 


## Launch Alluxio Containers for Master and Worker

According to Alluxio architecture, the Alluxio clients (local or remote) need to communicate with both Alluxio master and Workers. Therefore it is important to make sure the client can reach out both of the following services:

- Master:19998   (master's rpc port)
- Worker:29999   (worker's rpc port)    

 There are two ways to launch Alluxio Docker containers on the Docker host -- using host network or the user-defined bridge network  ```alluxio_nw``` we created before. 



### Launch Docker Alluxio Containers Using Host Network

```
# Launch the Alluxio Master
docker run -d --rm \
  --net=host \
  --name=alluxio-master \
  -v ufs:/opt/alluxio/underFSStorage \
  alluxio/alluxio master
  
#Launch the Alluxio Worker
docker run -d --rm \
  --net=host \
  --name=alluxio-worker \
  --shm-size=1G \
  -v ufs:/opt/alluxio/underFSStorage \
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.worker.memory.size=1G -Dalluxio.master.hostname=$(hostname -i)" \
  alluxio/alluxio worker  
```

Notes: 

1. The ```--net=host ``` argument will tell docker to use the host network.  All containers will have the same hostname and IP address as the Docker host, and all the host's ports are exposed to and directly mapped to containers, of course include the three important ports ```19999, 19998, 29999``` and they are available for the clients. 
2.  The ```-e ALLUXIO_JAVA_OPTS="-Dalluxio.worker.memory.size=1G -Dalluxio.master.hostname=$(hostname -i)"``` argument allocates the worker's memory capacity and bind the master address. In host network,  the master can't be refereed by the container name. It will throw "No Alluxio worker available" error if use the master's container name.  Instead, it can be refereed by the host IP address. The substitution ```$(hostname -i)``` does the trick.
3. The `--shm-size=1G` argument will allocate a `1G` tmpfs for the worker to store Alluxio data.



### Launch Docker Alluxio Containers Using User-Defined Network

Using host network is simple, but it has disadvantages. For example 

+ The Services running inside the container could potentially conflict with other services in other containers which run on the same port.
+ Containers can access to the host's full network stack and bring up protentional security risks. 

The better way is using the user-defined network, but we need to explicitly expose the required ports:   


```console
# Launch the Alluxio master
$ docker run -d  --rm \
  -p 19999:19999 \
  -p 19998:19998 \
  --net=alluxio_network \
  --name=alluxio-master \
  -v ufs:/opt/alluxio/underFSStorage \
  alluxio/alluxio master
  
# Launch the Alluxio worker
  docker run -d --rm \
  -p 29999:29999 \
  --net=alluxio_network \
  --name=alluxio-worker \
  --shm-size=1G \
  -v /alluxio_ufs:/opt/alluxio/underFSStorage \
  -e ALLUXIO_JAVA_OPTS="-Dalluxio.worker.memory.size=1G  -Dalluxio.master.hostname=$(hostname -i)  -Dalluxio.worker.hostname=$(hostname -i)" \
  alluxio/alluxio worker
```

Notes: 

1. The ```--net=alluxio_network``` argument will tell docker to use the user-defined bridge network.  All containers will use their own container IDs as their hostname, and each of they has a different IP address within the network's subnet. Only the specified ports (-p option) are exposed to the Docker host.

2. You must expose the two ports 19999 and 19998 for the Master container and the port 29999 for the  Worker container. Otherwise, the clients can't communicate with the the master and worker. 

3. When run worker container, you can refer to the Master either by the master container name   ``` -Dalluxio.master.hostname=alluxio-master```  or  by the Host's IP address: ``` -Dalluxio.master.hostname=$(hostname -i) ```, since this is an internal communication between master and worker within the docker network. 

4. When run worker container, you must refer to the Worker by the docker host IP address:  ```-Dalluxio.worker.hostname=$(hostname -i) ```. You can't use the worker container name ```alluxio-worker", since it is an external communication between worker and clients outside the docker network.  Otherwise, clients but can't connect to worker, since clients do not recognize the worker's container Id. It will throw error like below:

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

```
textFile_alluxio_path = "alluxio://{docker_host-ip}:19998/path_to_the_file"  
textFile_RDD = sc.textFile (textFile_alluxio_path)

for line in textFile_RDD.collect(): 
  print (line)  
```

Congratulations, you've deployed a basic Dockerized Alluxio cluster! Read on to learn more about how to manage the cluster and make is production-ready.


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

```console
$ docker exec ${container_id} cat /opt/alluxio/conf/alluxio-env.sh
```

### Run in High-Availability Mode

A lone Alluxio master is a single point of failure. To guard against this, a production
cluster should run multiple Alluxio masters and use internal leader election or Zookeeper-based leader election.
One of the masters will be elected leader and serve client requests.
If it dies, one of the remaining masters will become leader and pick up where the previous master left off.

#### Internal leader election

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

#### Zookeeper-based leader election

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

