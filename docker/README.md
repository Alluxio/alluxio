## What is docker

Please refer to [official docker homepage](https://www.docker.com) for help. Simply put, you can think of it as a lightweight virtual machine although this is not an exact description.

## Why to use tachyon-dev

If you're developing tachyon on your host machine, you make some modifications to tachyon on host
and want to see how will it work on a cluster. With tachyon-dev, just `mvn package` on host
machine, and follow the instructions below to deploy the current tachyon "release" of your own on a
"docker cluster".

## What is included
* **precise** in the docker containers' names mean that the container's base image is **ubuntu:precise**
* **dnsmasq-precise:** a container serving as dns server to resolve hosts in the virtualized cluster
* **apache-hadoop-hdfs1.0.4-precise:** a container with the following software preinstalled like hadoop1.0.4, openjdk6, and some system utils like ping, python2.7, sudo
* The two containers above are modified from [github:amplab/docker-scripts](https://github.com/amplab/docker-scripts)
* **tachyon-dev/tachyon-base:** a container built from apache-hadoop-hdfs1.0.4-precise with configurations of tachyon
* **tachyon-dev/tachyon-master:** a container built from tachyon-base, serving as tachyon's master node
* **tachyon-dev/tachyon-worker:** a container built from tachyon-base, serving as tachyon's worker node
* in the future, there will be **tachyon-0.4.0**, **tachyon-0.5.0**, etc. all directories will be named as **tachyon-{version}**
* tachyon-dev is unique because it deploys the tachyon source directory containing the docker directory on your host to the cluster, so, before deploying tachyon-dev, you have to run `mvn package`. while the future tachyon-0.4.0, tachyon-0.5.0 etc. will deploy tachyon releases downloaded from online repositories to the cluster
to your cluster.
* **deploy:** scripts to start and stop the cluster


## Tips for Mac Users

It will be much easier to use docker on a **ubuntu:14.04** virtual machine, other versions of ubuntu won't provide docker in apt-get, and low version of kernel may lack the feature needed by docker. 

since docker leverages linux kernel features, even if you install docker directly on Mac according to [official document](https://docs.docker.com/installation/), it actually
installs a tiny-core linux virtual machine on VirtualBox, but this tiny-core linux doesn't have a
browser for you to view tachyon web UI, you need to configure the network between your host and VirtualBox and share folders
between them. 

If you would prefer to use a Mac enviornment, first ssh into boot2docker.

* boot2docker ssh

Then install bash.

* tce-ab
* Search for bash.tcz, then install

Clone the repo in your boot2docker VM.

SCP the tachyon snapshot jar you wish to test to tachyon/core/target. You can get the VM's ip by

* boot2docker ip

Now you can build and deploy.

* cd tachyon-dev
* ./build
* cd ..
* ./deploy/deploy.sh

## How to use tachyon-dev
### Enter docker directory
* Make sure the docker directory is under the tachyon development directory on your host and enter the docker directory, run `pwd`, you should get something like `/..../tachyon/docker`

### Install docker
* If you haven't installed docker, please refer to [official docker document](https://docs.docker.com/installation/) for help.
* **sudo apt-get install docker** will install something related to KDE3/GENOME's docklet instead of the docker we want to use, do visit the official site for instructions!

### Configure tachyon-dev
* Before building containers, please configure tachyon template configuration files in
`tachyon-dev/tachyon-base/files`, otherwise, default configurations will be used
* If the configuration files are modified, you have to run `cd tachyon-dev`, `./build` to re-build containers in tachyon-dev

### Become root
* Since docker typically needs to be run as root(of course, you can change its executive privilege), run `sudo -s` to become root before the following steps

### Build containers
* Make sure you are under the docker directory now
* `./build` to build all docker containers, this may take a while because some containers need to download several packages
* If you want to independently build images in e.x. tachyon-dev, `cd tachyon-dev`, `./build` or in dnsmasq-precise, `cd dnsmasq-precise`, `./build`

### Check containers
* You can check whether containers in tachyon-dev have been successfully built by running `docker images | grep tachyon`, you should see information related to tachyon-base:dev, tachyon-master:dev and tachyon-worker:dev

### Package tachyon snapshot
* If tachyon jars on your host are not up-to-date, run `mvn package`

### Deploy the cluster
* Make sure to be under the docker directory
* `./deploy/deploy.sh` to start nameserver, master and worker nodes. Default worker
number is 2, you can change the default value of `NUM_WORKERS` in `deploy/deploy.sh`

### Wait for several seconds

* Now, you have a virtualized tachyon cluster on your host. 
* there is help information on how to ssh into your cluster, how to see the web UI, etc in stdout

### Stop the cluster
* `./deploy/killAll.sh`, this will kill dnsmasq, tachyon-master, tachyon-worker containers

## Example Usages:
### Testing environment:
* ubuntu14.04 and ubuntu12.04
* docker0.9.1

*If you're running a virtual machine, all the examples below should be run in the VM, you need to
share the tachyon home folder on your host with your VM.*

### Example 0

#### Goal
* run a tachyon cluster in local mode with 1 master and 2 workers
* ssh into master, run tachyon tests
* finally have a look at web UI

#### Instructions
set `TACHYON_UNDERFS_ADDRESS` in `tachyon-dev/tachyon-base/files/tachyon-env.sh.template` to `$TACHYON_HOME/underfs`

Then:

    sudo -s

if you have built other containers, just re-build containers in tachyon-dev via `cd tachyon-dev && ./build && cd ..` or you can re-build all containers via `./build`

Then: 

    ./deploy/deploy.sh

you'll have output in your terminal like:

    starting nameserver container
    WARNING: Local (127.0.0.1) DNS resolver found in resolv.conf and containers can't use it. Using default external servers : [8.8.8.8 8.8.4.4]    
    started nameserver container:  4cc00    bdcb23bf76264a3c807a328c439d36aca0452fe1c80af04270ef9c31c40    
    DNS host->IP file mapped:      /tmp/dnsdir_29039/0hosts    
    NAMESERVER_IP:                 172.17.0.9    
    waiting for nameserver to come up    
    starting master container    
    local tachyon directory is resolved to: /media/sf_tachyon    
    started master container:     323f34924a44781a338d6a60baf181cfc779c6dde47c7ef8a90329650a44cbe1    
    MASTER_IP:                    172.17.0.10    
    NO.1 remote worker    
    starting worker1    
    started worker container: 1df5589fb07a32f0160c00b51f479eba8c9f667090ea9409add120465c44485e    
    WORKER_IP:                 172.17.0.11    
    NO.2 remote worker    
    starting worker2    
    started worker container: e3a2a61c7177848432c8ad70b1461c8c467d98f41a0af83b7a6d545c30fe7dad    
    WORKER_IP:                 172.17.0.12    
    **************************************************    
    
    visit Tachyon Web UI at: http://172.17.0.10:19999
    
    ssh into master via:     ssh -i /tmp/id_rsa13436 -o UserKnownHostsFile=/dev/null -o     StrictHostKeyChecking=no root@172.17.0.10    
    you can also ssh into workers via the command above with the ip substituted    
    
    after ssh into either master/worker, /root/tachyon_container is tachyon home
    
    to enable the host to resolve {'master', 'worker1', 'worker2'...} to corresponding ip, set 'nameserver     172.17.0.9' as first line in your host's /etc/resolv.conf    
    
    ************************************************** 

The instructions between star lines will tell you how to see Tachyon web UI and how to ssh to cluster nodes.

Copy the web UI's URL in the instruction to your browser, you should see tachyon's web UI, if worker
number is not 2, refresh the page after a while because workers may be connecting to master.

copy the command for sshing into master node from the instruction to your command line, run it and you should be in your master node as root now.

cd to the tachyon home on your master node:

    cd /opt/tachyon_container

start a local worker on master because runTests need a local worker:

    bin/tachyon-start.sh worker Mount

after the local worker start, refresh the web UI, 3 workers should be active now

run tests:

    bin/tachyon runTests

all tests should pass if everything works well

refresh web UI again, and browse the file system in the UI, see files in memory!

If something goes wrong, you can check via:

    # get the container ID of a running container you want to inspect
    docker ps
    # copy the first column(container ID), 
    docker logs #containerID

or ssh into master, worker, then:

    cd /opt/tachyon_container/logs
    ls

and view the logs

after all the work, remember to stop the cluster, if you're now in a container, exit it and then

    ./deploy/killAll.sh

### Example 1

#### Goal
* deploy hdfs as under filesystem for tachyon.

#### Instructions
set `TACHYON_UNDERFS_ADDRESS` in `tachyon-dev/tachyon-base/files/tachyon-env.sh.template` to `hdfs://master:9000`

Now follow the same instructions in Example 0 above, except the first step
