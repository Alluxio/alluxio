## What's docker
Please refer to www.docker.com for help, simply put, you can think it as a lightweight virtual machine, but this is not an exact description.

## What's included
* dnsmasq-precise is a dns server docker image from ubuntu:precise
* apache-hadoop-hdfs-precise is a docker image pre-installed hadoop1.0.4, openjdk6, and some system utils like ping, python, sudo..., also built from ubuntu:precise
* The two images above are modified from amplab/docker-scripts
* tachyon-dev contains three docker images: tachyon-base:dev, tachyon-master:dev, tachyon-worker:dev, their names have self-explained their functions
* in the future, there will be tachyon-1.4.1, tachyon-1.5.0, etc. all directories are named as tachyon-{version}
* Uniqueness of tachyon-dev is it deploys your tachyon home on your host to the cluster, while the future tachyon-1.4.1, tachyon-1.5.0 etc. deploy tachyon releases directly from online repositories to your cluster.

## Why to use tachyon docker
If you're developing tachyon on your host machine, you make some modifications to tachyon on host and want to see how will it works on a cluster. With tachyon-dev, just "mvn package" on host machine, and follow the instructions below to deploy the current tachyon "release" of you own on a "docker cluster".

## How to use tachyon docker
* make sure this directory is under your tachyon home directory
* before building images, you should configure tachyon configuration template file in tachyon-dev/tachyon-base/files, otherwise, default configuration will be used
* cd docker, ./build to build all docker images in this directory
* if you want to, say, independently build images in tachyon-dev, cd tachyon-dev, ./build
* after building the images, ./deploy/deploy.sh to start nameserver, master and worker nodes. then you will have a virtualized tachyon cluster with hdfs support on your host. Default worker number is 2, you can change it in deploy/deploy.sh:NUM_WORKERS
* the script will output help information on how to ssh into your cluster, how to see the web UI,etc
* ./deploy/killAll.sh to kill the nameserver, master and workers

## Suggestion for Mac Users
Suggestion is: install a ubuntu virtual machine to use docker, since docker leverages linux features, even if you install docker directly on Mac according to official document, it actually installs a tinycore linux virtual machine on VirtualBox, but this tinycore linux doesn't have a browser, you need to configure the network between your host and VirtualBox and share folders between them. so why not just choose your favorite virtual machine software, install a ubuntu, and work in this ubuntu either directly in the VM or ssh into it from host with your favorite terminal.

## Development issues
### Testing environment:
* ubuntu14.04
* docker0.9.1

If you're running a virtual machine, all the examples below should be run in the VM, you have to share the tachyon home folder on your host with your VM.

### Example 0:
Follow instructions below, you will run a cluster with a master and 2 workers, ssh into master, run tachyon tests, and finally have a look at web UI

  set TACHYON_UNDERFS_ADDRESS in tachyon-dev/tachyon_base/files/tachyon-env.sh.template to $TACHYON_HOME/underfs

  cd tachyon-dev && ./build && cd ..

  ./deploy/deploy.sh


Then, you'll have output in your terminal, finally, there will be instructions surrounded by 2 lines of stars. The instructions will tell you how to see Tachyon web UI and how to ssh to cluster nodes.

Copy the web UI's URL in the instruction to your browser, you should see tachyon's web UI, if worker number is not 2, refresh the page after a while because workers may be connecting to master.  

Then, we want to run "bin/tachyon runTests" on master node, so, copy the command for sshing into master node from the instruction to your command line, run it and you should be in your master node now. 

  cd /opt/tachyon_container, here is the tachyon home on your master node

  run "bin/tachyon-start.sh worker" to start a local worker on master because runTests need a local worker

  after the local worker start, refresh the web UI, 3 workers should be active now

  now, run "bin/tachyon runTests", all tests should pass

  refresh web UI again, and browse the file system in the UI, see files in memory!

Now, you should know how to use tachyon docker images.

### Example 1
In this example, we want to deploy a hdfs as under filesystem for tachyon.

* set under filesystem settings to hdfs in tachyon-dev/tachyon_base/files/tachyon-env.sh.template
* cd tachyon-dev && ./build && cd ..
* ./deploy/deploy.sh
* "sudo docker ps" to see all the running docker containers, find the ID(first item) for tachyon-master:dev, replace "$ID" in following commands with this ID
* "sudo docker logs $ID" to see what happens when docker starts this container and runs its default commands
* Bug happens! I got the exception:

  Exception in thread "main" java.lang.NoSuchMethodError: org.apache.hadoop.ipc.RPC.getProxy
  
  You can ssh into master, and see hadoop logs in /var/log/hadoop/*, you'll find that hadoop runs well, but when tachyon tries to format, this exception is thrown!

  I've digged into tachyon-0.5.0-SNAPSHOT-with-dependency.jar, and find the RPC class and getProxy method. I'm trapped by this problem.
