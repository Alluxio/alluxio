## Vagrant

Vagrant creates VirtualBox VM images based on a receipe.

## Installation
Install VirtualBox and Vagrant. Make sure they work properly.

## Create a VM

Just run

    $ cd vagrant
    $ vagrant up

You then get a VM console. The `root` password is `vagrant`.
In the background, the shell scripts also download necessary packages, create a single brick gluster cluster, and run unit tests.

## Run unit test

Run unit test using Glusterfs as under filesystem in the following way.

    $cd /tachyon
    $mvn test -Dtest.profile=glusterfs -Dhadoop.version=2.3.0 -Dtachyon.underfs.glusterfs.mounts=/vol -Dtachyon.underfs.glusterfs.volumes=testvol 
