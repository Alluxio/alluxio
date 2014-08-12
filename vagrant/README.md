## Vagrant

Vagrant creates VirtualBox VM images based on a receipe.

## Installation
Install VirtualBox and Vagrant. Make sure they work properly.

## Create a VM

Just run

    $ cd vagrant
    $ vagrant up

You then get a VM console. The `root` password is `vagrant`.
In the background, the shell scripts also download necessary packages, create a single node Ceph cluster, and run unit tests.

## Run unit test

Run unit test using CephFS as under filesystem in the following way.

    $cd /tachyon
    $mvn test -Dtest.profile=cephfs -Dhadoop.version=2.3.0 -Dtachyon.underfs.hadoop.core-site=/tachyon/conf/core-site.xml
