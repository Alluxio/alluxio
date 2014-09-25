## What is Vagrant?

Vagrant can create VM images (VirtualBox VMWare Fusion), Docker containers, and AWS instances. 

## Why Use Vagrant?

Installing a Tachyon cluster is a huge undertaking. Building a realistic and correct environment usually requires multifacted knowledge. 

Tachyon uses a variety of under filesystems. Some of these filesystems must be installed and configured on target systems that are different from development environment. Building identical VMs across all running environments accelerates development, testing, and adoption process.

## What inside?

This directory contains Vagrant recipe to create VM images and configurations to initialize Hadoop (both 1.x and 2.x), CephFS, and GlusterFS.

Once vagrant is installed, starting an Tachyon cluster requires only `vagrant up` command. A two-VM cluster is then created. `vagrant destroy` command destroys the cluster.
 
`init.yaml` is the configuration file that sets different cluster parameters. The file is self-explanatory.  
