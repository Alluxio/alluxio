## What is Vagrant?

Vagrant can create VM images (VirtualBox VMWare Fusion), Docker containers, and AWS and OpenStack instances. 

## Why Use Vagrant?

Installing a Tachyon cluster is a huge undertaking. Building a realistic and correct environment usually requires multifacted knowledge. 

Tachyon uses a variety of under filesystems. Some of these filesystems must be installed and configured on target systems that are different from development environment. Building identical VMs across all running environments accelerates development, testing, and adoption process.

## What inside?

This directory contains Vagrant recipe to create VirtualBox images and Amazon EC2 instances and configurations to initialize Hadoop (both 1.x and 2.x), CephFS, and GlusterFS.

Once Vagrant is installed, starting an Tachyon cluster requires only `vagrant up` command. A two-VM cluster is then created. `vagrant destroy` command destroys the cluster.

`init.yml` is the configuration file that sets different cluster parameters. The file is self-explanatory.  

## VirtualBox VM

Run command `vagrant up [--provider=virtualbox]` to start VirtualBox VM. After VM is up, login to the VM as `root` and password as `vagrant`.

## Amazon EC2 Instances

Install aws vagrant plugin first. To date, 0.5.0 plugin is tested.

`vagrant plugin install vagrant-aws`

Then update configurations in `ec2-config.yml` and shell environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`.

Run `vagrant up --provider=aws [--no-parallel]` to create EC2 instances.