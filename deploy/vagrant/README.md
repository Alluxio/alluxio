## What is Vagrant?

Vagrant can create VM images (VirtualBox VMWare Fusion), Docker containers, and AWS and OpenStack
instances.

## Why Use Vagrant?

Installing a Tachyon cluster is a huge undertaking. Building a realistic and correct environment
usually requires multifacted knowledge.

Tachyon uses a variety of under filesystems. Some of these filesystems must be installed and
configured on target systems that are different from development environment. Building identical VMs
across all running environments accelerates development, testing, and adoption process.

## What inside?

This directory contains Vagrant recipe to create VirtualBox images and Amazon EC2 instances and
configurations to initialize Hadoop (both 1.x and 2.x), CephFS, and GlusterFS.

Please download and install Vagrant (at least version 1.6.5). Once Vagrant is installed, starting an
Tachyon cluster requires only `vagrant up` command. A two-VM cluster is then created. `vagrant
destroy` command destroys the cluster.

`conf/init.yml` is the configuration file that sets different cluster parameters. They are explained
below.

<table class="table">
<tr>
    <th>Parameter</th><th>Description</th><th>Values</th>
</tr>
<tr>
    <td>Ufs</td><td>Tachyon Underfilesystem</td><td>glusterfs|hadoop2|hadoop1</td>
</tr>
<tr>
    <td>Provider</td><td>Vagrant Providers</td><td>vb|aws|openstack</td>
</tr>
<tr>
    <td>Memory</td><td>Memory (in MB) to allocate for Virtualbox image</td><td>at least 1024</td>
</tr>
<tr>
    <td>Total</td><td>Number of images to start</td><td>at least 1</td>
</tr>
<tr>
    <td>Addresses</td><td>Internal IPs given to each VM. The last one is designated as Tachyon master.
For VirtualBox, the addresses can be arbitrary.
For AWS, the addresses should be within the same availability zone.
For OpenStack, since the compute node instances use DHCP, these addresses are not used.
</td><td>IPv4 address string</td>
</tr>
</table>

## VirtualBox Provider

Run command `vagrant up [--provider=virtualbox]` to start VirtualBox VM. After VM is up, login to
the VM as `root` and password as `vagrant`.

## AWS Provider

Install aws vagrant plugin first. To date, 0.5.0 plugin is tested.

    vagrant plugin install vagrant-aws

Then update configurations in `conf/ec2-config.yml` and shell environment variables `AWS_ACCESS_KEY`
and `AWS_SECRET_KEY`.

Run `./run_aws.sh` to create EC2 VPC instances.

## OpenStack Provider

Install openstack vagrant plugin first. To date, 0.8.0 plugin is tested.

    vagrant plugin install vagrant-openstack-plugin

Then update configurations in `conf/openstack-config.yml` and shell environment variables
`OS_USERNAME` and `OS_PASSWORD`.

Run `run_openstack.sh` to create OpenStack Compute Node instances.

## Examples of Running VirtualBox Clusters Using Glusterfs as Underfilesystem

A sample `conf/init.yml.glusterfs` is provided. Copy or link it to `init.yml`. Make sure parameter
`Ufs` is `glusterfs` and `Provider` is `vb`. Change the rest of parameters to what you want if
necessary.

Then start the clusters.

    vagrant up --provider=virtualbox

## Examples of Running AWS Clusters Using HDFS 2.4 as Underfilesystem

A sample `conf/init.yml.aws` is provided. Copy or link it to `init.yml`. Make sure parameter `Ufs`
is `hadoop2` and `Provider` is `aws`. Change the rest of parameters, especially network addresses,
to what you want if necessary.

Then start the clusters.

    ./run_aws.sh


## Examples of Running OpenStack Compute Node Clusters Using HDFS 2.4 as Underfilesystem

A sample `conf/init.yml.openstack` is provided. Copy or link it to `init.yml`. Make sure parameter
`Ufs` is `hadoop2` and `Provider` is `openstack`. The `Addresses` are currently not used.

Then start the clusters.

    ./run_openstack.sh


## Use Tachyon Cluster

Once clusters are up running, tachyon is installed and configured. The tachyon source directory is
mapped to `/tachyon` directory on each image. Editions are visible on the images.

## Destroy Tachyon Cluster

To stop and destroy the images, run command

    vagrant destroy [-f]