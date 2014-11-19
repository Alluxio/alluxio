---
layout: global
title: Running Tachyon on Amazon EC2
---

## Deploy Tachyon Cluster on Amazon EC2 via Vagrant

[Vagrant](https://www.vagrantup.com/downloads.html) can spawn Tachyon cluster in the cloud at [AWS EC2 VPC](http://aws.amazon.com/vpc/).

A set of pre-configured Vagrant recipe and shell scripts can be found at `tachyon/deploy/vagrant`
directory:

<pre>
vagrant
├── conf
│   ├── ec2-config.yml
│   ├── init.yml.aws
│   ├── init.yml.glusterfs
│   ├── init.yml.hdfs1
│   ├── init.yml.hdfs2
│   ├── init.yml.os
│   ├── init.yml.docker
│   └── openstack-config.yml
├── init.yml -> conf/init.yml.hdfs2
├── README.md
├── run_aws.sh
└── ...
</pre>

The `run_aws.sh` script allows you to launch a Tachyon VPC on Amazon EC2. It reads `ec2` configurations from `tachyon/deploy/vagrant/conf/ec2-config.yml`, your shell environmen variables `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`, and Tachyon cluster configuration from `tachyon/deploy/vagrant/init.yml`, then the script automatically creates the cluster, sets up under filesystem, and starts Tachyon master and workers on the cluster for you.

`run_aws.sh` is designed to create multiple clusters. Node of each cluster is tagged so multiple people can use their own Tachyon cluster with the same EC2 account.

## Prerequisite
* **Install Vagrant**. You can download and install [Vagrant](https://www.vagrantup.com/downloads.html). Version 1.6.5 and higher is required

* **Obtain EC2 key pair**. This is your EC2 login keypair. You can find information at http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html

* **Create EC2 Network Secruity Group**. Security Group determines if network access is allowed. Your Security Group should permit `ssh` access. You can find information at http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html

* **Choose EC2 Availability Zone**. You need to provide a `Availability Zone` for your VPC's network address range.

## Configure

With the above EC2 information, fill them in `tachyon/deploy/vagrant/conf/ec2-config.yml`. Also ensure your shell environmen variables `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` are set correctly.

Next, you provide your desired Tachyon cluster configuration. A sample configuration can be found in `tachyon/deploy/vagrant/conf/init.yml.aws`. Link or copy it to `tachyon/deploy/vagrant/init.yml`. Make sure the IP addresses are in the range of your `Availability Zone` and are not used by others.

The parameters in `tachyon/deploy/vagrant/init.yml` are explained as the following.

<table class="table">
<tr>
    <th>Parameter</th><th>Description</th><th>Values</th>
</tr>
<tr>
    <td>Ufs</td><td>Tachyon Underfilesystem</td><td>glusterfs|hadoop2|hadoop1</td>
</tr>
<tr>
    <td>Provider</td><td>Vagrant Providers</td><td>vb|aws|openstack|docker</td>
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
For Docker provider, containers use DHCP, these addresses are not used.
</td><td>IPv4 address string</td>
</tr>
</table>

## Launch Cluster

Run command to launch your Tachyon cluster

    $ ./run_aws.sh

A successful deployment will end up with messages showing Tachyon master and workers are launched.

## Access Cluster

You can access your cluster nodes by `vagrant ssh` command. `vagrant ssh TachyonMaster` connects you to the node that acts as the TachyonMaster. If you have multiple worker nodes, `vagrant ssh TachyonWorker1` connects to node TachyonWorker1.

## Destroy Cluster

Run command to destroy the cluster

    $ vagrant destroy [-f]
