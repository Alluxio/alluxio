---
layout: global
title: Running Tachyon on OpenStack Compute
---

## Deploy Tachyon Cluster on OpenStack Compute via Vagrant

[Vagrant](https://www.vagrantup.com/downloads.html) can spawn Tachyon cluster in the cloud at [OpenStack Compute](http://www.openstack.org/software/openstack-compute/).

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
├── run_openstack.sh
└── ...
</pre>

The `run_openstack.sh` script allows you to launch a Tachyon cluster on OpenStack Compute nodes. It reads OpenStack configurations from `tachyon/deploy/vagrant/conf/openstack-config.yml`, your shell environmen variables `OS_USERNAME` and `OS_PASSWORD`, and Tachyon cluster configuration from `tachyon/deploy/vagrant/init.yml`, then the script automatically creates the cluster, sets up under filesystem, and starts Tachyon master and workers on the cluster for you.

`run_openstack.sh` is designed to create multiple clusters. Node of each cluster is tagged so multiple people can use their own Tachyon cluster with the same credential and tenant.

## Prerequisite
* **Install Vagrant**. You can download and install [Vagrant](https://www.vagrantup.com/downloads.html). Version 1.6.5 and higher is required. 

* **Install Vagrant OpenStack plugin**. Install [openstack vagrant plugin](https://github.com/cloudbau/vagrant-openstack-plugin) first. To date, 0.8.0 plugin is tested. 

* **Obtain OpenStack key pair**. This is your Compute node login keypair. 

* **Create OpenStack Network Secruity Group**. Security Group determines if network access is allowed. Your Security Group should permit `ssh` access. 


## Configure

With the above information, fill them in `tachyon/deploy/vagrant/conf/openstack-config.yml`. Also ensure your OpenStack login credentials are set in your shell environmen variables `OS_USERNAME` and `OS_PASSWORD`.

Next, you provide your desired Tachyon cluster configuration. A sample configuration can be found in `tachyon/deploy/vagrant/conf/init.yml.openstack`. Link or copy it to `tachyon/deploy/vagrant/init.yml`. Since the Compute nodes use DHCP, the Addresses are not used.

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

    $ ./run_openstack.sh

A successful deployment will end up with messages showing Tachyon master and workers are launched.

## Access Cluster

You can access your cluster nodes by `vagrant ssh` command. `vagrant ssh TachyonMaster` connects you to the node that acts as the TachyonMaster. If you have multiple worker nodes, `vagrant ssh TachyonWorker1` connects to node TachyonWorker1.

## Destroy Cluster

Run command to destroy the cluster

    $ vagrant destroy [-f]
