---
layout: global
title: Running Tachyon on Linux Container
---

## Deploy Tachyon Cluster on Linux Container via Vagrant

[Vagrant](https://www.vagrantup.com/downloads.html) can spawn Tachyon cluster locally inside [Linux container](http://www.docker.com/), and provision all nodes in parallel with [Ansible](http://www.ansible.com/home).

A set of pre-configured Vagrant recipe and shell scripts can be found at `tachyon/deploy/vagrant`
directory:

<pre>
vagrant
├── conf
│   ├── ec2-config.yml
│   ├── init.yml
│   ├── spark_version.yml
│   ├── tachyon_version.yml
│   ├── ufs_version.yml
│   └── openstack-config.yml
├── README.md
├── run_docker.sh
└── ...
</pre>

The `run_docker.sh` script allows you to launch a Tachyon inside containers. It reads Tachyon cluster configuration from `tachyon/deploy/vagrant/conf/init.yml`, then the script automatically creates the cluster, sets up under filesystem, and starts Tachyon master and workers on the cluster for you.

## Prerequisite
* **Install Vagrant**. You can download and install [Vagrant](https://www.vagrantup.com/downloads.html). Version 1.6.5 and higher is required

* **On Linux host, install Docker package**. 

* **Install Ansible**. [Installation Guide](http://docs.ansible.com/intro_installation.html#installation). Version 1.5 and higher is recommended for ssh speedup.

## Configure

Configure version of softwares you want to deploy, refer to [this doc](Running-Specific-Version-Of-Tachyon-Or-Spark-Via-Vagrant.html)

Provide your desired Tachyon cluster configuration. Edit `conf/init.yml`. The parameters in `tachyon/deploy/vagrant/conf/init.yml` are explained as the following.

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

    $ ./run_docker.sh

A successful deployment will end up with messages showing Tachyon master and workers are launched.

## Access Cluster

You can access your cluster nodes by `vagrant ssh` command. `vagrant ssh TachyonMaster` connects you to the node that acts as the TachyonMaster. If you have multiple worker nodes, `vagrant ssh TachyonWorker1` connects to node TachyonWorker1.

## Destroy Cluster

Run command to destroy the cluster

    $ vagrant destroy [-f]
