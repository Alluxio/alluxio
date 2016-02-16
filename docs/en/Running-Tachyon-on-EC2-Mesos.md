---
layout: global
title: Running Tachyon with Mesos on EC2
nickname: Tachyon on EC2 with Mesos
group: User Guide
priority: 4
---

Tachyon can be deployed on Amazon EC2 using Apache Mesos, which is in turn deployed on EC2 using the
[Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant) that come with
Tachyon. The scripts let you create, configure, and destroy clusters that come automatically
configured with HDFS.

# Prerequisites

**Install Vagrant and the AWS plugins**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install AWS Vagrant plugin:

{% include Running-Tachyon-on-EC2-Mesos/install-aws-vagrant-plugin.md %}

**Install Tachyon**

Download Tachyon to your local machine, and unzip it:

{% include Running-Tachyon-on-EC2-Mesos/download-tachyon.md %}

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your home directory, run:

{% include Running-Tachyon-on-EC2-Mesos/install-vagrant.md %}

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then
in `deploy/vagrant` run:

{% include Running-Tachyon-on-EC2-Mesos/install-pip.md %}

# Launch a Cluster

To run a Tachyon cluster on EC2, first sign up for an Amazon EC2 account
on the [Amazon Web Services site](http://aws.amazon.com/).

Then create [access keys](https://aws.amazon.com/developers/access-keys/)and set shell environment
variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` by:

{% include Running-Tachyon-on-EC2-Mesos/access-key.md %}

Next generate your EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). Make sure to set
the permissions of your private key file that only you can read it:

{% include Running-Tachyon-on-EC2-Mesos/generate-key-pair.md %}

In the configuration file `deploy/vagrant/conf/ec2.yml`, set the value of `Keypair` to your keypair
name and `Key_Path` to the path to the pem key.

By default, the Vagrant script creates a
[Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
named *tachyon-vagrant-test* at
[Region(**us-east-1**) and Availability Zone(**us-east-1a**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).
The security group will be set up automatically in the region with all inbound/outbound network
traffic opened. You can change the security group, region and availability zone in `ec2.yml`.

In the configuration file `deploy/vagrant/conf/mesos.yml`, set the value of `Type` to either
`Github` or `Release` dependening on whether you would like to build Mesos from a GitHub branch or a
release.

Now you can launch the Mesos cluster and Tachyon Mesos Framework which in turn launches a Tachyon
cluster with Hadoop version 2.4.1 as under filesystem in us-east-1a by running the script under
`deploy/vagrant`:

{% include Running-Tachyon-on-EC2-Mesos/launch-cluster.md %}

# Access the cluster

**Access through Web UI**

After command `./create <number of machines> aws` succeeds, you can see two green lines like below
shown at the end of the shell output:

{% include Running-Tachyon-on-EC2-Mesos/shell-output.md %}

Default port for Tachyon Web UI is **19999**.

Default port for Mesos Web UI is **50050**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

NOTE: Tachyon Mesos framework does not guarantee to start the Tachyon master service on the 
TachyonMaster machine; use the Mesos Web UI to find out which machine is the Tachyon master 
service running on.

You can also monitor the instances state through [AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1).

**Access with ssh**

The nodes set up are named to `TachyonMaster`, `TachyonWorker1`, `TachyonWorker2` and so on.

To ssh into a node, run:

{% include Running-Tachyon-on-EC2-Mesos/ssh.md %}

For example, you can ssh into `TachyonMaster` with:

{% include Running-Tachyon-on-EC2-Mesos/ssh-TachyonMaster.md %}

All software is installed under root directory, e.g. Tachyon is installed in `/tachyon`, Hadoop is
installed in `/hadoop`, Mesos is installed in `/mesos`.

You can run tests against Tachyon to check its health:

{% include Running-Tachyon-on-EC2-Mesos/runTests.md %}

After the tests finish, visit Tachyon web UI at `http://{MASTER_IP}:19999` again. Click `Browse
File System` in the navigation bar, and you should see the files written to Tachyon by the above
tests.

# Destroy the cluster

Under `deploy/vagrant` directory, you can run:

{% include Running-Tachyon-on-EC2-Mesos/destroy.md %}

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the EC2 instances are terminated.
