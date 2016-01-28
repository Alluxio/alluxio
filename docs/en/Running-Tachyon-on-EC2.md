---
layout: global
title: Running Tachyon on EC2
nickname: Tachyon on EC2
group: User Guide
priority: 3
---

Tachyon can be deployed on Amazon EC2 using the
[Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant) that come with
Tachyon. The scripts let you create, configure, and destroy clusters that come automatically
configured with HDFS.

# Prerequisites

**Install Vagrant and the AWS plugins**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install AWS Vagrant plugin:

```bash
$ vagrant plugin install vagrant-aws
$ vagrant box add dummy https://github.com/mitchellh/vagrant-aws/raw/master/dummy.box
```

**Install Tachyon**

Download Tachyon to your local machine, and unzip it:

```bash
$ wget http://tachyon-project.org/downloads/files/{{site.TACHYON_RELEASED_VERSION}}/tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
$ tar xvfz tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
```

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your home directory, run:

```bash
$ sudo bash bin/install.sh
```

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then
in `deploy/vagrant` run:

```bash
$ sudo pip install -r pip-req.txt
```

# Launch a Cluster

To run a Tachyon cluster on EC2, first sign up for an Amazon EC2 account
on the [Amazon Web Services site](http://aws.amazon.com/).

If you are not familiar with Amazon EC2, you can read [this tutorial](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html) first.

Then create [access keys](https://aws.amazon.com/developers/access-keys/) and set shell environment
variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` by:

```bash
$ export AWS_ACCESS_KEY_ID=<your access key>
$ export AWS_SECRET_ACCESS_KEY=<your secret access key>
```

Next generate your EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). Make sure to set
the permissions of your private key file that only you can read it:

```bash
$ chmod 400 <your key pair>.pem
```

Copy `deploy/vagrant/conf/ec2.yml.template` to `deploy/vagrant/conf/ec2.yml` by:

```bash
$ cp deploy/vagrant/conf/ec2.yml.template deploy/vagrant/conf/ec2.yml
```

In the configuration file `deploy/vagrant/conf/ec2.yml`, set the value of `Keypair` to your keypair
name and `Key_Path` to the path to the pem key.

By default, the Vagrant script creates a
[Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
named *tachyon-vagrant-test* at
[Region(**us-east-1**) and Availability Zone(**us-east-1a**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).
The security group will be set up automatically in the region with all inbound/outbound network
traffic opened. You can change the *security group*, *region* and *availability zone* in `ec2.yml`. Sometimes the default zone can be unavailable.
Note: the keypair is associated with a specific region. For example, if you created a keypair in us-east-1, the keypair is invalid in other regions (like us-west-1).  If you ran into permission/connection errors, please first check the region/zone.

**Spot instances**

Using spot instances is a way to reduce EC2 cost. Spot instances are non-guaranteed instances which are priced with bidding.
Note that spot instances may be taken away from you if someone bids more, and there are no more spot instances available.
However, for short-term testing, spot instances are very appropriate, because it is rare that spot instances are taken from you.

By default, the deploy scripts DO NOT use spot instances. Therefore, you have to enable deploy scripts to use spot instances.

In order to enable spot instances, you have to modify the file: `deploy/vagrant/conf/ec2.yml`:

    Spot_Price: “X.XX”

Now you can launch the Tachyon cluster with Hadoop2.4.1 as under filesystem in us-east-1a by running
the script under `deploy/vagrant`:

```bash
$ ./create <number of machines> aws
```

Each node of the cluster runs a Tachyon worker, and the `TachyonMaster` runs the Tachyon master.

# Access the cluster

**Access through Web UI**

After the command `./create <number of machines> aws` succeeds, you can see two green lines like
below shown at the end of the shell output:

    >>> TachyonMaster public IP is xxx, visit xxx:19999 for Tachyon web UI<<<
    >>> visit default port of the web UI of what you deployed <<<

Default port for Tachyon Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

You can also monitor the instances state through
[AWS web console](https://console.aws.amazon.com/console).
Make sure that you are in the console for the region where you started the cluster.

Here are some scenarios when you may want to check the console:
 - When the cluster creation fails, check EC2 instances status/logs.
 - After the cluster is destroyed, confirm EC2 instances are terminated.
 - When you no longer need the cluster, make sure EC2 instances are NOT costing you extra money.

**Access with ssh**

The nodes set up are named to `TachyonMaster`, `TachyonWorker1`, `TachyonWorker2` and so on.

To ssh into a node, run:

```bash
$ vagrant ssh <node name>
```

For example, you can ssh into `TachyonMaster` with:

```bash
$ vagrant ssh TachyonMaster
```

All software is installed under the root directory, e.g. Tachyon is installed in `/tachyon`,
and Hadoop is installed in `/hadoop`.

On the `TachyonMaster` node, you can run tests against Tachyon to check its health:

```bash
$ /tachyon/bin/tachyon runTests
```

After the tests finish, visit Tachyon web UI at `http://{MASTER_IP}:19999` again. Click `Browse
File System` in the navigation bar, and you should see the files written to Tachyon by the above
tests.

From a node in the cluster, you can ssh to other nodes in the cluster without password with:

```bash
$ ssh TachyonWorker1
```

# Destroy the cluster

Under `deploy/vagrant` directory, you can run:

```bash
$ ./destroy
```

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the EC2 instances are terminated.
