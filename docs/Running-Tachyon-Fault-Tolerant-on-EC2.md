---
layout: global
title: Running Tachyon with Fault Tolerance on EC2
nickname: Tachyon on EC2 with Fault Tolerance
group: User Guide
priority: 4
---

Tachyon with Fault Tolerance can be deployed on Amazon EC2 using the
[Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant) that come with
Tachyon. The scripts let you create, configure, and destroy clusters that come automatically
configured with Apache HDFS.

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

Then create [access keys](https://aws.amazon.com/developers/access-keys/)
and set shell environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` by:

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

In the configuration file `deploy/vagrant/conf/tachyon.yml`, set the value of `Masters` to the
number of TachyonMasters you want. In fault tolerant mode, value of `Masters` should be larger than
1.

By default, the Vagrant script creates a
[Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
named *tachyon-vagrant-test* at
[Region(**us-east-1**) and Availability Zone(**us-east-1a**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).
The security group will be set up automatically in the region with all inbound/outbound network
traffic opened. You can change the *security group*, *region* and *availability zone* in `ec2.yml`.

Now you can launch the Tachyon cluster with Hadoop2.4.1 as under filesystem in us-east-1a by running
the script under `deploy/vagrant`:

```bash
$ ./create <number of machines> aws
```

Note that the `<number of machines>` above should be larger than or equal to `Masters` set in
`deploy/vagrant/conf/tachyon.yml`.

Each node of the cluster has a Tachyon worker and each master node has a Tachyon master. The leader
is in one of the master nodes.

# Access the cluster

**Access through Web UI**

After command `./create <number of machines> aws` succeeds, you can see three green lines like below
shown at the end of the shell output:

    >>> Master public IP for Tachyon is xxx, visit xxx:19999 for Tachyon web UI<<<
    >>> Master public IP for other softwares is xxx <<<
    >>> visit default port of the web UI of what you deployed <<<

The first line shows public IP for current leader of all Tachyon masters.

The second line shows public IP for master of other softwares like Hadoop.

Default port for Tachyon Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

You can also monitor the instances state through
[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1).

**Access with ssh**

The nodes created are placed in one of two categories.

One category contains `TachyonMaster`, `TachyonMaster2` and so on, representing all Tachyon masters;
one of them is the leader, and the others are standbys. `TachyonMaster` is also the master for other
software, like Hadoop. Each node also runs workers for Tachyon and other software like Hadoop.

Another group contains `TachyonWorker1`, `TachyonWorker2` and so on. Each node runs workers
for Tachyon and other software like Hadoop.

To ssh into a node, run:

```bash
$ vagrant ssh <node name>
```

For example, you can ssh into `TachyonMaster` with:

```bash
$ vagrant ssh TachyonMaster
```

All software is installed under the root directory, e.g. Tachyon is installed in `/tachyon`,
Hadoop is installed in `/hadoop`, and Zookeeper is installed in `/zookeeper`.

On the leader master node, you can run tests against Tachyon to check its health:

```bash
$ /tachyon/bin/tachyon runTests
```

After the tests finish, visit Tachyon web UI at `http://{MASTER_IP}:19999` again. Click
`Browse File System` in the navigation bar, and you should see the files written to Tachyon by the
above tests.

You can ssh into the current Tachyon master leader, and find process ID of the TachyonMaster
process with:

```bash
$ jps | grep TachyonMaster
```

Then kill the leader with:

```bash
$ kill -9 <leader pid found via the above command>
```

Then you can ssh into `TachyonMaster` where [zookeeper](http://zookeeper.apache.org/) is
running to find out the new leader, and run the zookeeper client via:

```bash
$ /zookeeper/bin/zkCli.sh
```

In the zookeeper client shell, you can see the leader with the command:

```bash
$ ls /leader
```

The output of the command should show the new leader. You may need to wait for a moment for the
new leader to be elected. You can query the public IP for the new leader based on its name in
[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1).

Visit Tachyon web UI at `http://{NEW_LEADER_MASTER_IP}:19999`. Click `Browse File System` in the
navigation bar, and you should see all files are still there.

From a node in the cluster, you can ssh to other nodes in the cluster without password with:

```bash
$ ssh TachyonWorker1
```

# Destroy the cluster

Under `deploy/vagrant` directory, you can run

```bash
$ ./destroy
```

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the EC2 instances are terminated.
