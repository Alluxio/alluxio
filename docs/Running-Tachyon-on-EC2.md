---
layout: global
title: Running Tachyon on EC2
nickname: Tachyon on EC2
group: User Guide
priority: 3
---

Tachyon can be deployed on EC2 using the [Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant) that come with Tachyon. The scripts let you create, configure and destroy clusters that come automatically configured with HDFS.

# Prerequisites

**Install Vagrant and the AWS plugins**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install AWS Vagrant plugin:

    vagrant plugin install vagrant-aws
    vagrant box add dummy https://github.com/mitchellh/vagrant-aws/raw/master/dummy.box

**Install Tachyon**

Download Tachyon to your local machine, and unzip it:

    $ wget https://github.com/amplab/tachyon/releases/download/v0.7.1/tachyon-0.7.1-bin.tar.gz
    $ tar xvfz tachyon-0.7.1-bin.tar.gz

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your home directory, run:

    $ sudo bash bin/install.sh

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then in `deploy/vagrant` run:

    $ sudo pip install -r pip-req.txt

# Launch a Cluster

To run a Tachyon cluster on EC2, first sign up for an Amazon EC2 account
on the [Amazon Web Services site](http://aws.amazon.com/). 

Then create [access keys](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html) 
and set shell environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` by:

    $ export AWS_ACCESS_KEY_ID=<your access key>
    $ export AWS_SECRET_ACCESS_KEY=<your secret access key>

Next generate your EC2 [Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). Make sure to set the permissions of your private key file that only you can read it:

    $ chmod 400 <your key pair>.pem

In the configuration file `deploy/vagrant/conf/ec2.yml`, set the value of `Keypair` to your keypair name and `Key_Path` to the path to the pem key.

By default, the Vagrant script creates a [Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html) named *tachyon-vagrant-test* at [Region(**us-east-1**) and Availability Zone(**us-east-1a**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html). The security group will be set up automatically in the region with all inbound/outbound network traffic opened. You can change the security group, region and availability zone in `ec2.yml`.

Now you can launch the Tachyon cluster with Hadoop2.4.1 as under filesystem in us-east-1a by running the script under `deploy/vagrant`:

    ./create <number of machines> aws

# Access the cluster

**Access through Web UI**

After command `./create <number of machines> aws` succeeds, you can see two green lines like below shown at the end of the shell output:

    >>> TachyonMaster public IP is xxx <<<
    >>> visit default port of the web UI of what you deployed <<<

Default port for Tachyon Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

You can also monitor the instances state through [AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1).

**Access with ssh**

The nodes set up are named to `TachyonMaster`, `TachyonWorker1`, `TachyonWorker2` and so on.

To ssh into a node, run

    $ vagrant ssh <node name>

For example, you can ssh into `TachyonMaster` with

    $ vagrant ssh TachyonMaster

All software are installed under root directory, e.g. Tachyon is installed in /tachyon, Hadoop is installed in /hadoop.

You can run some tests against Tachyon to check its health:

    $ /tachyon/bin/tachyon runTests

After the tests all pass, visit Tachyon web UI at `http://{MASTER_IP}:19999` again. Click `Browse FileSystem` in the navigation bar, and you should see the files written to Tachyon by the above tests.

Similarly, you can ssh to a worker node without password like

    $ ssh TachyonWorker1

# Destroy the cluster

Under `deploy/vagrant` directory, you can run
    
    $ ./destroy

to destroy the cluster that you created. Only one cluster can be created at a time. After the command succeeds, the EC2 instances are terminated.
