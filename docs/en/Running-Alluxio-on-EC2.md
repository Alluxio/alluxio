---
layout: global
title: Running Alluxio on EC2
nickname: Alluxio on EC2
group: User Guide
priority: 3
---

Alluxio can be deployed on Amazon EC2 using the
[Vagrant scripts](https://github.com/amplab/alluxio/tree/master/deploy/vagrant) that come with
Alluxio. The scripts let you create, configure, and destroy clusters that come automatically
configured with [Amazon S3](https://s3.amazonaws.com/).

# Prerequisites

**Install Vagrant and the AWS plugins**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install AWS Vagrant plugin:

{% include Running-Alluxio-on-EC2/install-aws-vagrant-plugin.md %}

**Install Alluxio**

Download Alluxio to your local machine, and unzip it:

{% include Common-Commands/download-alluxio.md %}

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your home directory, run:

{% include Running-Alluxio-on-EC2/install-vagrant.md %}

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then
in `deploy/vagrant` run:

{% include Running-Alluxio-on-EC2/install-pip.md %}

# Launch a Cluster

To run a Alluxio cluster on EC2, first sign up for an Amazon EC2 account
on the [Amazon Web Services site](http://aws.amazon.com/).

If you are not familiar with Amazon EC2, you can read [this tutorial](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html) first.

Then create [access keys](https://aws.amazon.com/developers/access-keys/) and set shell environment
variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` by:

{% include Running-Alluxio-on-EC2/access-key.md %}

Next generate your EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). Make sure to set
the permissions of your private key file that only you can read it:

{% include Running-Alluxio-on-EC2/generate-key-pair.md %}

Copy `deploy/vagrant/conf/ec2.yml.template` to `deploy/vagrant/conf/ec2.yml` by:

{% include Running-Alluxio-on-EC2/copy-ec2.md %}

In the configuration file `deploy/vagrant/conf/ec2.yml`, set the value of `Keypair` to your keypair
name and `Key_Path` to the path to the pem key.

By default, the Vagrant script creates a
[Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
named *alluxio-vagrant-test* at
[Region(**us-east-1**) and Availability Zone(**us-east-1b**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).
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

For AWS EC2, the default underfs is S3. You need to sign into your [Amazon S3 console](http://aws.amazon.com/s3/), create a S3 bucket and write the bucket's name to the field `S3:Bucket` in `conf/ufs.yml`. To use other under storage systems, configure the field `Type` and the corresponding configurations in `conf/ufs.yml`.

Now you can launch the Alluxio cluster with your chosen under filesystem in your chosen availability zone by running
the script under `deploy/vagrant`:

{% include Running-Alluxio-on-EC2/launch-cluster.md %}

Each node of the cluster runs a Alluxio worker, and the `AlluxioMaster` runs the Alluxio master.

# Access the cluster

**Access through Web UI**

After the command `./create <number of machines> aws` succeeds, you can see two green lines like
below shown at the end of the shell output:

{% include Running-Alluxio-on-EC2/shell-output.md %}

Default port for Alluxio Web UI is **19999**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

You can also monitor the instances state through
[AWS web console](https://console.aws.amazon.com/console).
Make sure that you are in the console for the region where you started the cluster.

Here are some scenarios when you may want to check the console:
 - When the cluster creation fails, check EC2 instances status/logs.
 - After the cluster is destroyed, confirm EC2 instances are terminated.
 - When you no longer need the cluster, make sure EC2 instances are NOT costing you extra money.

**Access with ssh**

The nodes set up are named to `AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2` and so on.

To ssh into a node, run:

{% include Running-Alluxio-on-EC2/ssh.md %}

For example, you can ssh into `AlluxioMaster` with:

{% include Running-Alluxio-on-EC2/ssh-AlluxioMaster.md %}

All software is installed under the root directory, e.g. Alluxio is installed in `/alluxio`.

On the `AlluxioMaster` node, you can run tests against Alluxio to check its health:

{% include Running-Alluxio-on-EC2/runTests.md %}

After the tests finish, visit Alluxio web UI at `http://{MASTER_IP}:19999` again. Click `Browse
File System` in the navigation bar, and you should see the files written to Alluxio by the above
tests.

You can login to [AWS web console](https://console.aws.amazon.com/console), then go to your S3 console,
and find some files written into your S3 bucket by the above tests.

From a node in the cluster, you can ssh to other nodes in the cluster without password with:

{% include Running-Alluxio-on-EC2/ssh-other-node.md %}

# Destroy the cluster

Under `deploy/vagrant` directory, you can run:

{% include Running-Alluxio-on-EC2/destroy.md %}

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the EC2 instances are terminated.
