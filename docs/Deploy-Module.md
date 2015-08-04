---
layout: global
title: Deploy Module
---

## Summary

In Tachyon source tree, [`deploy/vagrant`](https://github.com/amplab/tachyon/blob/master/deploy/vagrant) directory contains utilities to help you set up a Tachyon cluster on AWS EC2 or virtualbox.

Apart from Tachyon, you can choose different computation frameworks and under filesystems from the following list. 

* Computation Framework
  * Spark
* Under Filesystem
  * Hadoop1
  * Hadoop2 (both Apache and CDH)
  * GlusterFS
  * AWS S3

New frameworks and filesystems will be added, please see [**Extension** section in `deploy/vagrant/README.md`](https://github.com/amplab/tachyon/blob/master/deploy/vagrant/README.md#extension), welcome contribution!

## Prerequisites

[Download vagrant](https://www.vagrantup.com/downloads.html).

If you want to deploy on desktop, [download virtualbox](https://www.virtualbox.org/wiki/Downloads).

Install [python >= 2.7, not python3](http://python.org/).

Under `deploy/vagrant` directory in your Tachyon repo, run the following to install dependent python libs:

    sudo bash bin/install.sh

If anything goes wrong, install [pip](https://pip.pypa.io/en/latest/installing.html) on your own, then in the same directory, run(may need sudo):

    pip install -r pip-req.txt

## Quick Start With VirtualBox

Under directory `deploy/vagrant`, run

    ./create 2 vb

A two node Tachyon cluster with Hadoop2.4.1 as under filesystem will be set up. 

It will be slow when you run this for the first time because 
a virtualbox image with necessary software installed will be created and saved for future use.

## Quick Start With AWS EC2

**Optional:** Set up [AWS CLI](http://docs.aws.amazon.com/AWSEC2/latest/CommandLineReference/ec2-cli-get-set-up.html) may be helpful.

[Sign up](https://aws.amazon.com/) AWS account with access to EC2.

Set shell environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, 
refer to [this doc](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html) for more detail.

Download [key pair](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). Be sure to chmod private ssh key to **0600**.

Set name of keypair to **Keypair** and path to the pem key to **Key_Path** in `conf/ec2.yml`.

Install aws vagrant plugin(To date, version 0.5.0 is tested):

    vagrant plugin install vagrant-aws
    vagrant box add dummy https://github.com/mitchellh/vagrant-aws/raw/master/dummy.box

By default, if you don't have a 
[Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html) 
named **tachyon-vagrant-test** in 
[Region(**us-east-1**) and Availability Zone(**us-east-1a**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html), 
the security group will be set up automatically in the region with all inbound/outbound network traffic opened. 
If you know what you are doing, you can change security group, region and availability zone in configuration file `conf/ec2.yml`, 
otherwise, you do not need to configure these values, the default will be fine.

A Tachyon cluster with Hadoop2.4.1 as under filesystem will be set up in us-east-1a by running 

    ./create <number of machines> aws


You can monitor instances' running state through [AWS web console](https://console.aws.amazon.com).

## After Cluster Set Up

When command `./create <number of machines> aws` succeeds, two purple lines like below will be shown in shell output:

    >>> TachyonMaster public IP is xxx <<<
    >>> visit default port of the web UI of what you deployed <<<

Default port for Tachyon web UI is **19999**.

Default port for Hadoop web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in your browser, you can see those web UIs!

The nodes set up are named to `TachyonMaster`, `TachyonWorker1`, `TachyonWorker2` and so on.

To ssh into a node, run

    vagrant ssh {NODE_NAME}

Let's ssh into TachyonMaster: 

    vagrant ssh TachyonMaster 
    
You should have entered TachyonMaster node, all software is installed under root directory, 
e.x. Tachyon is installed in `/tachyon`, Hadoop is installed in `/hadoop`.

Let's run some tests against Tachyon:

    /tachyon/bin/tachyon runTests

After the tests all pass, visit Tachyon web UI: `http://{MASTER_IP}:19999` again, 
in navigation bar `Browse FileSystem`, you should see files written to Tachyon by the above tests.

Util scripts are provided under directory `~/vagrant-utils` on TachyonMaster node.

Run

    ~/vagrant-utils/copy-dir <path>

to distribute the file or directory represented by `path` on TachyonMaster node to all worker nodes. 
For example, after changing some configurations on TachyonMaster node, you can use this script to update the configuration on all worker nodes.

Run

    ~/vagrant-utils/copy-dir --delete <path>

to delete the file or directory represented by `path` in all worker nodes.

Run

    ~/vagrant-utils/remount <source directory> <destination directory>

to unmount a device from current mount point `<source directory>` and mount to another point `<destination directory>` in all nodes.
  
In TachyonMaster node, you can ssh to any node without password like

    ssh TachyonWorker1

Under `deploy/vagrant` directory on your local host, run

    ./destroy

to destroy the cluster you just created, only one cluster can be created each time. 
After the command succeeds, for virtualbox, virtual machines will be deleted, for EC2, instances will be terminated.

## Configurations

There are two kinds of configuration files under `conf`:

1. configuration for target deploy platform, like `vb.yml` for virtualbox, `ec2.yml` for AWS EC2.
2. software configuration, like `tachyon.yml`, `spark.yml`, `ufs.yml`(ufs means under filesystem). 

All the configuration files use [yaml syntax](http://en.wikipedia.org/wiki/YAML).

Meanings of the configurations are explained in detail in the configuration files themselves.

If you are new to AWS EC2, we recommend you to understand comments in `ec2.yml`.

## FAQ

#### How to increase virtualbox virtual machine's memory size?

Increase **MachineMemory** in **conf/vb.yml**.

#### How to increase Tachyon worker's memory?

Increase **WorkerMemory** in **conf/tachyon.yml**.

#### How to increase HDFS capacity in AWS?

Specify [block device mapping](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/block-device-mapping-concepts.html).

Please refer to comments on **Block_Device_Mapping** in **conf/ec2.yml**.

#### How to use EBS in AWS?

Specify [block device mapping](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/block-device-mapping-concepts.html).

Please refer to comments on **Block_Device_Mapping** in **conf/ec2.yml**.

#### What kind of EC2 instance should I use?

Please choose from [instance types](http://aws.amazon.com/ec2/instance-types/) to best fit your requirements.

#### The EC2 instance should have a 60GB SSD disk, but `df -h` says the root is only 10GB, where is the 60GB SSD?

The extra SSD disk won't be used unless you specify it via [block device mapping](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/block-device-mapping-concepts.html).

Please refer to comments on **Block_Device_Mapping** in **conf/ec2.yml**.

#### How to increase root device's space?

First, migrate data in current root device to a larger EBS volume and attach the larger volume as root device, 
refer to [how to expand ebs volume](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-expand-volume.html).

Second, repartition the new root device to use the whole disk, 
refer to [how to expand partition](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage_expand_partition.html).

#### What's PV/HVM?

Please refer to [official AWS documentation](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/virtualization_types.html).
