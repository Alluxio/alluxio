---
layout: global
title: Deploy Module
---

## Summary

In Tachyon source tree, `deploy/vagrant` directory contains utilities to help you set up a Tachyon cluster within 10min, it can be deployed on AWS EC2 or virtualbox.

Apart from Tachyon, you can choose to deploy computation frameworks and under filesystems from the following list. New frameworks and filesystems will be added, please see **Extension** section in `deploy/vagrant/README.md`, welcome contribution!

* Computation Framework
  * Spark
* Under Filesystem
  * Hadoop1
  * Hadoop2
  * GlusterFS
  * AWS S3

## Prerequisites

1. [download vagrant](https://www.vagrantup.com/downloads.html)
2. if want to deploy on desktop, [download virtualbox](https://www.virtualbox.org/wiki/Downloads)
3. [python](http://python.org/)
4. Under `deploy/vagrant` directory in your Tachyon repo, run `sudo bash bin/install.sh`. If anything goes wrong, install [pip](https://pip.pypa.io/en/latest/installing.html) on your own, then in the same directory, run `[sudo] pip install -r pip-req.txt`. This will install dependent python libs.

## Quick Start With VirtualBox

After installing the prerequisites, if have virtualbox installed and the memory capacity of your host is larger than 4G, you can follow this quick start to set up a two node Tachyon cluster with apache hadoop2.4.1 as under filesystem, one node is named TachyonMaster, another is TachyonWorker.

1. `cd TACHYON/deploy/vagrant`, replace TACHYON with the Tachyon repo on your host, you can clone the latest from [github](https://github.com/amplab/tachyon.git)
2. `./create 2 vb`, it will be slow when you first run this command, because vagrant will download a centos6.5 virtual machine, and install necessary software in this machine. The console output is verbose enough for you to know what's going on. 

## Quick Start With AWS EC2

**Optional** Set up [AWS CLI](http://docs.aws.amazon.com/AWSEC2/latest/CommandLineReference/ec2-cli-get-set-up.html) may be helpful.

1. [Sign up](https://aws.amazon.com/) AWS account with access to EC2
2. Set shell environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, 
refer to [this doc](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html) for more detail
3. Download [key pair](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). Be sure 
to chmod private ssh key to **0600**. 
4. Set **Keypair** and **Key_Path** in `conf/ec2.yml`
5. `vagrant plugin install vagrant-aws`. Install aws vagrant plugin. To date, 0.5.0 plugin is tested.
6. By default, if you don't have a [Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html) 
called **tachyon-vagrant-test** in [Region and Availability Zone](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html) **(us-east-1, us-east-1a)**,
a cluster will be set up in us-east-1a by running `./create <number of machines> aws`.
If you do not understand the terminologies, you probably should just try the command.
7. If the Security Group has existed, configure a new one in `ec2.yml` according to comments of the `Security_Group` field.
8. You can monitor instance running state through [AWS web console](https://console.aws.amazon.com).

## After Cluster Set Up
1. When command `./create <number of machines> aws` succeeds, a purple line in the output tells you the IP of TachyonMaster, say, it's IP, then visit `http://{IP}:19999` in your browser, you should see Tachyon's web UI
2. Visit `http://{IP}:50070` in your browser, you should see Hadoop's web UI!
3. `vagrant ssh TachyonMaster`, and you should have entered TachyonMaster node, all software is installed under root directory, e.x. Tachyon is installed in `/tachyon`, Hadoop is installed in `/hadoop`
4. `/tachyon/bin/tachyon runTests` to run some tests against Tachyon
5. After the tests all pass, visit Tachyon web UI: `http://{IP}:19999` again, in `Browse FileSystem`, you should see files written to Tachyon by the tests run above
6. In TachyonMaster node, `~/vagrant-utils` provides utilities to work with the cluster, one is `copy-dir` which can copy or delete a directory in all nodes, another is `remount` which can unmount a device from current mount point and mount to another mount point. Play with them if you are interested
7. From inside TachyonMaster node, run `ssh TachyonWorker1` to login to TachyonWorker1 without password
8. If you don't want to play around in the cluster any more, `./destroy` to destroy the cluster, 
for virtualbox, virtual machines will be deleted, for EC2, instances will be terminated.

## Configurations

There are two kinds of configuration files under `conf`:

1. provider specific configuration, like `vb.yml` for virtualbox, `ec2.yml` for AWS EC2
2. software configuration, like `tachyon.yml`, `spark.yml`, `ufs.yml`(ufs means under filesystem). 

All the configuration files use [yaml syntax](http://en.wikipedia.org/wiki/YAML).

Meanings of fields in the configurations are explained in detail in the yml files.

If you are new to AWS EC2, we recommend you to understand comments in `ec2.yml`.

## FAQ

1. How to increase virtualbox virtual machine's memory size?

  Increase **MachineMemory** in **conf/vb.yml**

2. How to increase Tachyon worker's memory?

  Increase **WorkerMemory** in **conf/tachyon.yml**

3. How to increase HDFS capacity in AWS?
4. How to use EBS in AWS?
5. Why extra storage specified in Instance Type not available?

  Set **Block_Device_Mapping** in **conf/ec2.yml** according to the comment there

6. What's PV/HVM?

  explanation in http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/virtualization_types.html
