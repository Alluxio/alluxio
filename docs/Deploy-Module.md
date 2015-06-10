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

[Vagrant](https://www.vagrantup.com), here is the [download link](https://www.vagrantup.com/downloads.html).

### What is Vagrant?

Vagrant can create VM images (VirtualBox, VMWare Fusion), Docker containers, and AWS and OpenStack
instances.

### Why Use Vagrant?

Setting up a Tachyon cluster correctly with under filesystem and computation frameworks is a tedious huge undertaking. It requires not only expertise on both Tachyon and the related systems, but also expertise on the target deployment platform. 

Vagrant makes it possible to predefine how to install and configure software on a "machine" which can be an aws instance, a virtualbox vm, a docker container or an openstack instance. Then with the same workflow, you can create the same environment on all these platforms.

[Ansible](http://docs.ansible.com), here is the [download link](http://docs.ansible.com/intro_installation.html)

Ansible is a pure python package, so you need to have python installed, follow the docs in the link above. 

### What is Ansible?

Ansible is a language and toolset to define how to provision(install software, configure the system). It allows you to manipulate remote servers on your laptop, any number of nodes in parallel!

### Why Use Ansible?

When set up a Tachyon cluster, we need the ability to manipulate target machines in parallel, say, install java on all nodes. Ansible satisfies this requirement. It's supported by Vagrant and has simple syntax, so we adopt it as the provisioning tool.

If you want to deploy on virtualbox on your laptop, please install [Virtualbox](https://www.virtualbox.org/wiki/Downloads).

## Quick Start With VirtualBox

After installing the prerequisites, if have virtualbox installed, you can follow this quick start to set up a two node Tachyon cluster with apache hadoop2.4.1 as under filesystem, one node is named TachyonMaster, another is TachyonWorker.

1. `cd TACHYON/deploy/vagrant`, replace TACHYON with the Tachyon repo on your host, you can clone the latest from [github](https://github.com/amplab/tachyon.git)
2. `./run_vb.sh`, it will be slow when you first run this command, because vagrant will download a centos6.5 virtual machine, and install necessary software in this machine. The console output is verbose enough for you to know what's going on. 
3. After the above command, a purple line in the output tells you the IP of TachyonMaster, the IP should be "192.168.1.12", visit `http://192.168.1.12:19999` in your browser, you should see Tachyon's web UI
4. Visit `http://192.168.1.12:50070` in your browser, you should see Hadoop's web UI!
5. `vagrant ssh TachyonMaster`, and you should have entered the virtual machine named TachyonMaster
6. `/tachyon/bin/tachyon runTests` to run some tests against Tachyon
7. After the tests all pass, visit Tachyon web UI: `http://192.168.1.12:19999` again, in `Browse FileSystem`, you should see files written to Tachyon by the tests run above
8. If you don't want to play around in the cluster any more, `vagrant destory` to delete the virtual machines. 

Cool! The more exciting aspect of deploy module is that you can set up the same cluster in AWS EC2 with the same commands and workflow described above, just change some configurations!

## Configurations

There are three kinds of configuration files under `conf`. All the configuration files use [yaml syntax](http://en.wikipedia.org/wiki/YAML).

1. configuration of virtual machine itself in `init.yml`. 
2. provider specific configuration, like `ec2.yml`, `openstack.yml`.
3. software configuration, like `tachyon.yml`, `spark.yml`, `ufs.yml`(ufs means under filesystem). 

Meanings of fields in the configurations are explained in detail in the yml files.

## AWS EC2

You need to have AWS account with access to EC2. If not, sign up [here](https://aws.amazon.com/). 

Set up [AWS CLI](http://docs.aws.amazon.com/AWSEC2/latest/CommandLineReference/ec2-cli-get-set-up.html) may be helpful.

Then you need to set shell environment variables `AWS_ACCESS_KEY`
and `AWS_SECRET_KEY`, refer to [this doc](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html). 

Then download [key pair](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html).

Install aws vagrant plugin. To date, 0.5.0 plugin is tested.

    vagrant plugin install vagrant-aws

Then update configurations in `conf/ec2.yml`.

Update `init.yml` and software configurations. 

Run `./run_aws.sh`, then `vagrant ssh TachyonMaster` or `vagrant ssh TachyonWorker#{number}`, to log into remote nodes. 

You can monitor instance running state through [AWS web console](https://console.aws.amazon.com).

`vagrant destroy` to terminate the instances.

