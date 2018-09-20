---
layout: global
title: Deploy Alluxio Using Vagrant
nickname: Alluxio on Vagrant
group: Deploying Alluxio
priority: 5
---
* Table of Contents
{:toc}

## Introduction
Alluxio can be deployed locally or in the cloud using the [Vagrant scripts](https://github.com/alluxio/alluxio/tree/master/deploy/vagrant) that come with Alluxio. The scripts let you create, configure, and destroy clusters that come automatically configured locally, with [Amazon EC2](https://ec2.amazon.com/) or with [Google Compute Engine](https:/cloud.google.com).

## Deploy using Vagrant on Virtual Box
### Prerequisites
**Install VirtualBox**

Download [VirtualBox](https://www.virtualbox.org/wiki/Downloads)

**Install Vagrant**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

**Install Alluxio**

[Clone Alluxio Repository](https://github.com/Alluxio/alluxio) to your local machine

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
### Launch Cluster

Now you can launch the Alluxio cluster with Hadoop2.4.1 as under filesystem by running the script under deploy/vagrant:

```bash
$ ./create <number of machines> vb
```

Each node of the cluster runs an Alluxio worker, and the AlluxioMaster runs an Alluxio master.

### Access and Verify Cluster
**Access through Web UI**

After the command `./create <number of machines> vb` succeeds, you can see two green lines like
below shown at the end of the shell output:

```bash
>>> AlluxioMaster public IP is xxx, visit xxx:19999 for Alluxio web UI<<<
>>> visit default port of the web UI of what you deployed <<<
```

Default port for Alluxio Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

**Access with ssh**

The nodes set up are named as `AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2` and so on.

To ssh into a node, run:

```bash
$ vagrant ssh <node name>
```

For example, you can ssh into `AlluxioMaster` with:

```bash
$ vagrant ssh AlluxioMaster
```

All software is installed under the root directory, e.g. Alluxio is installed in `/alluxio`,
and Hadoop is installed in `/hadoop`.

On the `AlluxioMaster` node, you can run tests against Alluxio to check its health:

```bash
$ /alluxio/bin/alluxio runTests
```

After the tests finish, visit Alluxio web UI at `http://{MASTER_IP}:19999` again. Click `Browse File System` in the navigation bar, and you should see the files written to Alluxio by the above tests.

From a node in the cluster, you can ssh to other nodes in the cluster without password with:

```bash
$ ssh AlluxioWorker1
```

### Destroy the cluster

Under `deploy/vagrant` directory, you can run:

```bash
$ ./destroy
```

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the virtual machines are terminated.

## Deploy using Vagrant on AWS EC2

### Prerequisites

**Install Vagrant and the AWS plugins**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install AWS Vagrant plugin:

```bash
$ vagrant plugin install vagrant-aws
$ vagrant box add dummy https://github.com/mitchellh/vagrant-aws/raw/master/dummy.box
```

**Install Alluxio**

[Clone Alluxio Repository](https://github.com/Alluxio/alluxio) to your local machine.

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

### Launch Cluster
To run an Alluxio cluster on EC2, first sign up for an Amazon EC2 account
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
named *alluxio-vagrant-test* at
[Region(**us-east-1**) and Availability Zone(**us-east-1b**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).
The security group will be set up automatically in the region with all inbound/outbound network
traffic opened. You can change the *security group*, *region* and *availability zone* in `ec2.yml`. Sometimes the default zone can be unavailable.
Note: the keypair is associated with a specific region. For example, if you created a keypair in us-east-1, the keypair is invalid in other regions (like us-west-1).  If you ran into permission/connection errors, please first check the region/zone.


### Access the cluster

**Access through Web UI**

After the command `./create <number of machines> aws` succeeds, you can see two green lines like
below shown at the end of the shell output:

```bash
    >>> AlluxioMaster public IP is xxx, visit xxx:19999 for Alluxio web UI<<<
    >>> visit default port of the web UI of what you deployed <<<
```

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

```bash
$ vagrant ssh <node name>
```

For example, you can ssh into `AlluxioMaster` with:

```bash
$ vagrant ssh AlluxioMaster
```

All software is installed under the root directory, e.g. Alluxio is installed in `/alluxio`.

On the `AlluxioMaster` node, you can run tests against Alluxio to check its health:

```bash
$ /alluxio/bin/alluxio runTests
```

After the tests finish, visit Alluxio web UI at `http://{MASTER_IP}:19999` again. Click `Browse
File System` in the navigation bar, and you should see the files written to Alluxio by the above
tests.

You can login to [AWS web console](https://console.aws.amazon.com/console), then go to your S3 console,
and find some files written into your S3 bucket by the above tests.

From a node in the cluster, you can ssh to other nodes in the cluster without password with:

```bash
$ ssh AlluxioWorker1
```

### Destroy the cluster

Under `deploy/vagrant` directory, you can run:

```bash
$ ./destroy
```

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the EC2 instances are terminated.

### Advanced Tips
**Spot Instance**
Using spot instances is a way to reduce EC2 cost. Spot instances are non-guaranteed instances which are priced with bidding.
Note that spot instances may be taken away from you if someone bids more, and there are no more spot instances available.
However, for short-term testing, spot instances are very appropriate, because it is rare that spot instances are taken from you.

By default, the deploy scripts DO NOT use spot instances. Therefore, you have to enable deploy scripts to use spot instances.

In order to enable spot instances, you have to modify the file: `deploy/vagrant/conf/ec2.yml`:

    Spot_Price: “X.XX”

For AWS EC2, the default underfs is S3. You need to sign into your [Amazon S3 console](http://aws.amazon.com/s3/), create a S3 bucket and write the bucket's name to the field `S3:Bucket` in `conf/ufs.yml`. To use other under storage systems, configure the field `Type` and the corresponding configurations in `conf/ufs.yml`.

Now you can launch the Alluxio cluster with your chosen under filesystem in your chosen availability zone by running
the script under `deploy/vagrant`:

```bash
$ ./create <number of machines> aws
```

Each node of the cluster runs an Alluxio worker, and the `AlluxioMaster` runs the Alluxio master.

## Deploy with Vagrant on GCE

### Prerequisites

**Install Vagrant and the Google plugin**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install the Google Vagrant plugin:

```bash
$ vagrant plugin install vagrant-google
$ vagrant box add google https://github.com/mitchellh/vagrant-google/raw/master/google.box
```

**Install Alluxio**

[Clone the Alluxio Repository](https://github.com/Alluxio/alluxio) to your local machine.

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

If you already have [pip](https://pip.pypa.io/en/latest/installing/) installed, you can directly
install the dependencies by running the following in the `deploy/vagrant` of the Alluxio install:

```bash
$ sudo bash bin/install.sh
```

If you do not have `pip` installed, in the `deploy/vagrant` directory of the Alluxio install, run:

```bash
$ sudo pip install -r pip-req.txt
```

### Launch a Cluster

To run an Alluxio cluster on GCE, you need a [Google Cloud](https://cloud.google.com) billing
account, project, service account and JSON keys for the service account.

If you are new to Google Cloud, create a billing account and project at the
[free trial signup page](https://console.cloud.google.com/billing/freetrial). Also, If you are not
familiar with Google Compute Engine, you may want to review the
[documentation](https://cloud.google.com/compute/docs) first.

Next, you will need your JSON keys for your GCE project. Go to the
[Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) section of the
[IAM & Admin](https://console.cloud.google.com/projectselector/iam-admin) page in the Console.

If you are creating a new service account, make sure to check the option "Furnish a new private key"
and the "JSON" key type, and the JSON key will be downloaded. 
Save the JSON key in a safe location.

If you are using an existing service account, you should have already downloaded the JSON keys.
If not, you can create a new JSON key for the existing service account (click on the 3 dots to the
right, then "create key"), which will download the JSON key. 
Save the JSON key in a safe location.

Using the [gcloud sdk](https://console.cloud.google.com) configure keys for ssh:

```bash
$ curl https://sdk.cloud.google.com | bash
$ exec -l $SHELL
$ gcloud init
$ gcloud compute config-ssh
```

Create the Vagrant GCE config file by copying the provided template:

```bash
$ cp deploy/vagrant/conf/gce.yml.template deploy/vagrant/conf/gce.yml
```

In the configuration file `deploy/vagrant/conf/gce.yml`, set the project id, service account,
location to JSON key and ssh username you've just created.

For GCE, the default underfs is Google Cloud Storage (GCS). Visit the
[Storage page](https://console.cloud.google.com/storage/) of the Google Cloud console, create a GCS
bucket, and set the bucket's name to the field `GCS:Bucket` in `conf/ufs.yml`. To use other
under storage systems, configure the field `Type` and the corresponding configurations in
`conf/ufs.yml`.

To access GCS, you need to create [developer keys](https://cloud.google.com/storage/docs/migrating#keys)
in the [Interoperability tab](https://console.cloud.google.com/storage/settings) of the GCS console,
and set shell environment variables `GCS_ACCESS_KEY_ID` and `GCS_SECRET_ACCESS_KEY` by:

```bash
$ export GCS_ACCESS_KEY_ID=<your access key>
$ export GCS_SECRET_ACCESS_KEY=<your secret access key>
```

Now you can launch the Alluxio cluster by running the script under `deploy/vagrant`:

```bash
$ ./create <number of machines> google
```

Each node of the cluster runs an Alluxio worker, and the `AlluxioMaster` node runs the Alluxio master.

### Access the cluster

**Access through the Web UI**

After the command `./create <number of machines> google` succeeds, you will see two green lines like
below shown at the end of the shell output:

```bash
>>> AlluxioMaster public IP is xxx, visit xxx:19999 for Alluxio web UI
>>> visit default port of the web UI of what you deployed
```

Default port for Alluxio Web UI is **19999**.

Before you can access the Web UI, a network firewall rule needs to be made to allow tcp traffic on port 19999.
This can be done through the [Console](https://console.cloud.google.com) UI or using a gcloud command like the
following, which assumes a network named 'default'.

```bash
$ gcloud compute firewall-rules create alluxio-ui --allow tcp:19999
```

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Alluxio Web UI.

You can also monitor the instances state through
[Google Cloud console](https://console.cloud.google.com).

Here are some scenarios when you may want to check the console:
 - When the cluster creation fails, check GCE instances status/logs.
 - After the cluster is destroyed, confirm GCE instances are terminated (to avoid unexpected costs).

**Access with ssh**

The nodes set up are named to `AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2` and so on.

To ssh into a node, run:

```bash
$ vagrant ssh <node name>
```

For example, you can ssh into `AlluxioMaster` with:

```bash
$ vagrant ssh AlluxioMaster
```

Alluxio is installed in `/alluxio`.

On the `AlluxioMaster` node, you can run sample tests on Alluxio to check its health:

```bash
$ /alluxio/bin/alluxio runTests
```

After the tests finish, visit Alluxio web UI at `http://{MASTER_IP}:19999` again. Click `Browse
File System` in the navigation bar, and you should see the files written to Alluxio by the tests.

From a node in the cluster, you can ssh to other nodes in the cluster with:

```bash
$ ssh AlluxioWorker1
```

### Destroy the cluster

In the `deploy/vagrant` directory, you can run:

```bash
$ ./destroy
```

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the GCE instances are terminated.

