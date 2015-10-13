---
layout: global
title: Running Tachyon with Yarn on EC2
nickname: Tachyon on EC2 with Yarn
group: User Guide
priority: 5
---

Tachyon can be started and managed by Yarn. This guide demonstrates how to launch Tachyon with Yarn
on EC2 machines using the
[Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant) that come with
Tachyon.

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

In the configuration file `deploy/vagrant/conf/ec2.yml`, set the value of `Keypair` to your keypair
name and `Key_Path` to the path to the pem key.

By default, the Vagrant script creates a
[Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
named *tachyon-vagrant-test* at
[Region(**us-east-1**) and Availability Zone(**us-east-1a**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).
The security group will be set up automatically in the region with all inbound/outbound network
traffic opened. You can change the security group, region and availability zone in `ec2.yml`.

Now you can launch the Tachyon cluster with Hadoop2.4.1 as under filesystem in us-east-1a by running
the script under `deploy/vagrant`:

```bash
$ ./create <number of machines> aws
```

# Access the cluster

**Access through Web UI**

After command `./create <number of machines> aws` succeeds, you can see two green lines like below
shown at the end of the shell output:

    >>> TachyonMaster public IP is xxx <<<
    >>> visit default port of the web UI of what you deployed <<<

Default port for Tachyon Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

You can also monitor the instances state through
[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1).

**Access with ssh**

The nodes set up are named to `TachyonMaster`, `TachyonWorker1`, `TachyonWorker2` and so on.

To ssh into a node, run

```bash
$ vagrant ssh <node name>
```

For example, you can ssh into `TachyonMaster` with

```bash
$ vagrant ssh TachyonMaster
```

All software are installed under root directory, e.g. Tachyon is installed in /tachyon, Hadoop is
installed in /hadoop.

# Configure Tachyon integration with Yarn

Yarn has been installed as a part of Hadoop2.4.1. Notice that, by default Tachyon binaries built by
vagrant script do not include this YARN integration. You should first stop the default Tachyon
service, re-compile Tachyon with profile yarn specified to have the Yarn client and
ApplicationMaster for Tachyon.

```bash
$ cd /tachyon
$ ./bin/tachyon-stop.sh
$ mvn clean install -Dhadoop.version=2.4.1 -Pyarn
```

To customize Tachyon master and worker with specific properties (e.g., tiered storage setup on each
worker), one can refer to [Configuration settings](Configuration-Settings.html) for more
information. To ensure your configuration can be read by both the ApplicationMaster and Tachyon
master/workers, put `tachyon-site.properties` under `${TACHYON_HOME}/conf` on each EC2 machine.

# Start Tachyon

Use script `integration/bin/tachyon-yarn.sh` to start Tachyon. This script requires three parameters in order:
1. A dir pointing to `${TACHYON_HOME}` on each machine so Yarn NodeManager could access Tachyon
scripts and binaries to launch masters and workers. With our EC2 setup, this dir is `/tachyon`.
2. The total number of Tachyon workers to start.
3. A HDFS path to distribute the binaries for Tachyon ApplicationMaster.

For example, here we launch a Tachyon cluster with 3 worker nodes, where an HDFS temp directory is
`hdfs://TachyonMaster:9000/tmp/` and each Yarn container can access Tachyon in `/tachyon`

```bash
$ /tachyon/integration/bin/tachyon-yarn.sh /tachyon 3 hdfs://TachyonMaster:9000/tmp/
```

This script will first upload the binaries with Yarn client and ApplicationMaster to the HDFS path
specified, then inform Yarn to run the client binary jar. The script will keep running with
ApplicationMaster status reported. You can also check `http://TachyonMaster:8088` in the browser to
access the Web UIs and watch the status of Tachyon job as well as the application ID.

NOTE: currently Tachyon Yarn framework does not guarantee to start the Tachyon master on the
TachyonMaster machine; use the Yarn Web UI to find out which machine is Tachyon master running on.

# Test Tachyon

You can run some tests against Tachyon to check its health:

```bash
$ /tachyon/bin/tachyon runTests
```

After the tests all pass, visit Tachyon web UI at `http://{MASTER_IP}:19999` again. Click `Browse
File System` in the navigation bar, and you should see the files written to Tachyon by the above
tests.


# Stop Tachyon

Tachyon can be stopped by using the following Yarn command where the application ID of Tachyon can
 be either retrieved from Yarn web UI or the output of `tachyon-yarn.sh`.

```bash
$ /hadoop/bin/yarn application -kill TACHYON_APPLICATION_ID
```

# Destroy the cluster

Under `deploy/vagrant` directory, you can run

```bash
$ ./destroy
```

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the EC2 instances are terminated.
