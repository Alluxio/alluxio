---
layout: global
title: Running Alluxio with YARN on EC2
nickname: Alluxio on EC2 with YARN
group: Deploying Alluxio
priority: 5
---

Alluxio can be started and managed by Apache YARN. This guide demonstrates how to launch Alluxio
with YARN on EC2 machines using the
[Vagrant scripts](https://github.com/alluxio/alluxio/tree/master/deploy/vagrant) that come with
Alluxio.

# Prerequisites

**Install Vagrant and the AWS plugins**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install AWS Vagrant plugin:

{% include Running-Alluxio-on-EC2-Yarn/install-vagrant-aws.md %}

**Install Alluxio**

Download Alluxio to your local machine, and unzip it:

{% include Running-Alluxio-on-EC2-Yarn/download-Alluxio-unzip.md %}

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your Alluxio home directory, run:

{% include Running-Alluxio-on-EC2-Yarn/install-python.md %}

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then
in `deploy/vagrant` run:

{% include Running-Alluxio-on-EC2-Yarn/install-pip.md %}

# Launch a Cluster

To run an Alluxio cluster on EC2, first sign up for an Amazon EC2 account
on the [Amazon Web Services site](http://aws.amazon.com/).

Then create [access keys](https://aws.amazon.com/developers/access-keys/) and set shell environment
variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` by:

{% include Running-Alluxio-on-EC2-Yarn/access-key.md %}

Next generate your EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html) in the region
you want to deploy to (**us-east-1** by default). Make sure to set the permissions of your private
key file so that only you can read it:

{% include Running-Alluxio-on-EC2-Yarn/generate-key-pair.md %}

Copy `deploy/vagrant/conf/ec2.yml.template` to `deploy/vagrant/conf/ec2.yml`, then
set the value of `Keypair` to your keypair name and `Key_Path` to the path to the pem key.

By default, the Vagrant script creates a
[Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
named *alluxio-vagrant-test* at
[Region(**us-east-1**) and Availability Zone(**us-east-1b**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).
The security group will be set up automatically in the region with all inbound/outbound network
traffic opened. You can change the security group, region and availability zone in `ec2.yml`.

Finally, set the "Type" field in `deploy/vagrant/conf/ufs.yml` to `hadoop2`.

Now you can launch the Alluxio cluster with Hadoop2.4.1 as under filesystem in us-east-1b by running
the script under `deploy/vagrant`:

{% include Running-Alluxio-on-EC2-Yarn/launch-Alluxio.md %}

# Access the cluster

**Access through Web UI**

After command `./create <number of machines> aws` succeeds, you can see two green lines like below
shown at the end of the shell output:

{% include Running-Alluxio-on-EC2-Yarn/shell-end.md %}

Default port for Alluxio Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

You can also monitor the instances state through
[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1).

**Access with ssh**

The nodes set up are named to `AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2` and so on.

To ssh into a node, run:

{% include Running-Alluxio-on-EC2-Yarn/ssh-node.md %}

For example, you can ssh into `AlluxioMaster` with:

{% include Running-Alluxio-on-EC2-Yarn/ssh-master.md %}

All software is installed under root directory, e.g. Alluxio is installed in `/alluxio`, Hadoop is
installed in `/hadoop`.

# Configure Alluxio integration with YARN

On our EC2 machines, YARN has been installed as a part of Hadoop version 2.4.1. Notice that, by
default Alluxio binaries
built by vagrant script do not include this YARN integration. You should first stop the default
Alluxio service, re-compile Alluxio with profile "yarn" specified to have the YARN client and
ApplicationMaster for Alluxio.

{% include Running-Alluxio-on-EC2-Yarn/stop-install-yarn.md %}

Note that adding `-DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip` is not strictly necessary,
but it makes the build run significantly faster.

To customize Alluxio master and worker with specific properties (e.g., tiered storage setup on each
worker), one can refer to [Configuration settings](Configuration-Settings.html) for more
information. To ensure your configuration can be read by both the ApplicationMaster and Alluxio
master/workers, put `alluxio-site.properties` under `~/.alluxio/` on each EC2 machine.

# Start Alluxio

Use script `integration/bin/alluxio-yarn.sh` to start Alluxio. This script takes three arguments:

1. The total number of Alluxio workers to start. (required)
2. An HDFS path to distribute the binaries for Alluxio ApplicationMaster. (required)
3. The Yarn name for the node on which to run the Alluxio Master (optional, defaults to `ALLUXIO_MASTER_HOSTNAME`)

For example, here we launch an Alluxio cluster with 3 worker nodes, where an HDFS temp directory is
`hdfs://AlluxioMaster:9000/tmp/` and the master hostname is `AlluxioMaster`

{% include Running-Alluxio-on-EC2-Yarn/three-arguments.md %}

This script will launch an Alluxio Application Master on Yarn, which will then request containers for the Alluxio master and workers. You can also check `http://AlluxioMaster:8088` in the browser to
access the Web UIs and watch the status of the Alluxio job as well as the application ID.

The output of the above script may produce output like the following:

{% include Running-Alluxio-on-EC2-Yarn/script-output.md %}

From the output, we know the application ID to run Alluxio is
**`application_1445469376652_0002`**. This application ID is needed to kill the application.


# Test Alluxio

When you know the IP of Alluxio master container, you can modify the `conf/alluxio-env.sh` to set
 up environment variable `ALLUXIO_MASTER_HOSTNAME` on each EC2 machine:

{% include Running-Alluxio-on-EC2-Yarn/environment-variable.md %}

You can run tests against Alluxio to check its health:

{% include Running-Alluxio-on-EC2-Yarn/runTests.md %}

After the tests finish, visit Alluxio web UI at `http://ALLUXIO_MASTER_IP:19999` again. Click
`Browse` in the navigation bar, and you should see the files written to Alluxio by the above
tests.


# Stop Alluxio

Alluxio can be stopped by using the following YARN command where the application ID of Alluxio can
be retrieved from either YARN web UI or the output of `alluxio-yarn.sh` as mentioned above. For
instance, if the application Id is `application_1445469376652_0002`, you can stop Alluxio by killing
the application using:

{% include Running-Alluxio-on-EC2-Yarn/kill-application.md %}

# Destroy the cluster

Under `deploy/vagrant` directory in your local machine where EC2 machines are launched, you can run:

{% include Running-Alluxio-on-EC2-Yarn/destroy.md %}

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the EC2 instances are terminated.

# Trouble Shooting

1 If you compile Alluxio with YARN integration using maven and see compilation errors like the
following messages:

{% include Running-Alluxio-on-EC2-Yarn/compile-error.md %}

Please make sure you are using the proper hadoop version
{% include Running-Alluxio-on-EC2-Yarn/Hadoop-version.md %}
