---
layout: global
title: Running Tachyon with YARN on EC2
nickname: Tachyon on EC2 with YARN
group: User Guide
priority: 5
---

Tachyon can be started and managed by Apache YARN. This guide demonstrates how to launch Tachyon
with YARN on EC2 machines using the
[Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant) that come with
Tachyon.

# Prerequisites

**Install Vagrant and the AWS plugins**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install AWS Vagrant plugin:

{% include Running-Tachyon-on-EC2-Yarn/install-vagrant-aws.md %}

**Install Tachyon**

Download Tachyon to your local machine, and unzip it:

{% include Running-Tachyon-on-EC2-Yarn/download-Tachyon-unzip.md %}

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your Tachyon home directory, run:

{% include Running-Tachyon-on-EC2-Yarn/install-python.md %}

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then
in `deploy/vagrant` run:

{% include Running-Tachyon-on-EC2-Yarn/install-pip.md %}

# Launch a Cluster

To run a Tachyon cluster on EC2, first sign up for an Amazon EC2 account
on the [Amazon Web Services site](http://aws.amazon.com/).

Then create [access keys](https://aws.amazon.com/developers/access-keys/) and set shell environment
variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` by:

{% include Running-Tachyon-on-EC2-Yarn/access-key.md %}

Next generate your EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html) in the region 
you want to deploy to (**us-east-1** by default). Make sure to set the permissions of your private 
key file so that only you can read it:

{% include Running-Tachyon-on-EC2-Yarn/generate-key-pair.md %}

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

{% include Running-Tachyon-on-EC2-Yarn/launch-Tachyon.md %}

# Access the cluster

**Access through Web UI**

After command `./create <number of machines> aws` succeeds, you can see two green lines like below
shown at the end of the shell output:

{% include Running-Tachyon-on-EC2-Yarn/shell-end.md %}

Default port for Tachyon Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

You can also monitor the instances state through
[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1).

**Access with ssh**

The nodes set up are named to `TachyonMaster`, `TachyonWorker1`, `TachyonWorker2` and so on.

To ssh into a node, run:

{% include Running-Tachyon-on-EC2-Yarn/ssh-node.md %}

For example, you can ssh into `TachyonMaster` with:

{% include Running-Tachyon-on-EC2-Yarn/ssh-master.md %}

All software is installed under root directory, e.g. Tachyon is installed in `/tachyon`, Hadoop is
installed in `/hadoop`.

# Configure Tachyon integration with YARN

On our EC2 machines, YARN has been installed as a part of Hadoop version 2.4.1. Notice that, by 
default Tachyon binaries 
built by vagrant script do not include this YARN integration. You should first stop the default 
Tachyon service, re-compile Tachyon with profile "yarn" specified to have the YARN client and
ApplicationMaster for Tachyon.

{% include Running-Tachyon-on-EC2-Yarn/stop-install-yarn.md %}

Note that adding `-DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip` is not strictly necessary,
but it makes the build run significantly faster.

To customize Tachyon master and worker with specific properties (e.g., tiered storage setup on each
worker), one can refer to [Configuration settings](Configuration-Settings.html) for more
information. To ensure your configuration can be read by both the ApplicationMaster and Tachyon
master/workers, put `tachyon-site.properties` under `${TACHYON_HOME}/conf` on each EC2 machine.

# Start Tachyon

Use script `integration/bin/tachyon-yarn.sh` to start Tachyon. This script requires three arguments:
1. A path pointing to `${TACHYON_HOME}` on each machine so YARN NodeManager can access Tachyon
scripts and binaries to launch masters and workers. With our EC2 setup, this path is `/tachyon`.
2. The total number of Tachyon workers to start.
3. A HDFS path to distribute the binaries for Tachyon ApplicationMaster.

For example, here we launch a Tachyon cluster with 3 worker nodes, where an HDFS temp directory is
`hdfs://TachyonMaster:9000/tmp/` and each YARN container can access Tachyon in `/tachyon`

{% include Running-Tachyon-on-EC2-Yarn/three-arguments.md %}

This script will first upload the binaries with YARN client and ApplicationMaster to the HDFS path
specified, then inform YARN to run the client binary jar. The script will keep running with
ApplicationMaster status reported. You can also check `http://TachyonMaster:8088` in the browser to
access the Web UIs and watch the status of the Tachyon job as well as the application ID.

The output of the above script may produce output like the following:

{% include Running-Tachyon-on-EC2-Yarn/script-output.md %}

From the output, we know the application ID to run Tachyon is 
**`application_1445469376652_0002`**. This application ID is needed to kill the application.

NOTE: currently Tachyon YARN framework does not guarantee to start the Tachyon master on the
TachyonMaster machine; use the YARN Web UI to read the logs of this YARN application. The log
of this application records which machine is used to launch a Tachyon master container like:

{% include Running-Tachyon-on-EC2-Yarn/log-Tachyon-master.md %}

# Test Tachyon

When you know the IP of Tachyon master container, you can modify the `conf/tachyon-env.sh` to set
 up environment variable `TACHYON_MASTER_ADDRESS` on each EC2 machine:

{% include Running-Tachyon-on-EC2-Yarn/environment-variable.md %}

You can run tests against Tachyon to check its health:

{% include Running-Tachyon-on-EC2-Yarn/runTests.md %}

After the tests finish, visit Tachyon web UI at `http://TACHYON_MASTER_IP:19999` again. Click
`Browse File System` in the navigation bar, and you should see the files written to Tachyon by the above
tests.


# Stop Tachyon

Tachyon can be stopped by using the following YARN command where the application ID of Tachyon can
be retrieved from either YARN web UI or the output of `tachyon-yarn.sh` as mentioned above. For
instance, if the application Id is `application_1445469376652_0002`, you can stop Tachyon by killing
the application using:

{% include Running-Tachyon-on-EC2-Yarn/kill-application.md %}

# Destroy the cluster

Under `deploy/vagrant` directory in your local machine where EC2 machines are launched, you can run:

{% include Running-Tachyon-on-EC2-Yarn/destroy.md %}

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the EC2 instances are terminated.

# Trouble Shooting

1 If you compile Tachyon with YARN integration using maven and see compilation errors like the
following messages:

{% include Running-Tachyon-on-EC2-Yarn/compile-error.md %}

Please make sure you are using the proper hadoop version
{% include Running-Tachyon-on-EC2-Yarn/Hadoop-version.md %}
