---
layout: global
title: Running Alluxio on Virtual Box
nickname: Alluxio on Virtual Box
group: User Guide
priority: 2
---

Alluxio can be deployed on [VirtualBox](https://www.virtualbox.org/) on your local machine
using the [Vagrant scripts](https://github.com/amplab/alluxio/tree/master/deploy/vagrant)
that come with Alluxio. The scripts let you create, configure, and destroy clusters that come
automatically configured with HDFS.

# Prerequisites

**Install VirtualBox**

Download [VirtualBox](https://www.virtualbox.org/wiki/Downloads)

**Install Vagrant**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

**Install Alluxio**

Download Alluxio to your local machine, and unzip it:

{% include Common-Commands/download-alluxio.md %}

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your home directory, run:

{% include Running-Alluxio-on-Virtual-Box/install-vagrant.md %}

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then
in `deploy/vagrant` run:

{% include Running-Alluxio-on-Virtual-Box/install-pip.md %}

# Launch a Cluster

Now you can launch the Alluxio cluster with Hadoop2.4.1 as under filesystem by running the script
under `deploy/vagrant`:

{% include Running-Alluxio-on-Virtual-Box/launch-cluster.md %}

Each node of the cluster runs an Alluxio worker, and the `AlluxioMaster` runs an Alluxio master.

# Access the cluster

**Access through Web UI**

After the command `./create <number of machines> vb` succeeds, you can see two green lines like
below shown at the end of the shell output:

{% include Running-Alluxio-on-Virtual-Box/shell-output.md %}

Default port for Alluxio Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

**Access with ssh**

The nodes set up are named as `AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2` and so on.

To ssh into a node, run:

{% include Running-Alluxio-on-Virtual-Box/ssh.md %}

For example, you can ssh into `AlluxioMaster` with:

{% include Running-Alluxio-on-Virtual-Box/ssh-AlluxioMaster.md %}

All software is installed under the root directory, e.g. Alluxio is installed in `/alluxio`,
and Hadoop is installed in `/hadoop`.

On the `AlluxioMaster` node, you can run tests against Alluxio to check its health:

{% include Running-Alluxio-on-Virtual-Box/runTests.md %}

After the tests finish, visit Alluxio web UI at `http://{MASTER_IP}:19999` again. Click `Browse
File System` in the navigation bar, and you should see the files written to Alluxio by the above
tests.

From a node in the cluster, you can ssh to other nodes in the cluster without password with:

{% include Running-Alluxio-on-Virtual-Box/ssh-other-node.md %}

# Destroy the cluster

Under `deploy/vagrant` directory, you can run:

{% include Running-Alluxio-on-Virtual-Box/destroy.md %}

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the virtual machines are terminated.
