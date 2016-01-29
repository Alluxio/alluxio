---
layout: global
title: Running Tachyon on Virtual Box
nickname: Tachyon on Virtual Box
group: User Guide
priority: 2
---

Tachyon can be deployed on [VirtualBox](https://www.virtualbox.org/) on your local machine
using the [Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant)
that come with Tachyon. The scripts let you create, configure, and destroy clusters that come
automatically configured with HDFS.

# Prerequisites

**Install VirtualBox**

Download [VirtualBox](https://www.virtualbox.org/wiki/Downloads)

**Install Vagrant**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

**Install Tachyon**

Download Tachyon to your local machine, and unzip it:

{% include Running-Tachyon-on-Virtual-Box/download-tachyon.md %}

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your home directory, run:

{% include Running-Tachyon-on-Virtual-Box/install-vagrant.md %}

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then
in `deploy/vagrant` run:

{% include Running-Tachyon-on-Virtual-Box/install-pip.md %}

# Launch a Cluster

Now you can launch the Tachyon cluster with Hadoop2.4.1 as under filesystem by running the script
under `deploy/vagrant`:

{% include Running-Tachyon-on-Virtual-Box/launch-cluster.md %}

Each node of the cluster runs a Tachyon worker, and the `TachyonMaster` runs a Tachyon master.

# Access the cluster

**Access through Web UI**

After the command `./create <number of machines> vb` succeeds, you can see two green lines like
below shown at the end of the shell output:

{% include Running-Tachyon-on-Virtual-Box/shell-output.md %}

Default port for Tachyon Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

**Access with ssh**

The nodes set up are named as `TachyonMaster`, `TachyonWorker1`, `TachyonWorker2` and so on.

To ssh into a node, run:

{% include Running-Tachyon-on-Virtual-Box/ssh.md %}

For example, you can ssh into `TachyonMaster` with:

{% include Running-Tachyon-on-Virtual-Box/ssh-TachyonMaster.md %}

All software is installed under the root directory, e.g. Tachyon is installed in `/tachyon`,
and Hadoop is installed in `/hadoop`.

On the `TachyonMaster` node, you can run tests against Tachyon to check its health:

{% include Running-Tachyon-on-Virtual-Box/runTests.md %}

After the tests finish, visit Tachyon web UI at `http://{MASTER_IP}:19999` again. Click `Browse
File System` in the navigation bar, and you should see the files written to Tachyon by the above
tests.

From a node in the cluster, you can ssh to other nodes in the cluster without password with:

{% include Running-Tachyon-on-Virtual-Box/ssh-other-node.md %}

# Destroy the cluster

Under `deploy/vagrant` directory, you can run:

{% include Running-Tachyon-on-Virtual-Box/destroy.md %}

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the virtual machines are terminated.
