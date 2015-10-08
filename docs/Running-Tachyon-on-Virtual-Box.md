---
layout: global
title: Running Tachyon on Virtual Box
nickname: Tachyon on Virtual Box
group: User Guide
priority: 2
---

Tachyon can be deployed on VirtualBox on your local machine using the [Vagrant scripts](https://github.com/amplab/tachyon/tree/master/deploy/vagrant) that come with Tachyon. The scripts let you create, configure and destroy clusters that come automatically configured with HDFS.

# Prerequisites

**Install VirtualBox**

Download [VirtualBox](https://www.virtualbox.org/wiki/Downloads)

**Install Vagrant**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

**Install Tachyon**

Download Tachyon to your local machine, and unzip it:

    $ wget http://tachyon-project.org/downloads/files/{{site.TACHYON_RELEASED_VERSION}}/tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz
    $ tar xvfz tachyon-{{site.TACHYON_RELEASED_VERSION}}-bin.tar.gz

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your home directory, run:

    $ sudo bash bin/install.sh

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then in `deploy/vagrant` run:

    $ sudo pip install -r pip-req.txt

# Launch a Cluster

Now you can launch the Tachyon cluster with Hadoop2.4.1 as under filesystem by running the script under `deploy/vagrant`:

    ./create <number of machines> vb

Each node of the cluster has a Tachyon worker, `TachyonMaster` has a Tachyon master.

# Access the cluster

**Access through Web UI**

After command `./create <number of machines> vb` succeeds, you can see two green lines like below shown at the end of the shell output:

    >>> TachyonMaster public IP is xxx <<<
    >>> visit default port of the web UI of what you deployed <<<

Default port for Tachyon Web UI is **19999**.

Default port for Hadoop Web UI is **50070**.

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

**Access with ssh**

The nodes set up are named to `TachyonMaster`, `TachyonWorker1`, `TachyonWorker2` and so on.

To ssh into a node, run

    $ vagrant ssh <node name>

For example, you can ssh into `TachyonMaster` with

    $ vagrant ssh TachyonMaster

All software are installed under root directory, e.g. Tachyon is installed in /tachyon, Hadoop is installed in /hadoop.

On `TachyonMaster` node, you can run some tests against Tachyon to check its health:

    $ /tachyon/bin/tachyon runTests

After the tests all pass, visit Tachyon web UI at `http://{MASTER_IP}:19999` again. Click `Browse File System` in the navigation bar, and you should see the files written to Tachyon by the above tests.

From a node in the cluster, you can ssh to other nodes in the cluster without password like

    $ ssh TachyonWorker1

# Destroy the cluster

Under `deploy/vagrant` directory, you can run
    
    $ ./destroy

to destroy the cluster that you created. Only one cluster can be created at a time. After the command succeeds, the EC2 instances are terminated.
