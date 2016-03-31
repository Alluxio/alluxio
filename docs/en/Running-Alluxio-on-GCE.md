---
layout: global
title: Running Alluxio on GCE
nickname: Alluxio on GCE
group: User Guide
priority: 4
---

Alluxio can be deployed on Google Compute Engine (GCE) using the
[Vagrant scripts](https://github.com/alluxio/alluxio/tree/master/deploy/vagrant) that come with
Alluxio. The scripts let you create, configure, and destroy clusters.

# Prerequisites

**Install Vagrant and the Google plugin**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install Google Vagrant plugin:

{% include Running-Alluxio-on-GCE/install-google-vagrant-plugin.md %}

**Install Alluxio**

Download Alluxio to your local machine, and unzip it:

{% include Common-Commands/download-alluxio.md %}

**Install python library dependencies**

Install [python>=2.7](https://www.python.org/), not python3.

Under `deploy/vagrant` directory in your home directory, run:

{% include Running-Alluxio-on-GCE/install-vagrant.md %}

Alternatively, you can manually install [pip](https://pip.pypa.io/en/latest/installing/), and then
in `deploy/vagrant` run:

{% include Running-Alluxio-on-GCE/install-pip.md %}

# Launch a Cluster

To run an Alluxio cluster on GCE, you need a [Google Cloud](cloud.google.com) billing account, project, service account and JSON keys for the service account. 

If you are new to Google Cloud, create a billing account and project at the [free trial signup page](https://console.cloud.google.com/billing/freetrial). Also, If you are not familiar with Google Compute Engine, you may want to review the [documentation](http://cloud.google.com/compute/docs) first.

Next, new and existing Google Cloud users need to choose or create a service account within the [Console](console.google.com) on the [Permissions](http://console.cloud.google.com/permissions) page, under the [Service Accounts](http://console.cloud.google.com/permissions) tab.
If creating a new service account, check "Furnish a new private key." from the account creation dialog box. Download and store the JSON key in a safe location.
If reusing a service account, you'll need to have saved JSON keys for the account or download new keys. To download keys for an existing service account, while still in the [Service Accounts](http://console.cloud.google.com/permissions) tab, find the menu for the account under the 3 dots at the right of the service account list and select "create key." Save the JSON key in a safe location.

Using the [gcloud sdk](http://console.cloud.google.com) configure keys for ssh:

{% include Running-Alluxio-on-GCE/config-ssh.md %}

Copy `deploy/vagrant/conf/gce.yml.template` to `deploy/vagrant/conf/gce.yml` by:

{% include Running-Alluxio-on-GCE/copy-gce.md %}

In the configuration file `deploy/vagrant/conf/gce.yml`, set the project id, service account, location to JSON key and ssh username you've just created.

GCE currently defaults to using Hadoop2 as the underfs, as GCS is not yet supported with Alluxio. 

Now you can launch the Alluxio cluster by running
the script under `deploy/vagrant`:

{% include Running-Alluxio-on-GCE/launch-cluster.md %}

Each node of the cluster runs an Alluxio worker, and the `AlluxioMaster` runs the Alluxio master.

# Access the cluster

**Access through Web UI**

After the command `./create <number of machines> google` succeeds, you can see two green lines like
below shown at the end of the shell output:

{% include Running-Alluxio-on-GCE/shell-output.md %}

Default port for Alluxio Web UI is **19999**.

Before you can access the Web UI, a network firewall rule needs to be made to allow tcp traffic on port 19999.
This can be done through the [Console](console.cloud.google.com) UI or using a gcloud command like the
following, which assumes a network named 'default'.

{% include Running-Alluxio-on-GCE/add-firewall-rule.md %}

Visit `http://{MASTER_IP}:{PORT}` in the browser to access the Web UIs.

You can also monitor the instances state through
[Google Cloud console](https://console.cloud.google.com).

Here are some scenarios when you may want to check the console:
 - When the cluster creation fails, check GCE instances status/logs.
 - After the cluster is destroyed, confirm GCE instances are terminated.
 - When you no longer need the cluster, make sure GCE instances are NOT costing you extra money.

**Access with ssh**

The nodes set up are named to `AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2` and so on.

To ssh into a node, run:

{% include Running-Alluxio-on-GCE/ssh.md %}

For example, you can ssh into `AlluxioMaster` with:

{% include Running-Alluxio-on-GCE/ssh-AlluxioMaster.md %}

All software is installed under the root directory, e.g. Alluxio is installed in `/alluxio`.

On the `AlluxioMaster` node, you can run tests against Alluxio to check its health:

{% include Running-Alluxio-on-GCE/runTests.md %}

After the tests finish, visit Alluxio web UI at `http://{MASTER_IP}:19999` again. Click `Browse
File System` in the navigation bar, and you should see the files written to Alluxio by the above
tests.

From a node in the cluster, you can ssh to other nodes in the cluster without password with:

{% include Running-Alluxio-on-GCE/ssh-other-node.md %}

# Destroy the cluster

Under `deploy/vagrant` directory, you can run:

{% include Running-Alluxio-on-GCE/destroy.md %}

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the GCE instances are terminated.
