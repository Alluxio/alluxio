---
layout: global
title: Running Alluxio on GCE
nickname: Alluxio on GCE
group: Deploying Alluxio
priority: 4
---

* Table of Contents
{:toc}

Alluxio can be deployed on Google Compute Engine (GCE) using the
[Vagrant scripts](https://github.com/alluxio/alluxio/tree/master/deploy/vagrant) that come with
Alluxio. The scripts let you create, configure, and destroy clusters.

## Prerequisites

**Install Vagrant and the Google plugin**

Download [Vagrant](https://www.vagrantup.com/downloads.html)

Install Google Vagrant plugin:

```bash
$ vagrant plugin install vagrant-google
$ vagrant box add google https://github.com/mitchellh/vagrant-google/raw/master/google.box
```

**Install Alluxio**

[Download Alluxio](https://alluxio.org/download) to your local machine, and unzip it:

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

## Launch a Cluster

To run an Alluxio cluster on GCE, you need a [Google Cloud](https://cloud.google.com) billing account, project, service account and JSON keys for the service account.

If you are new to Google Cloud, create a billing account and project at the [free trial signup page](https://console.cloud.google.com/billing/freetrial). Also, If you are not familiar with Google Compute Engine, you may want to review the [documentation](https://cloud.google.com/compute/docs) first.

Next, you will need your JSON keys for your GCE project. Go to the
[Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) section of the
[IAM & Admin](https://console.cloud.google.com/projectselector/iam-admin) page in the Console.

If you are creating a new service account, make sure to check the option "Furnish a new private key"
and the "JSON" key type, and the JSON key will be downloaded. Save the JSON key in a safe location.

If you are using an existing service account, you should have already downloaded the JSON keys.
If not, you can create a new JSON key for the existing service account (click on the 3 dots to the
right, then "create key"), which will download the JSON key. Save the JSON key in a safe location.

Using the [gcloud sdk](https://console.cloud.google.com) configure keys for ssh:

```bash
$ curl https://sdk.cloud.google.com | bash
$ exec -l $SHELL
$ gcloud init
$ gcloud compute config-ssh
```

Copy `deploy/vagrant/conf/gce.yml.template` to `deploy/vagrant/conf/gce.yml` by:

```bash
$ cp deploy/vagrant/conf/gce.yml.template deploy/vagrant/conf/gce.yml
```

In the configuration file `deploy/vagrant/conf/gce.yml`, set the project id, service account, location to JSON key and ssh username you've just created.

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

Now you can launch the Alluxio cluster by running
the script under `deploy/vagrant`:

```bash
$ ./create <number of machines> google
```


Each node of the cluster runs an Alluxio worker, and the `AlluxioMaster` runs the Alluxio master.

## Access the cluster

**Access through Web UI**

After the command `./create <number of machines> google` succeeds, you can see two green lines like
below shown at the end of the shell output:

```bash
>>> AlluxioMaster public IP is xxx, visit xxx:19999 for Alluxio web UI<<<
>>> visit default port of the web UI of what you deployed <<<
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

All software is installed under the root directory, e.g. Alluxio is installed in `/alluxio`.

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

## Destroy the cluster

In the `deploy/vagrant` directory, you can run:

```bash
$ ./destroy
```

to destroy the cluster that you created. Only one cluster can be created at a time. After the
command succeeds, the GCE instances are terminated.
