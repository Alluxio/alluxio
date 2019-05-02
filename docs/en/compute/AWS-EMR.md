---
layout: global
title: Running Alluxio on EMR
nickname: AWS EMR
group: Data Applications
priority: 0
---

This guide describes how to configure Alluxio to run on [AWS EMR](https://aws.amazon.com/emr/).

* Table of Contents
{:toc}

## Overview

AWS EMR provides great options for running clusters on-demand to handle compute workloads. It manages the
deployment of various Hadoop Services and allows for hooks into these services for customizations. Alluxio
can run on EMR to provide functionality above what EMRFS currently provides. Aside from the added performance
benefits of caching, Alluxio also enables users to run compute workloads against on-premise storage or even a
different cloud provider's storage i.e. GCS, Azure Blob Store. 

## Prerequisites

* Account with AWS
* IAM Account with the default EMR Roles
* Key Pair for EC2
* An S3 Bucket
* AWS CLI

The majority of the pre-requisites can be found by going through the
[AWS EMR Getting Started](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html) guide. An S3 bucket
is needed as Alluxio's Root Under File System. If required, this can be reconfigured to be HDFS if needed.

## Basic Setup

To begin with, download the alluxio-emr.sh and alluxio-presto.json files from Github. These files will serve as the
main mechanisms to change the Alluxio configuration in the future. Make sure that the AWS CLI is also set up and ready
with the required AWS Access/Secret key.

1. Edit the alluxio-emr.sh file and set the value for `alluxio.underfs.address=s3a://my-bucket/emr/alluxio`.
2. Configure the below command with the required parameters

```bash
aws emr create-cluster --release-label emr-5.23.0 --instance-count 3 --instance-type <instance-type> --applications Name=Presto Name=Hive --name '<ClusterName>' --bootstrap-actions Path=s3://bucket/path/to//alluxio-emr.sh --configurations file:///path/to/file/alluxio-presto.json --ec2-attributes KeyName=<ec2-keypair-name>
```

3. On the [EMR Console](https://console.aws.amazon.com/elasticmapreduce/home), you should be able to see the cluster
going through the different stages of setup. Once the cluster is in the 'Waiting' stage, click on the cluster details
to get the 'Master public DNS'. SSH into this instance using the keypair provided in the previous command.
4. Test that Alluxio is running as expected by running `sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio runTests"`

Alluxio is installed in `/opt/alluxio/` by default. Hive and Presto are already configured to connect to Alluxio. The
cluster also uses AWS Glue as the default metastore for both Presto and Hive. This will allow you to maintain table
definitions between multiple runs of the Alluxio cluster.

## Creating a Table

The simplest step to using EMR with Alluxio is to create a table on Alluxio and query it using Presto/Hive.

1. SSH into the 'hadoop' user in the master node. Then switch to the 'alluxio' user.
2. Create a directory in Alluxio to be the external location of your table.
```bash
/opt/alluxio/bin/alluxio fs mkdir /testTable
```
3. Set the 'hadoop' user to be the owner of the directory
```bash
/opt/alluxio/bin/alluxio fs chown hadoop:hadoop /testTable
```
4. Exit to switch back into the 'hadoop' user and start the hive CLI.
```bash
hive
```
5. Create a new database to see if AWS Glue is working as expected. Check the [console](https://console.aws.amazon.com/glue/home)
to see if the database is created.
```
CREATE DATABASE glue;
```
6. Use the newly created database and define a table.
```
USE glue;
create external table test1 (userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION 'alluxio:///testTable';
```
7. Create the Presto /tmp directory
```
#Create Presto temp directory
sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio fs mkdir /tmp"
sudo runuser -l alluxio -c "/opt/alluxio/bin/alluxio fs chmod 777 /tmp"
presto-cli --catalog hive
```
8. Insert values into the table
```
USE glue;
INSERT INTO test1 VALUES ('1', 24, 'F', 'Developer', '12345');
```
9. Read back the values in the table
```
SELECT * FROM test1;
```


