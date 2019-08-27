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

AWS EMR provides great options for running clusters on-demand to handle compute workloads.
It manages the deployment of various Hadoop Services and allows for hooks into these services for
customizations.
Alluxio can run on EMR to provide functionality above what EMRFS currently provides.
Aside from the added performance benefits of caching, Alluxio also enables users to run compute 
workloads against on-premise storage or even a different cloud provider's storage i.e. GCS, Azure
Blob Store.

## Prerequisites

* Account with AWS
* IAM Account with the default EMR Roles
* Key Pair for EC2
* An S3 Bucket
* AWS CLI: Make sure that the AWS CLI is set up and ready with the required AWS Access/Secret key

The majority of the pre-requisites can be found by going through the
[AWS EMR Getting Started](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html) guide. An S3 bucket
is needed as Alluxio's Root Under File System and to serve as the location for the bootstrap script. If required,
the root UFS can be reconfigured to be HDFS.

## Basic Setup

To begin with, [download an Alluxio release](https://www.alluxio.io/download) and unzip it.

1. Set up the required IAM roles for the account to be able to use the EMR service.
```bash
aws emr create-default-roles
```
2. The Alluxio bootstrap script is hosted in a publicly readable
[S3 bucket](https://alluxio-public.s3.amazonaws.com/2.0.1/emr/alluxio-emr.sh).
This bucket can also be accessed using it's S3 URI: `s3://alluxio-public/emr/alluxio-emr.sh`
The bootstrap script only requires a root UFS URI as an argument.
Additional options can be seen in the comments at the top of the bootstrap script.
```console
$ aws emr create-cluster \
--release-label emr-5.25.0 \
--instance-count <num-instances> \
--instance-type <instance-type> \
--applications Name=Presto Name=Hive Name=Spark \
--name '<cluster-name>' \
--bootstrap-actions \
Path=s3://alluxio-public/emr/2.0.1/alluxio-emr.sh,\
Args=[s3://test-bucket/path/to/mount/] \
--configurations https://alluxio-public.s3.amazonaws.com/emr/2.0.1/alluxio-emr.json \
--ec2-attributes KeyName=<ec2-keypair-name>
```
4. On the [EMR Console](https://console.aws.amazon.com/elasticmapreduce/home), you should be able to see the cluster
going through the different stages of setup. Once the cluster is in the 'Waiting' stage, click on the cluster details
to get the 'Master public DNS'. SSH into this instance using the keypair provided in the previous command. If a
security group isn't specified via CLI, the default EMR security group will not allow inbound SSH. To SSH into the
machine, a new rule will need to be added.
5. Test that Alluxio is running as expected
```console
$ alluxio runTests
```

Alluxio is installed in `/opt/alluxio/` by default. Hive and Presto are already configured to connect to Alluxio. The
cluster also uses AWS Glue as the default metastore for both Presto and Hive. This will allow you to maintain table
definitions between multiple runs of the Alluxio cluster.

See the below sample command for reference.

```console
$ aws emr create-cluster \
--release-label emr-5.25.0 \
--instance-count 3 \
--instance-type m4.xlarge \
--applications Name=Presto Name=Hive \
--name 'Test Cluster' \
--bootstrap-actions \
Path=s3://alluxio-public/emr/2.0.1/alluxio-emr.sh,\
Args=[s3://test-bucket/path/to/mount/] \
--configurations https://alluxio-public.s3.amazonaws.com/emr/2.0.1/alluxio-emr.json \
--ec2-attributes KeyName=admin-key
```

Notes: The default Alluxio Worker memory is set to 20GB. If the instance type has less than 20GB of memory, change
the value in the `alluxio-emr.sh` script.

## Creating a Table

The simplest step to using EMR with Alluxio is to create a table on Alluxio and query it using Presto/Hive.

1. SSH into the 'hadoop' user in the master node.
2. Create a directory in Alluxio to be the external location of your table.
```bash
/opt/alluxio/bin/alluxio fs mkdir /testTable
```
3. Start the hive CLI.
```console
$ hive
```
4. Create a new database to see if AWS Glue is working as expected.
Check the [console](https://console.aws.amazon.com/glue/home) to see if the database is created.
```sql
CREATE DATABASE glue;
```
5. Use the newly created database and define a table.
```sql
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
6. Insert values into the table
```sql
USE glue;
INSERT INTO test1 VALUES (1, 24, 'F', 'Developer', '12345');
```
7. Read back the values in the table
```sql
SELECT * FROM test1;
```

## Run a Spark Job
The Alluxio bootstrap also takes care of setting up EMR for you. Follow the steps in our Alluxio on Spark [documentation]({{ '/en/compute/Spark.html#examples-use-alluxio-as-input-and-output' | relativize_url }})
to get started.

## Customization
Tuning of Alluxio properties can be done in a few different locations. Depending on which service needs tuning, EMR
offers different ways of modifying the service settings/environment variables.

### Alluxio Service
Any server-side configuration changes must be made in the `alluxio-emr.sh` bootstrap script. In the section for generating
the `alluxio-site.properties`, add a line with the configuration needed to append to the bottom of the file. Options can also
be passed as the 3rd argument to the bootstrap script with a ';' delimiter.

### Alluxio Client
Generic client-side properties can also be edited via the bootstrap script as mentioned above.
This is mostly for the native client (CLI).
Property changes for a specific service like Presto/Hive should be done in the respective
configuration file i.e. `core-site.xml`, `hive.catalog`.