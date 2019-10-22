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
[AWS EMR Getting Started](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html) guide.
An S3 bucket is needed as Alluxio's Root Under File System and to serve as the location for the
bootstrap script.
If required, the root UFS can be reconfigured to be HDFS.

## Basic Setup

To begin with, [download an Alluxio release](https://www.alluxio.io/download) and unzip it.

1. Set up the required IAM roles for the account to be able to use the EMR service.
```console
$ aws emr create-default-roles
```
2. The Alluxio bootstrap script is hosted in a publicly readable
[S3 bucket](https://alluxio-public.s3.amazonaws.com/emr/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-emr.sh).
This bucket can also be accessed using it's S3 URI: `s3://alluxio-public/emr/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-emr.sh`
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
Path=s3://alluxio-public/emr/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-emr.sh,\
Args=[s3://test-bucket/path/to/mount/] \
--configurations https://alluxio-public.s3.amazonaws.com/emr/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-emr.json \
--ec2-attributes KeyName=<ec2-keypair-name>
```
4. On the [EMR Console](https://console.aws.amazon.com/elasticmapreduce/home), you should be able to
see the cluster going through the different stages of setup.
Once the cluster is in the 'Waiting' stage, click on the cluster details to get the
'Master public DNS'.
SSH into this instance using the keypair provided in the previous command.
If a security group isn't specified via CLI, the default EMR security group will not allow inbound
SSH.
To SSH into the machine, a new rule will need to be added.
5. Test that Alluxio is running as expected
```console
$ alluxio runTests
```

Alluxio is installed in `/opt/alluxio/` by default.
Hive and Presto are already configured to connect to Alluxio.
The cluster also uses AWS Glue as the default metastore for both Presto and Hive.
This will allow you to maintain table definitions between multiple runs of the Alluxio cluster.

See the below sample command for reference.

```console
$ aws emr create-cluster \
--release-label emr-5.25.0 \
--instance-count 3 \
--instance-type m4.xlarge \
--applications Name=Presto Name=Hive \
--name 'Test Cluster' \
--bootstrap-actions \
Path=s3://alluxio-public/emr/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-emr.sh,\
Args=[s3://test-bucket/path/to/mount/] \
--configurations https://alluxio-public.s3.amazonaws.com/emr/{{site.ALLUXIO_RELEASED_VERSION}}/alluxio-emr.json \
--ec2-attributes KeyName=admin-key
```

> Note: The default Alluxio Worker memory is set to 1/3 of the physical memory on the instance.
If a specific value is desired, set `alluxio.worker.memory.size` in the provided
`alluxio-site.properties` or in the additional options argument.

## Creating a Table

The simplest step to using EMR with Alluxio is to create a table on Alluxio and query it using Presto/Hive.

1. SSH into the 'hadoop' user in the master node.
2. Create a directory in Alluxio to be the external location of your table.
```console
$ /opt/alluxio/bin/alluxio fs mkdir /testTable
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

The Alluxio bootstrap also takes care of setting up EMR for you.
Follow the steps in our Alluxio on Spark
[documentation]({{ '/en/compute/Spark.html#examples-use-alluxio-as-input-and-output' | relativize_url }})
to get started.

## Customization

Tuning of Alluxio properties can be done in a few different locations.
Depending on which service needs tuning, EMR offers different ways of modifying the service
settings/environment variables.

### Bootstrap Script Usage

```
Usage: alluxio-emr.sh <root-ufs-uri>
                             [-b <backup_uri>]
                             [ -d <alluxio-download-uri>]
                             [-f <file_uri>]
                             [-i <journal_backup_uri>]
                             [-p <delimited_properties>]
                             [-s <property_delimiter>]
                             
alluxio-emr.sh is a script which can be used to bootstrap an AWS EMR cluster
with Alluxio. It can download and install Alluxio as well as add properties
specified as arguments to the script.
  
By default, if the environment this script executes in does not already contain
an Alluxio install at /opt/alluxio then it will download, untar, and configure
the environment at /opt/alluxio. If an install already exists at /opt/alluxio,
nothing will be installed over it, even if -d is specified.
  
If a specific Alluxio tarball is desired, see the -d option.
  
  <root-ufs-uri>    (Required) The URI of the root UFS in the Alluxio
                    namespace.
                    
  -b                An s3:// URI that the Alluxio master will write a backup
                    to upon shutdown of the EMR cluster. The backup and and
                    upload MUST be run within 60 seconds. If the backup cannot
                    finish within 60 seconds, then an incomplete journal may
                    be uploaded. This option is not recommended for production
                    or mission critical use cases where the backup is relied
                    upon to restore cluster state after a previous shutdown.
                    

  -d                An s3:// or http(s):// URI which points to an Alluxio
                    tarball. This script will download and untar the
                    Alluxio tarball and install Alluxio at /opt/alluxio if an
                    Alluxio installation doesn't already exist at that location.
                    

  -f                An s3:// or http(s):// URI to any remote file. This property
                    can be specified multiple times. Any file specified through
                    this property will be downloaded and stored with the same
                    name to /opt/alluxio/conf/
                    

  -i                An s3:// or http(s):// URI which represents the URI of a
                    previous Alluxio journal backup. If supplied, the backup
                    will be downloaded, and upon Alluxio startup, the Alluxio
                    master will read and restore the backup.
                    

  -p                A string containing a delimited set of properties which
                    should be added to the
                    ${ALLUXIO_HOME}/conf/alluxio-site.properties file. The
                    delimiter by default is a semicolon ";". If a different
                    delimiter is desired use the [-s] argument.
                    

  -s                A string containing a single character representing what
                    delimiter should be used to split the Alluxio properties
                    provided in the [-p] argument.
```

### Alluxio Service

Making configuration changes to the Alluxio Service can be done in a few different ways via the
bootstrap script.
The `[-p]` flag allows users to pass in a set of delimited key-value properties to be set on all of
the Alluxio nodes.
An alternative would be to pass in a custom file using the `[-f]` flag named
`alluxio-site.properties`.
The bootstrap will make sure to overwrite any user-provided configs while retaining any defaults
that are not overwritten.
The bootstrap also allows users to install previous versions of Alluxio (>=2.0) by specifying
a download URL (HTTP or S3 only).

### Alluxio Client

Generic client-side properties can also be edited via the bootstrap script as mentioned above.
This is mostly for the native client (CLI).
Property changes for a specific service like Presto/Hive should be done in the respective section
of the EMR JSON configuration file i.e. `core-site.xml` or `hive.catalog`.
