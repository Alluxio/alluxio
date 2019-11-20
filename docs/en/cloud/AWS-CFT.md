---
layout: global
title: Deploying Alluxio using AWS CloudFormation
nickname: AWS CFT
group: Alluxio in the Cloud
priority: 2
---

This guide describes how to deploy Alluxio using [AWS CloudFormation](https://aws.amazon.com/cloudformation/).

* Table of Contents
{:toc}

## Overview

Among the many ways to deploy Alluxio on AWS, one of the simplest approaches is to use AWS CloudFormation.
With a few clicks, all the resources needed for an Alluxio cluster will be automatically modeled and provisioned.
You can define the type of Alluxio cluster you want and how you want to configure it.
This tutorial outlines the steps to use the Alluxio CloudFormation template to provision a cluster,
including setting up a cluster in high availability mode.

## Prerequisites

- An account on [AWS](https://aws.amazon.com/)
- Web console access to [CloudFormation](https://console.aws.amazon.com/cloudformation) and [EC2](https://console.aws.amazon.com/ec2)
- A terminal window with [SSH](https://man.openbsd.org/ssh.1)

Familiarity with Cloudformation and EC2 is helpful but not required.
The tutorial launches an Alluxio cluster from the perspective of a user with a newly created AWS account.

Note that the launched instances do not qualify for free usage tier.
The default instance type we will be using is **r4.4xlarge**. 

## Launch an Alluxio Cluster

This section outlines how to launch an Alluxio cluster.

### Create an EC2 key pair

If you have valid EC2 key pair already, skip ahead to the next step.

{% accordion keypair %}
  {% collapsible Create an EC2 key pair %}
CloudFormation will analyze existing AWS resources on your account to fill in values needed by the Alluxio CloudFormation template (CFT).
For example, you need to choose an existing EC2 key pair that is in the region in which you are creating the Alluxio cluster stack.  

Open a web browser and navigate to the EC2 console, logging in with your credentials when prompted.
Note the AWS region in the upper right corner; click on the region to open a dropdown of available regions.
It is recommended to select the region that is geographically closest to your computer.

On the left sidebar, click the Key Pairs link to view created key pairs for this region.
Click the Create Key Pair button to create a new key pair and download the it to your computer. 

Assuming the downloaded key pair is located at `~/Downloads/keypair.pem`,
change the file permissions of the key pair to read-only by running the following command:
```console
chmod 400 ~/Downloads/keypair.pem
```

  {% endcollapsible %}
{% endaccordion %}

### Choose the CloudFormation template to launch

{% accordion launch %}
  {% collapsible Choose the CloudFormation template to launch %}
Navigate to the [CloudFormation console](https://console.aws.amazon.com/cloudformation) and click Create Stack.
We have two templates available: a basic and an advanced version.
The advanced version is a copy of the basic one with additional parameters and configuration options exposed when creating the stack.

Copy either URL in the *Amazon S3 URL* field:
- Basic: [https://alluxio-public.s3.amazonaws.com/cft/{{site.ALLUXIO_RELEASED_VERSION}}/basic.json](https://alluxio-public.s3.amazonaws.com/cft/{{site.ALLUXIO_RELEASED_VERSION}}/basic.json)
- Advanced: [https://alluxio-public.s3.amazonaws.com/cft/{{site.ALLUXIO_RELEASED_VERSION}}/advanced.json](https://alluxio-public.s3.amazonaws.com/cft/{{site.ALLUXIO_RELEASED_VERSION}}/advanced.json)

  {% endcollapsible %}
{% endaccordion %}

### Configure cluster

{% accordion configureCluster %}
  {% collapsible Configure cluster %}
Specify the details of your Alluxio cluster in this page:
- Stack details: Name of your cluster

- Network Configuration: 
  - Choose the [VPC and Subnet](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Subnets.html) from the existing values.
  The default values should be available to select.
  If not, create them following the [AWS VPC documentation](https://docs.aws.amazon.com/vpc/latest/userguide/working-with-vpcs.html).
  These parameters isolate the cluster from other virtual private clouds, AWS regions, and availability zones.
  - Choose at least [security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
  as a virtual firewall that controls the traffic allowed to reach your Alluxio cluster.
  It’s highly recommended that port 22 is open for SSH access.
  - The **AlluxioWebInboundRuleIp** parameter specifies the inbound locations for Alluxio web ports.
  The simplest configuration is to set this value to `0.0.0.0/0`, which opens traffic to anywhere.
  You may also choose to set this to a specific IP in CIDR notation to be more restrictive.

- EC2 Configuration:
  - Choose a **KeyName** specifying the name of a key pair.
  This key pair will be used to SSH into the cluster instances.
  - Choose the **MasterInstanceType** and **WorkerInstanceType** suitable for your workload.
  The default value is `r4.4xlarge`.
  Larger master instances provide more memory for Alluxio master to store metadata.
  Worker instance memory space is proportional to the Alluxio worker memory size, which determines how much data can be stored in this worker.
  - **WorkerSpotPrice** is an optional field to launch worker instances as spot instances,
  specifying the maximum hourly price that you are willing to pay for spot instances.
  Note that the price should be set according to the worker instance type.
  If the price is too low, Alluxio workers may not be fully provisioned and the stack will show as `CREATE_FAILED`.
  By default, all instances will be launched [on demand](https://aws.amazon.com/ec2/pricing/on-demand/).
  EC2 instances can also be launched as [spot instances](https://aws.amazon.com/ec2/spot/).
  This saves a significant portion of the instance cost,
  but at the risk of having them terminated and reclaimed by EC2 at any time.
  Alluxio masters are critical to the cluster and should not be launched as spot instances.
  In contrast, worker instances fit the use case of spot instances
  because new workers can register themselves to the cluster and unavailable workers will be marked as lost.
  Because the addition and loss of workers do not affect basic Alluxio functionality,
  we can support launching Alluxio clusters with spot instances for workers.

- Alluxio Configuration
  - **EnableHA** defaults to `No`, which instructs the cluster to launch a single master.
  If set to `Yes`, three master instances will be provisioned.
  - **WorkersCount** determines the number of Alluxio workers for the cluster.
  - **AlluxioRootMountS3BucketName** and **AlluxioRootMountS3BucketPath** are combined form a S3 URI to serve as Alluxio filesystem's root mount.
  The current user account must have the read/write/list permissions to this S3 URI and the URI will be mounted to the root of the Alluxio file system.
  - **OpenS3Access** should be set to `Yes` if Alluxio or other services in the cluster will need to access S3 buckets,
  other than the one defined for the root mount.
  - **AlluxioProperties** can be set to provide additional Alluxio site properties.
  Alluxio CFT only provides the necessary parameters to create an Alluxio cluster.
  If you desire to fine-tune Alluxio behavior, specify the desired properties in the format of `KEY1=VALUE1,KEY2=VALUE2`.
  The specified key-value pairs will be appended to the `alluxio-site.properties` file in all nodes inside the Alluxio cluster.

- Advanced Alluxio Configuration: These optional parameters are only available in the advanced template.
  - **AlluxioJournalSize** determines the EBS volume size, in GB, where Alluxio's embedded journal will write to.
  Each Alluxio master mounts a EBS volume dedicated for logging all metadata changes to achieve fault tolerance.
  The journal size should be proportional to the estimated magnitude of Alluxio metadata operations.
  - **AlluxioWorkerMemPercent** is the percentage of the instance's total memory to allocate for the Alluxio worker process.
  By default this is set to 70%.
  This space is used for caching remote data in order to drastically increase overall I/O throughput.
  - **AlluxioWorkerSSDSize** determines the SSD volume size, in GB, to attach to each worker instance.
  An EBS volume is attached for workers to attach remote data if the value is non-zero.
  A SSD drive is a simple scalable alternative to instance memory to help increase the Alluxio worker's caching capacity.
  - **AlluxioMetadataBackupDirectory** determines the path to backup Alluxio's journal contents.
  This path is relative to Alluxio's root mount point.
  Backup files can be use to relaunch an Alluxio cluster 
  - **AlluxioMetadataBackupTime** is the time to schedule the daily backup operation.
  If set to an empty string, the backup operation will be disabled.
  - **AlluxioMetadataRestoreUri** can be set to launch an Alluxio cluster
  whose state is restored from the backup file at the given URI.

- Advanced Configuration: This section provides advanced configuration outside the scope of Alluxio.
  - **IamInstanceProfile** assigns a custom role with permissions to access AWS.
  By default, a IAM role is created as part of setup, with permissions to access EC2, S3, and AutoScaling services.
  These are necessary for Alluxio to bootstrap and access its S3 mount points.
  Note that the **OpenS3Access** parameter is only applicable if **IamInstanceProfile** is not set.

Congratulations! All the necessary parameters are set now.
  
  {% endcollapsible %}
{% endaccordion %}

### Stack options

{% accordion stackOptions %}
  {% collapsible Configure stack options %}
All the stack options are provided by CloudFormation by default. 
These options include adding tags to resources created in the Alluxio cluster stack,
choosing the IAM role to limit the permissions available, 
and specifying the stack policy and rollback configuration.
For further insight into those options, see [stack options documentation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-add-tags.html).

  {% endcollapsible %}
{% endaccordion %}

### Review and Launch

{% accordion reviewAndLaunch %}
  {% collapsible Review and Launch %}
Finally, review the stack details of your Alluxio cluster.
After reviewing the content, press the Create stack to start generating Alluxio cluster. 

There may be a blue box informing you that the Alluxio CloudFormation template requires capabilities to create an IAM role.
If this appears, you will need to mark the checkbox in order to continue.

![cft_iam_capabilities]({{ '/img/cft_iam_capabilities.png' | relativize_url }})

The stack takes at least 5 minutes to create but can take longer depending on how many nodes the cluster has.
The stack status can be tracked in the Events tab.

![cft_cluster_in_progress]({{ '/img/cft_cluster_in_progress.png' | relativize_url }})

Once its status is `CREATE_COMPLETE`, the cluster is up and running.

![cft_cluster_created]({{ '/img/cft_cluster_created.png' | relativize_url }})

  {% endcollapsible %}
{% endaccordion %}

### Accessing the cluster

Note that the following instructions differ between a single master cluster 
versus a cluster running in high availability mode.

{% accordion outputSingle %}
  {% collapsible CloudFormation output for an Alluxio cluster with a single master %}
Navigate to the Outputs tab in the CloudFormation console.
This shows the command to SSH into the master instance,
replacing `/path/to/alluxio_keypair.pem` with the path to your key pair file.
There is also a link to the master's web UI.

![cft_output_single_master]({{ '/img/cft_output_single_master.png' | relativize_url }})

  {% endcollapsible %}
{% endaccordion %}
{% accordion outputHA %}
  {% collapsible CloudFormation output for an Alluxio cluster in High Availability mode %}
Three Alluxio master nodes will be launched using the internal leader election.
Note that the stack outputs section will be less informative as CloudFormation is not able to get the leader master address.
Navigate to the [EC2 console](https://console.aws.amazon.com/ec2) Instances page; there you can see multiple instances with name <STACK_NAME>-AlluxioMaster/Worker.
Click one of the instance to view its public DNS.

![cft_ha_ec2console]({{ '/img/cft_ha_ec2console.png' | relativize_url }})

SSH using the public DNS as the hostname,
replacing `/path/to/alluxio_keypair.pem` with the path to your key pair file.

```console
ssh -i /path/to/your/pem ec2-user@PUBLIC_DNS
```

Once inside the instance, run the Alluxio CLI to interact with Alluxio.
In particular, run `alluxio fsadmin report` to identify the private DNS of the leading master.

![cft_ha_fsadmin]({{ '/img/cft_ha_fsadmin.png' | relativize_url }})

Return back to the EC2 console. Search with the private DNS of the leading master to identify its public DNS.

  {% endcollapsible %}
{% endaccordion %}

### Exploring the cluster

{% accordion exploreCluster %}
  {% collapsible Exploring the cluster %}
The Alluxio home directory is `/opt/alluxio`.
This is where configuration and log files are located.
Note that the Alluxio processes are installed and started as the `alluxio` user.

To restart the cluster, run the following commands in each master node as the `alluxio` user:
```console
/opt/alluxio/bin/alluxio-start.sh -a master
/opt/alluxio//bin/alluxio-start.sh -a job_master
/opt/alluxio//bin/alluxio-start.sh -a proxy
```
And run the following commands in each worker node, also as the `alluxio` user:
```console
/opt/alluxio/bin/alluxio-start.sh -a worker
/opt/alluxio//bin/alluxio-start.sh -a job_worker
/opt/alluxio//bin/alluxio-start.sh -a proxy
```

  {% endcollapsible %}
{% endaccordion %}
